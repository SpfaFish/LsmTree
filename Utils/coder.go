package Utils

import (
	lsm "CoreKV/LsmTree"
	"os"
)

/*
结构体中顺序为编码顺序
opt 操作码对应数据类型
1: valueStruct
2: Entry
3: Block
4: Index
5: SSTable
*/
type Coder struct { //coder 设计成编码先编入buf再文件读写，解码直接从文件中读取
	opt  uint32
	file *os.File
}

func NewCoder(op uint32, f *os.File) *Coder {
	return &Coder{
		file: f,
		opt:  op,
	}
}

//func (this *Coder) decode(offset int64) interface{} {
//	this.opt = GetUint32(this.buf)
//	switch this.opt {
//	case 1:
//		return this.decodeVS(offset)
//	case 2:
//		return this.decodeEntry(offset)
//	case 3:
//		return this.decodeBlock(offset)
//	case 4:
//		return this.decodeIndex(offset)
//	case 5:
//		return this.decodeSSTable(offset)
//	}
//	return nil
//}

/*
	type ValueStruct struct {
		Meta        byte
		ExpiresTime uint64 // 过期时间 // Arena 中只用存过期时间和版本号
		Version     uint64 // 版本号
		valueSize uint64 // 编码时的信息
		Value       []byte
		//编码时需要多存一个 value 长度，因为value是变长的
	}
*/
func (this *Coder) EncodeVS(buf []byte, vs *ValueStruct) { //没有考虑文件报错，后期可加，考虑外面完全开好了buf
	size := vs.CoderSize()
	buf[0] = vs.Meta
	l := 1
	PutUint64(vs.ExpiresTime, buf[l:])
	l += int(LargeBaseSize)
	PutUint64(vs.Version, buf[l:])
	l += int(LargeBaseSize)
	PutUint64(uint64(len(vs.Value)), buf[l:])
	l += int(LargeBaseSize)
	copy(buf[l:size], vs.Value)
}
func (this *Coder) DecodeVS(offset int64) *ValueStruct {
	res := &ValueStruct{}
	buf := make([]byte, 1)
	_, _ = this.file.ReadAt(buf, offset)
	res.Meta = buf[0]
	buf = make([]byte, LargeBaseSize)

	l := offset + 1
	_, _ = this.file.ReadAt(buf, l)
	res.ExpiresTime = GetUint64(buf)

	l += int64(LargeBaseSize)
	_, _ = this.file.ReadAt(buf, l)
	res.Version = GetUint64(buf)

	l += int64(LargeBaseSize)
	_, _ = this.file.ReadAt(buf, l)
	valueSize := GetUint64(buf)

	l += int64(LargeBaseSize)
	res.Value = make([]byte, valueSize)
	_, _ = this.file.ReadAt(res.Value, l)
	return res
}

/*
type Entry struct { //只有一个value，不需要索引

		keySize uint64 //keySize
		Key   []byte
		Value *ValueStruct
	}
*/
func (this *Coder) EncodeEntry(buf []byte, entry *Entry) {
	keySize := entry.CoderSize()
	l := 0
	PutUint64(uint64(keySize), buf[l:])
	l += int(LargeBaseSize)
	copy(buf[l:l+keySize], entry.Key)
	l += keySize
	this.EncodeVS(buf[l:], entry.Value)
}
func (this *Coder) DecodeEntry(offset int64) *Entry {
	res := &Entry{}
	buf := make([]byte, LargeBaseSize)
	_, _ = this.file.ReadAt(buf, offset)
	l := offset
	keySize := GetUint64(buf)
	l += int64(LargeBaseSize)
	buf = make([]byte, keySize)
	_, _ = this.file.ReadAt(buf, l)
	l += int64(keySize)
	res.Value = this.DecodeVS(l)
	return res
}

/*
	type block struct {
		blockSize //解码信息
		count    uint64
		kvOffset []uint64 // 解码时才有用
		KV       []KVStruct
	}
*/
func (this *Coder) EncodeBlock(buf []byte, b *lsm.Block) {
	blockSize := b.CoderSize()
	l := 0
	PutUint64(uint64(blockSize), buf[l:])
	l += int(LargeBaseSize)
	PutUint64(b.Count, buf[l:])
	l += int(LargeBaseSize)
	prel := l
	l += int(LargeBaseSize) * int(b.Count)
	for i := range b.KV {
		b.KvOffset[i] = uint64(l)
		this.EncodeEntry(buf[l:], &b.KV[i])
		l += b.KV[i].CoderSize()
	}
	for i := 0; i < int(b.Count); i++ {
		PutUint64(b.KvOffset[i], buf[prel+i*int(LargeBaseSize):])
	}
}
func (this *Coder) DecodeBlock(offset int64) *lsm.Block { //拿到count后在索引上二分
	res := &lsm.Block{}
	buf := make([]byte, LargeBaseSize)
	_, _ = this.file.ReadAt(buf, offset)
	//blockSize := GetUint64(buf)
	l := offset
	l += int64(LargeBaseSize)
	_, _ = this.file.ReadAt(buf, l)
	res.Count = GetUint64(buf)
	//未读取完全
	return res
}

/*
type Index struct { //一个index 描述一个block

		indexSize //编解码时使用
		maxVersion  uint64
		maxkey      []byte
	}
*/
func (this *Coder) EncodeIndex(buf []byte, ind *lsm.Index) {
	indexSize := ind.CoderSize()
	l := 0
	PutUint64(uint64(indexSize), buf[l:])
	l += int(LargeBaseSize)
	PutUint64(ind.MaxVersion, buf[l:])
	l += int(LargeBaseSize)
	copy(buf[l:indexSize], ind.Maxkey)
}
func (this *Coder) DecodeIndex(offset int64) *lsm.Index {
	res := &lsm.Index{}
	buf := make([]byte, LargeBaseSize)
	_, _ = this.file.ReadAt(buf, offset)
	l := offset
	indexSize := GetUint64(buf)
	l += int64(LargeBaseSize)
	_, _ = this.file.ReadAt(buf, l)
	res.MaxVersion = GetUint64(buf)
	buf = make([]byte, indexSize-uint64(2*LargeBaseSize))
	_, _ = this.file.ReadAt(buf, l)
	res.Maxkey = buf
	return res
}

/*
	type Bloom struct {
		bloomSize //编解码时使用
		k    uint64   // 哈希个数
		seed []uint64 // 哈希种子（进制哈希）
		len  uint64   // m
		size uint64   //data 数组大小
		data []uint64 // bitset本体
	}
*/
func (this *Coder) EncodeBloom(buf []byte, filter *Bloom) { //需要开头
	bloomSize := filter.CoderSize()
	PutUint64(uint64(bloomSize), buf)
	l := int(LargeBaseSize)

	PutUint64(filter.k, buf[l:])
	l += int(LargeBaseSize)

	for i := uint64(0); i < filter.k; i++ {
		PutUint64(filter.seed[i], buf[l:])
		l += int(LargeBaseSize)
	}
	PutUint64(filter.len, buf[l:])
	l += int(LargeBaseSize)
	PutUint64(filter.size, buf[l:])
	l += int(LargeBaseSize)
	for i := uint64(0); i < filter.size; i++ {
		PutUint64(filter.data[i], buf[l:])
		l += int(LargeBaseSize)
	}
}
func (this *Coder) DecodeBloom(offset int64) *Bloom {
	res := &Bloom{}
	buf := make([]byte, LargeBaseSize)
	_, _ = this.file.ReadAt(buf, offset)
	l := offset
	bloomSize := GetUint64(buf)
	buf = make([]byte, bloomSize)
	l += int64(LargeBaseSize)
	_, _ = this.file.ReadAt(buf, l)
	res.k = GetUint64(buf[l:])
	l += int64(LargeBaseSize)
	res.seed = make([]uint64, res.k)
	for i := uint64(0); i < res.k; i++ {
		res.seed[i] = GetUint64(buf[l:])
		l += int64(LargeBaseSize)
	}
	res.len = GetUint64(buf[l:])
	l += int64(LargeBaseSize)
	res.size = GetUint64(buf[l:])
	l += int64(LargeBaseSize)
	res.data = make([]uint64, res.size)
	for i := uint64(0); i < res.size; i++ {
		res.data[i] = GetUint64(buf[l:])
		l += int64(LargeBaseSize)
	}
	return res
}

/*
   type SSTable struct { // 目的是可以在索引上二分，然后直接读取一个block
	//tableSize
   	blockCount uint64
   	IndOffset  []uint64
   	BlockOffset []uint64
	Filter *Bloom
   	Ind        []Index
   	Blocks     []Block
   }
*/

func (this *Coder) EncodeSSTable(buf []byte, table *lsm.SSTable) {
	tableSize := table.CoderSize()
	l := 0
	PutUint64(uint64(tableSize), buf[l:])
	l += int(LargeBaseSize)
	PutUint64(table.BlockCount, buf[l:])
	l += int(LargeBaseSize)
	preInd := l
	l += int(LargeBaseSize) * int(table.BlockCount)
	preBlock := l
	l += int(LargeBaseSize) * int(table.BlockCount)
	this.EncodeBloom(buf[l:], table.Filter)
	l += table.Filter.CoderSize()
	for i := 0; i < int(table.BlockCount); i++ {
		table.IndOffset[i] = uint64(l)
		this.EncodeIndex(buf[l:], table.Ind[i])
		l += table.Ind[i].CoderSize()
	}
	for i := 0; i < int(table.BlockCount); i++ {
		table.BlockOffset[i] = uint64(l)
		this.EncodeBlock(buf[l:], table.Blocks[i])
		l += table.Blocks[i].CoderSize()
	}
	for i := 0; i < int(table.BlockCount); i++ {
		PutUint64(table.IndOffset[i], buf[preInd+i*int(LargeBaseSize):])
	}
	for i := 0; i < int(table.BlockCount); i++ {
		PutUint64(table.BlockOffset[i], buf[preBlock+i*int(LargeBaseSize):])
	}
}

/*
查找时需要读取：
blockCount
IndexOffset
BlockOffset
Filter
Ind
*/
func (this *Coder) DecodeSSTable(offset int64) *lsm.SSTable {
	res := &lsm.SSTable{}
	buf := make([]byte, LargeBaseSize)
	_, _ = this.file.ReadAt(buf, offset)
	l := offset
	l += int64(LargeBaseSize)
	//tableSize := GetUint64(buf)
	_, _ = this.file.ReadAt(buf, l)
	res.BlockCount = GetUint64(buf)
	l += int64(LargeBaseSize)
	res.IndOffset = make([]uint64, res.BlockCount)
	res.BlockOffset = make([]uint64, res.BlockCount)
	buf = make([]byte, int(LargeBaseSize)*int(res.BlockCount)*2)
	_, _ = this.file.ReadAt(buf, l)
	for i := 0; i < int(res.BlockCount); i++ {
		res.IndOffset[i] = GetUint64(buf[i*int(LargeBaseSize):])
	}
	for i := 0; i < int(res.BlockCount); i++ {
		res.BlockOffset[i] = GetUint64(buf[(i+int(res.BlockCount))*int(LargeBaseSize):])
	}
	l += int64(res.BlockCount) * int64(LargeBaseSize) * 2
	res.Filter = this.DecodeBloom(l)
	l += int64(res.Filter.CoderSize())
	for i := 0; i < int(res.BlockCount); i++ {
		res.Ind[i] = this.DecodeIndex(l)
		l += int64(res.Ind[i].CoderSize())
	}
	return res
}
