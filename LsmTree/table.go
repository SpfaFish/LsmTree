package lsm

import (
	"CoreKV/Utils"
	"encoding/binary"
	"os"
)

type KVStruct struct {
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     *Utils.ValueStruct
}

func (this *KVStruct) CalcSize() uint64 {
	var sz uint64
	sz += uint64(Utils.LargeBaseSize) * 2
	sz += this.KeySize
	sz += this.ValueSize
	return sz
}

func (this *KVStruct) EncodeIn(buf []byte) int { //序列化
	sz := binary.PutUvarint(buf, this.KeySize)
	sz += binary.PutUvarint(buf[sz:], this.ValueSize)
	sz += copy(buf[sz:], this.Key)
	sz += int(this.Value.EncodeIn(buf[sz:]))
	return sz
}
func (this *KVStruct) DecodeFrom(buf []byte) int { //反序列化
	var sz, length int
	this.KeySize, length = binary.Uvarint(buf[sz:])
	sz += length
	this.ValueSize, length = binary.Uvarint(buf[sz:])
	this.Key = buf[sz : sz+int(this.KeySize)]
	sz += int(this.KeySize)
	this.Value.DecodeFrom(buf[sz : sz+int(this.ValueSize)])
	return sz + int(this.ValueSize)
}

// sstable
type Block struct {
	KV       []Utils.Entry
	KvOffset []uint64 // 解码时才有用
	Count    uint64
}

func (this *Block) getKV(baseOffset int64, ind int, f *os.File) *Utils.Entry {
	buf := make([]byte, Utils.LargeBaseSize)
	f.ReadAt(buf, baseOffset+int64(Utils.LargeBaseSize)+int64(ind)*int64(Utils.LargeBaseSize))
	KvOffset := Utils.GetUint64(buf)
	coder := Utils.NewCoder(1, f)
	kv := coder.DecodeEntry(baseOffset + int64(KvOffset))
	return kv
}
func (this *Block) Search(baseOffset int64, key []byte, maxVs uint64, f *os.File) *Utils.ValueStruct { //已有count
	l, r := 0, int(this.Count)
	var kv *Utils.Entry
	for l < r {
		mid := (l + r) >> 1
		kv = this.getKV(baseOffset, mid, f)
		tmp := Utils.Compare(key, kv.Key)
		if tmp <= 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if r == int(this.Count) {
		return nil
	}
	return kv.Value
}
func (this *Block) CoderSize() int {
	res := 0
	for i := range this.KV {
		res += this.KV[i].CoderSize()
	}
	res += int(Utils.LargeBaseSize) * (1 + int(this.Count))
	return res
}
func NewBlock() *Block {
	return &Block{
		KV:       make([]Utils.Entry, 0),
		KvOffset: make([]uint64, 0),
		Count:    0,
	}
}

//func (this *block) encodeIn(buf []byte) int {
//	var offset int
//	for i := uint64(0); i < this.count; i++ {
//		this.kvOffset[i] = uint64(offset)
//		offset += this.KV[i].EncodeIn(buf[offset:])
//	}
//	for i := uint64(0); i < this.count; i++ {
//		Utils.PutUint64(this.kvOffset[i], buf[offset:])
//		offset += int(Utils.LargeBaseSize)
//	}
//	Utils.PutUint64(this.count, buf[offset:])
//	return offset + int(Utils.LargeBaseSize)
//}
//func (this *block) decodeFrom(buf []byte) { // 有上界 //待做
//	var sz, length int
//	r := len(buf)
//	this.count = Utils.GetUint64(buf[r-int(Utils.LargeBaseSize) : r])
//	r -= int(Utils.LargeBaseSize)
//	l := r - int(Utils.LargeBaseSize*uint32(this.count))
//	for i := uint64(0); i < this.count; i++ {
//		this.kvOffset[i] = Utils.GetUint64(buf[l+int(uint32(i)*Utils.LargeBaseSize):])
//		sz += length
//	}
//	for i := uint64(0); i < this.count; i++ { // maybe not need
//		sz += this.KV[i].DecodeFrom(buf[sz:])
//	}
//}

type Index struct { //一个index 描述一个block
	MaxVersion uint64
	Maxkey     []byte
}

// 注意修改CoderSize
func (this *Index) CoderSize() int {
	return 2*int(Utils.LargeBaseSize) + len(this.Maxkey)
}

//func (this *Index) GetMaxKeyFrom(offset uint64, f *os.File) error {
//	tmp := make([]byte, 8*Utils.LargeBaseSize)
//	_, err := f.ReadAt(tmp, int64(offset+3*uint64(Utils.LargeBaseSize)))
//	if err != nil {
//		return err
//	}
//	var sz, lenth int
//	this.blockOffset, lenth = binary.Uvarint(tmp)
//	sz += lenth
//	this.maxVersion, lenth = binary.Uvarint(tmp[sz:])
//	sz += lenth
//	this.KeyCount, lenth = binary.Uvarint(tmp[sz:])
//	sz += lenth
//	this.maxKeySize, lenth = binary.Uvarint(tmp[sz:])
//	sz += lenth
//	tmp = make([]byte, this.maxKeySize)
//	_, err = f.ReadAt(tmp, int64(offset+uint64(sz)))
//	if err != nil {
//		return err
//	}
//	this.maxkey = tmp
//	return nil
//}
//func (this *Index) CalcSize() uint64 {
//	sz := uint64(0)
//	sz += uint64(Utils.CalcVarIntSize(this.blockOffset))
//	sz += uint64(Utils.CalcVarIntSize(this.maxVersion))
//	sz += uint64(Utils.CalcVarIntSize(this.KeyCount))
//	sz += uint64(Utils.CalcVarIntSize(this.maxKeySize))
//	sz += this.maxKeySize
//	sz += this.filter.CalcSize()
//	return sz
//}

//func (this *Index) encodeIn(buf []byte) int { //需要知道上下界
//	sz := binary.PutUvarint(buf, this.blockOffset)
//	sz += binary.PutUvarint(buf[sz:], this.maxVersion)
//	sz += binary.PutUvarint(buf[sz:], this.KeyCount)
//	sz += binary.PutUvarint(buf[sz:], this.maxKeySize)
//	sz += copy(buf[sz:], this.maxkey)
//	sz += this.filter.EncodeIn(buf[sz:])
//	return sz
//}
//func (this *Index) decodeFrom(buf []byte) int {
//	var sz, lenth int
//	this.blockOffset, lenth = binary.Uvarint(buf)
//	sz += lenth
//	this.maxVersion, lenth = binary.Uvarint(buf[sz:])
//	sz += lenth
//	this.KeyCount, lenth = binary.Uvarint(buf[sz:])
//	sz += lenth
//	this.maxKeySize, lenth = binary.Uvarint(buf[sz:])
//	sz += lenth
//	this.maxkey = make([]byte, this.maxKeySize)
//	sz += copy(this.maxkey, buf[sz:sz+int(this.maxKeySize)])
//	sz += this.filter.DecodeFrom(buf[sz:])
//	return sz
//}

type SSTable struct { // block 在前, index 在后，目的是可以在索引上二分，然后直接读取一个block
	Blocks      []*Block
	Ind         []*Index
	IndOffset   []uint64
	BlockOffset []uint64
	BlockCount  uint64
	Filter      *Utils.Bloom
}

func (this *SSTable) CoderSize() int {
	res := int(Utils.LargeBaseSize) * int(1+this.BlockCount+this.BlockCount)
	for i := range this.Ind {
		res += this.Ind[i].CoderSize()
		res += this.Blocks[i].CoderSize()
	}
	return res
}
func (this *SSTable) Search(key []byte, maxVs uint64, f *os.File) (*Utils.ValueStruct, error) {
	l, r := uint64(0), this.BlockCount
	if !this.Filter.Check(key) {
		return nil, nil
	}
	for l < r {
		mid := (l + r) >> 1
		if Utils.Compare(key, this.Ind[mid].Maxkey) <= 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if r == this.BlockCount {
		return nil, nil
	}
	codeHandler := Utils.NewCoder(3, f)
	this.Blocks[r] = codeHandler.DecodeBlock(int64(this.BlockOffset[r]))
	return this.Blocks[r].Search(int64(this.BlockOffset[r]), key, maxVs, f), nil
}

//	func (this *SSTable) CalcSize() uint64 {
//		var sz uint64
//		for i := range this.Blocks {
//			sz += this.Blocks[i].CalcSize()
//			sz += this.Ind[i].CalcSize()
//		}
//		sz += uint64(Utils.LargeBaseSize) * (this.blockCount + 1)
//		return sz
//	}
func (this *SSTable) Add(kv Utils.Entry) {
	this.Blocks[this.BlockCount-1].KV = append(this.Blocks[this.BlockCount-1].KV, kv)
	this.Blocks[this.BlockCount-1].Count++
	this.Filter.Insert(kv.Key)
	this.Ind[this.BlockCount-1].MaxVersion = max(this.Ind[this.BlockCount-1].MaxVersion, kv.Value.Version)

}

//func (this *SSTable) EncodeIn(buf []byte) uint64 {
//	var offset uint64
//	for i := uint64(0); i < this.blockCount; i++ {
//		this.Ind[i].blockOffset = offset
//		offset += uint64(this.Blocks[i].encodeIn(buf[offset:]))
//	}
//	for i := uint64(0); i < this.blockCount; i++ {
//		this.IndOffset[i] = offset
//		offset += uint64(this.Ind[i].encodeIn(buf[offset:]))
//	}
//	for i := uint64(0); i < this.blockCount; i++ {
//		Utils.PutUint64(this.IndOffset[i], buf[offset:])
//		offset += uint64(Utils.LargeBaseSize)
//	}
//	Utils.PutUint64(this.blockCount, buf[offset:])
//	offset += uint64(Utils.LargeBaseSize)
//	return offset
//}
