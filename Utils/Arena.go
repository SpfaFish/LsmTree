package Utils

import (
	"sync/atomic"
	"unsafe"
)

const (
	MaxNodeSize int = int(unsafe.Sizeof(node{})) //64MB
	MaxBufSize      = 1 << 30
)

type Arena struct {
	offset uint32 //偏移地址
	buf    []byte //数据
}

func newArena(n int64) *Arena { // 从 1 开始 0 设为空指针
	return &Arena{
		offset: BaseSize,
		buf:    make([]byte, n),
	}
}

func (s *Arena) allocate(n uint32) uint32 { //返回起始偏移地址
	offset := atomic.AddUint32(&s.offset, n)
	lenth := len(s.buf)
	if lenth-int(offset) < MaxNodeSize { //空间不足翻倍
		grow := lenth
		if grow > MaxBufSize {
			grow = MaxBufSize
		}
		if uint32(grow) < n { // 防止一次扩展不够
			grow = int(n)
		}
		newBuf := make([]byte, lenth+grow)
		copy(newBuf, s.buf)
		s.buf = newBuf
	}
	return offset - n
}
func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.offset))
}
func (s *Arena) putNode(height int) uint32 {
	//baseSize := uint32(unsafe.Sizeof(uint32(0))) //取得32/64位机器的基本数据大小
	unusedSize := uint32(MaxHeight-height-1) * BaseSize
	sz := uint32(MaxNodeSize) - unusedSize
	sz += BaseSize - (sz-1)%BaseSize - 1 //向前内存对齐
	offset := s.allocate(sz)
	return offset
}
func (s *Arena) putVal(v *ValueStruct) uint32 {
	sz := v.CalcEncodedSize()
	sz += BaseSize - (sz-1)%BaseSize - 1
	offset := s.allocate(sz)
	v.EncodeIn(s.buf[offset:])
	return offset
}
func (s *Arena) putKey(key []byte) uint32 {
	sz := uint32(len(key))
	sz += BaseSize - (sz-1)%BaseSize - 1
	offset := s.allocate(sz)
	copy(s.buf[offset:], key)
	return offset
}

func (this *Arena) GetNode(offset uint32) *node {
	return (*node)(unsafe.Pointer(&this.buf[offset]))
}
func (this *Arena) GetKey(offset, sz uint32) []byte {
	return this.buf[offset : offset+sz]
}
func (this *Arena) GetValue(offset, sz uint32) *ValueStruct {
	value := new(ValueStruct)
	value.DecodeFrom(this.buf[offset : offset+sz])
	return value
}
