package Utils

import (
	"encoding/binary"
	"unsafe"
)

const (
	BaseSize      uint32 = uint32(unsafe.Sizeof(uint32(0)))
	LargeBaseSize uint32 = uint32(unsafe.Sizeof(uint64(0)))
)

type ValueStruct struct {
	Meta        byte
	Value       []byte
	ExpiresTime uint64 // 过期时间
	Version     uint64 // 版本号
	// 编码时只存 value 和过期时间
}

func (this *ValueStruct) CoderSize() int {
	return 1 + int(LargeBaseSize)*3 + len(this.Value)
}
func CalcVarIntSize(x uint64) (sz uint32) {
	for {
		sz++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return sz
}
func (this *ValueStruct) CalcEncodedSize() uint32 {
	sz := len(this.Value) + 1 // + Meta
	return uint32(sz) + CalcVarIntSize(this.ExpiresTime)
}
func (this *ValueStruct) EncodeIn(buf []byte) uint64 { // 返回实际编码的大小
	buf[0] = this.Meta
	sz := binary.PutUvarint(buf[1:], this.ExpiresTime) //变长编码
	n := copy(buf[1+sz:], this.Value)
	return uint64(1 + sz + n)
}
func (this *ValueStruct) DecodeFrom(buf []byte) { // buf 要求有界
	this.Meta = buf[0]
	var sz int
	this.ExpiresTime, sz = binary.Uvarint(buf[1:])
	this.Value = buf[1+sz:]
}

type Entry struct {
	Key   []byte
	Value *ValueStruct
}

func (this *Entry) CoderSize() int {
	return int(LargeBaseSize) + len(this.Key) + this.Value.CoderSize()
}
func NewEntry(key, val []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: &ValueStruct{Value: val},
	}
}
