package Utils

import (
	"encoding/binary"
	"math"
	"math/rand"
)

type Bloom struct {
	k    uint64   // 哈希个数
	seed []uint64 // 哈希种子（进制哈希）
	len  uint64   // m
	size uint64   //data 数组大小
	data []uint64 // bitset本体
}

func (this *Bloom) CoderSize() int { //严谨大小
	sz := 0
	sz += int(LargeBaseSize) * int(3+this.k+this.size)
	return sz
}
func PutUint64(x uint64, buf []byte) {
	for j := 0; j < int(LargeBaseSize); j++ {
		buf[j] = x >> (j * 8) & 0xff
	}
}
func GetUint64(buf []byte) uint64 {
	var res uint64 = 0
	for j := int(LargeBaseSize) - 1; j >= 0; j-- {
		res <<= 8
		res |= uint64(buf[j])
	}
	return res
}
func PutUint32(x uint32, buf []byte) {
	for j := 0; j < 4; j++ {
		buf[j] = x >> (j * 8) & 0xff
	}
}
func GetUint32(buf []byte) uint32 {
	var res uint32 = 0
	for j := 4 - 1; j >= 0; j-- {
		res <<= 8
		res |= uint32(buf[j])
	}
	return res
}
func (this *Bloom) EncodeIn(buf []byte) int { //需要开头
	sz := binary.PutUvarint(buf, this.k)
	for i := uint64(0); i < this.k; i++ {
		sz += binary.PutUvarint(buf[sz:], this.seed[i])
	}
	sz += binary.PutUvarint(buf[sz:], this.len)
	sz += binary.PutUvarint(buf[sz:], this.size)
	for i := uint64(0); i < this.size; i++ {
		PutUint64(this.data[i], buf[sz:])
		sz += int(LargeBaseSize)
	}
	return sz
}
func (this *Bloom) DecodeFrom(buf []byte) int {
	var sz, lenth int
	this.k, sz = binary.Uvarint(buf)
	for i := uint64(0); i < this.k; i++ {
		this.seed[i], lenth = binary.Uvarint(buf[sz:])
		sz += lenth
	}
	this.len, lenth = binary.Uvarint(buf[sz:])
	sz += lenth
	this.size, lenth = binary.Uvarint(buf[sz:])
	sz += lenth
	for i := uint64(0); i < this.size; i++ {
		this.data[i] = GetUint64(buf[sz:])
		sz += int(LargeBaseSize)
	}
	return sz
}

func calcHash(b string, seed uint64) uint64 {
	res := uint64(0)
	for c := range b {
		res *= seed
		res += uint64(c)
	}
	return res
}
func calcLen(n int, p float64) int { //n个元素，预期概率为 p 计算bitmap位数
	return int(-math.Log(p)*float64(n)*math.Log2E*math.Log2E) + 1
}
func (a *Bloom) init(n int, p float64) {
	a.len = uint64(calcLen(n, p))
	a.k = uint64(max(1, int(0.69*float64(a.len)/float64(n))))
	a.seed = make([]uint64, a.k)
	for i := uint64(0); i < a.k; i++ {
		a.seed[i] = uint64(rand.Intn(1919810))
	}
	a.size = uint64(a.len)/64 + 1
	a.data = make([]uint64, a.size)
}
func NewBloom(n int, p float64) *Bloom {
	res := new(Bloom)
	res.init(n, p)
	return res
}
func (a *Bloom) Insert(s []byte) {
	for i := uint64(0); i < a.k; i++ {
		key := calcHash(string(s), a.seed[i]) % uint64(a.len)
		a.data[key/64] |= 1 << (key & 63)
	}
}
func (a *Bloom) Check(s []byte) bool {
	for i := uint64(0); i < a.k; i++ {
		key := calcHash(string(s), a.seed[i]) % uint64(a.len)
		if a.data[key/64]>>(key&63)&1 != 1 {
			return false
		}
	}
	return true
}
func (a *Bloom) InsertString(s string) {
	for i := uint64(0); i < a.k; i++ {
		key := calcHash(s, a.seed[i]) % uint64(a.len)
		a.data[key/64] |= 1 << (key & 63)
	}
}
func (a *Bloom) CheckString(s string) bool {
	for i := uint64(0); i < a.k; i++ {
		key := calcHash(s, a.seed[i]) % uint64(a.len)
		if a.data[key/64]>>(key&63)&1 != 1 {
			return false
		}
	}
	return true
}
func (a *Bloom) InsertUint(s uint64) {
	for i := uint64(0); i < a.k; i++ {
		key := (s ^ a.seed[i]) % uint64(a.len)
		a.data[key/64] |= 1 << (key & 63)
	}
}
func (a *Bloom) CheckUint(s uint64) bool {
	for i := uint64(0); i < a.k; i++ {
		key := (s ^ a.seed[i]) % uint64(a.len)
		if a.data[key/64]>>(key&63)&1 != 1 {
			return false
		}
	}
	return true
}
