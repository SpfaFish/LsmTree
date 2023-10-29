package cache

import (
	"math/rand"
	"time"
)

type cmRow []byte

func newCmRow(num int64) cmRow {
	return make(cmRow, num/2)
}

func (this cmRow) get(n uint64) byte {
	return this[n>>1] >> ((n & 1) << 2) & 0x0f
}
func (this cmRow) increment(n uint64) {
	offset := (n & 1) << 2
	i := n >> 1
	if (this[i]>>offset)&0x0f < 15 {
		this[i] += 1 << offset
	}
}
func (this cmRow) reset() {
	for i := range this {
		this[i] = (this[i] >> 1) & 0x77
	}
}
func (this cmRow) clear() {
	for i := range this {
		this[i] = 0
	}
}
func next2Power(x int64) int64 { // 最近上取整2的整数次幂
	x-- // 100 返回 100，101 返回 1000
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

const Size = 4

type cmSketch struct {
	rows [Size]cmRow
	seed [Size]uint64
	mask uint64 //用来将哈希结果映射到行中
}

func newCmSketch(num int64) *cmSketch { // num > 0
	num = next2Power(num)
	sketch := &cmSketch{mask: uint64(num - 1)}
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < Size; i++ {
		sketch.seed[i] = rd.Uint64()
		sketch.rows[i] = newCmRow(num)
	}
	return sketch
}
func (this *cmSketch) Increment(hashed uint64) {
	for i := range this.rows {
		this.rows[i].increment((hashed ^ this.seed[i]) & this.mask)
	}
}
func (this *cmSketch) Estimate(hashed uint64) int64 {
	mn := byte(255)
	for i := range this.rows {
		val := this.rows[i].get((hashed ^ this.seed[i]) & this.mask)
		if val < mn {
			mn = val
		}
	}
	return int64(mn)
}
func (this *cmSketch) Reset() { //遗忘策略
	for _, row := range this.rows {
		row.reset()
	}
}
func (this *cmSketch) Clear() {
	for _, row := range this.rows {
		row.clear()
	}
}
