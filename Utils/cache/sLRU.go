package cache

import "CoreKV/Utils"

type SLRU struct {
	tp             map[string]*List
	cap1, cap2     int
	stage1, stage2 *LRUCache
}

func NewSLRU(c1, c2 int, tp map[string]*List) *SLRU {
	return &SLRU{
		tp:     tp,
		cap1:   c1,
		cap2:   c2,
		stage1: NewLRU(c1, tp),
		stage2: NewLRU(c2, tp),
	}
}
func HashString(a string) uint64 {
	seed := uint64(131)
	var res uint64
	for i := range a {
		res *= seed
		res += uint64(a[i])
	}
	return res
}
func (this *SLRU) PutWithCompare(key string, value *Utils.ValueStruct, cms *cmSketch) {
	if this.tp[key] != nil {
		tmp := this.tp[key]
		tmp.pre.nxt = tmp.nxt
		tmp.nxt.pre = tmp.pre

		tmp.pre = this.stage1.dummy
		tmp.nxt = this.stage1.dummy.nxt
		this.stage1.dummy.nxt.pre = tmp
		this.stage1.dummy.nxt = tmp
		tmp.val = value
	} else {
		if this.stage1.n == this.stage1.cap {
			tmp := this.stage1.dummy.pre
			if cms.Estimate(HashString(key)) < cms.Estimate(HashString(tmp.key)) {
				return
			}
			delete(this.tp, tmp.key)
			tmp.pre.nxt = tmp.nxt
			tmp.nxt.pre = tmp.pre //go 会自动回收内存
			this.stage1.n--
		}
		tmp := new(List)
		tmp.key = key
		tmp.val = value
		tmp.stage = 1
		this.tp[key] = tmp
		tmp.pre = this.stage1.dummy
		tmp.nxt = this.stage1.dummy.nxt
		this.stage1.dummy.nxt.pre = tmp
		this.stage1.dummy.nxt = tmp
		this.stage1.n++
	}
}
