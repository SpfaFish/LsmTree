package cache

import (
	"CoreKV/Utils"
)

type List struct {
	key   string
	val   *Utils.ValueStruct
	stage int // 0->wlru, 1->Stage1, 2->Stage2
	nxt   *List
	pre   *List
}

type LRUCache struct {
	cap   int
	n     int
	dummy *List
	tp    map[string]*List
}

func Constructor(capacity int) LRUCache {
	res := LRUCache{
		cap:   capacity,
		n:     0,
		dummy: new(List),
		tp:    map[string]*List{},
	}
	res.dummy.pre = res.dummy
	res.dummy.nxt = res.dummy
	return res
}

func NewLRU(capacity int, tp map[string]*List) (res *LRUCache) {
	res.cap = capacity
	res.tp = tp
	res.dummy = new(List)
	res.dummy.pre = res.dummy
	res.dummy.nxt = res.dummy
	return res
}

func (this *LRUCache) Get(key string) *List {
	if this.tp[key] == nil {
		return nil
	}
	tmp := this.tp[key]
	tmp.pre.nxt = tmp.nxt
	tmp.nxt.pre = tmp.pre
	tmp.pre = this.dummy
	tmp.nxt = this.dummy.nxt
	this.dummy.nxt.pre = tmp
	this.dummy.nxt = tmp
	return tmp
}

func (this *LRUCache) Put(key string, value *Utils.ValueStruct, st int) *List { //拿到溢出的元素
	var res *List = nil
	if this.tp[key] != nil {
		tmp := this.tp[key]
		tmp.pre.nxt = tmp.nxt
		tmp.nxt.pre = tmp.pre

		tmp.pre = this.dummy
		tmp.nxt = this.dummy.nxt
		this.dummy.nxt.pre = tmp
		this.dummy.nxt = tmp
		tmp.val = value
	} else {
		if this.n == this.cap {
			tmp := this.dummy.pre
			delete(this.tp, tmp.key)
			tmp.pre.nxt = tmp.nxt
			tmp.nxt.pre = tmp.pre //go 会自动回收内存
			res = tmp
			this.n--
		}
		tmp := new(List)
		tmp.key = key
		tmp.val = value
		tmp.stage = st
		this.tp[key] = tmp
		tmp.pre = this.dummy
		tmp.nxt = this.dummy.nxt
		this.dummy.nxt.pre = tmp
		this.dummy.nxt = tmp
		this.n++
	}
	return res
}
