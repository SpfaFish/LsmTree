package cache

import "CoreKV/Utils"

type Node struct {
	data []byte
	list *List
	nxt  *Node
}
type Cache struct {
	wLRU        *LRUCache
	sLRU        *SLRU
	cms         *cmSketch
	bloom       *Utils.Bloom
	maxCnt, cnt int
	tp          map[string]*List
}

func NewCache(cap int, cnt int) *Cache { //非精确cap容量, cnt:刷新次数
	wLRUCap := cap/100 + 1
	sLRUCap := cap - wLRUCap
	if sLRUCap <= 0 {
		sLRUCap = 1
	}
	stage1 := int(float64(sLRUCap)*0.2) + 1
	stage2 := int(float64(sLRUCap)*0.8) + 1
	total := sLRUCap + stage1 + stage2
	tp := make(map[string]*List, total)
	return &Cache{
		tp:     map[string]*List{},
		wLRU:   NewLRU(sLRUCap, tp),
		sLRU:   NewSLRU(stage1, stage2, tp),
		bloom:  Utils.NewBloom(total, 0.01),
		cms:    newCmSketch(int64(total)),
		maxCnt: cnt,
	}
}
func (this *Cache) update(list *List) {
	this.cms.Increment(HashString(list.key))
	if list.stage == 1 {
		list.pre.nxt = list.nxt
		list.nxt.pre = list.pre
		this.sLRU.stage1.n--
		lft := this.sLRU.stage2.Put(list.key, list.val, 2) //待优化，直接把指针插进去
		if lft != nil {
			this.sLRU.stage1.Put(list.key, list.val, 1)
		}
	}
}
func (this *Cache) Get(key string) *Utils.ValueStruct {
	this.cnt++
	if this.cnt == this.maxCnt {
		this.cms.Reset()
		this.cnt = 0
	}
	res := this.tp[key]
	if res != nil {
		this.update(res)
	}
	return res.val
}
func (this *Cache) Put(key string, value *Utils.ValueStruct) {
	this.cnt++
	if this.cnt == this.maxCnt {
		this.cms.Reset()
		this.cnt = 0
	}
	list := this.tp[key]
	if list != nil {
		list.val = value
		this.update(list)
		return
	}
	list = this.wLRU.Put(key, value, 0)
	if list != nil {
		if !this.bloom.CheckString(list.key) {
			this.bloom.InsertString(list.key)
			return
		}
		this.sLRU.PutWithCompare(list.key, list.val, this.cms)
	}
}
