package lsm

import cache2 "CoreKV/Utils/cache"

type cache struct {
	kvCache *cache2.Cache
}
type blockBuffer struct {
	b []byte
}

// close
func (c *cache) close() error {
	return nil
}

// NewCache
func NewCache(cap int, cnt int) *cache { //容量，刷新次数
	return &cache{
		kvCache: cache2.NewCache(cap, cnt),
	}
}
