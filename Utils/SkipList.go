package Utils

import (
	"bytes"
	"math/rand"
	"sync/atomic"
)

const (
	MaxHeight = 32
)

type node struct {
	KeyBlock   uint64            // offset,sz
	valueBlock uint64            // offset,sz
	HashKey    uint64            // HashKey
	h          int               //存储节点高度
	levels     [MaxHeight]uint32 // 存储跳表节点在 Arena 中的偏移地址
}

func encodeKey(offset, sz uint32) uint64 {
	return uint64(offset)<<32 | uint64(sz)
}
func encodeValue(offset, sz uint32) uint64 {
	return uint64(offset)<<32 | uint64(sz)
}
func decodeKey(KeyBlock uint64) (offset, sz uint32) {
	offset = uint32(KeyBlock >> 32)
	sz = uint32(KeyBlock)
	return
}
func decodeValue(KeyBlock uint64) (offset, sz uint32) {
	offset = uint32(KeyBlock >> 32)
	sz = uint32(KeyBlock)
	return
}

type SkipList struct {
	arena      *Arena
	headOffset uint32 //存储head的地址
	size       int32  //存储节点数量
	OnClose    func() //删除跳表时调用
}

func (this *SkipList) IncrSize() {
	atomic.AddInt32(&this.size, 1)
}
func (this *SkipList) DecrSize() {
	sz := atomic.AddInt32(&this.size, -1)
	if sz > 0 {
		return
	}
	if this.OnClose != nil {
		this.OnClose()
	}
	this.arena = nil
}
func newNode(arena *Arena, key []byte, val *ValueStruct, height int) (*node, uint32) {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	keySize := uint32(len(key))
	valOffset := arena.putVal(val)
	valSize := val.CalcEncodedSize()
	nd := arena.GetNode(nodeOffset)
	nd.KeyBlock = encodeKey(keyOffset, keySize)
	nd.valueBlock = encodeValue(valOffset, valSize)
	nd.HashKey = calcHashKey(key)
	nd.h = height
	return nd, nodeOffset
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	_, headOffset := newNode(arena, nil, &ValueStruct{}, MaxHeight-1)
	return &SkipList{
		headOffset: headOffset,
		arena:      arena,
		size:       1,
	}
}
func calcHashKey(key []byte) uint64 {
	len := len(key)
	if len > 8 {
		len = 8
	}
	var res uint64
	for i := 0; i < len; i++ {
		res |= uint64(key[i]) << (64 - (i+1)*8)
	}
	return res
}
func cmp(hashKey uint64, key []byte, b *node, arena *Arena) int {
	if hashKey == b.HashKey {
		bkeyOffset, bkeySize := decodeKey(b.KeyBlock)
		return bytes.Compare(key, arena.GetKey(bkeyOffset, bkeySize))
	}
	if hashKey < b.HashKey {
		return -1
	} else {
		return 1
	}
}
func (list *SkipList) Find(key []byte) *ValueStruct {
	pre := list.arena.GetNode(list.headOffset)
	hashKey := calcHashKey(key)
	for i := MaxHeight - 1; i >= 0; i-- {
		for nxt := pre.levels[i]; nxt != 0; nxt = pre.levels[i] {
			now := list.arena.GetNode(nxt)
			res := cmp(hashKey, key, now, list.arena)
			if res == 0 {
				return list.arena.GetValue(decodeValue(now.valueBlock))
			} else {
				if res == -1 {
					break
				}
			}
			pre = now
		}
	}
	return nil
}
func checkUp() bool {
	return rand.Intn(2) == 1
}
func (list *SkipList) Insert(e *Entry) {
	pre := list.arena.GetNode(list.headOffset)
	hashKey := calcHashKey(e.Key)
	var preElemHeaders [MaxHeight]*node //注意上界，可能需要调整或取min
	for i := MaxHeight - 1; i >= 0; i-- {
		preElemHeaders[i] = pre
		for nxt := pre.levels[i]; nxt != 0; nxt = pre.levels[i] {
			now := list.arena.GetNode(nxt)
			res := cmp(hashKey, e.Key, now, list.arena)
			if res == 0 {
				valOffset := list.arena.putVal(e.Value)
				valSize := e.Value.CalcEncodedSize()
				now.valueBlock = encodeValue(valOffset, valSize)
			}
			if res == -1 {
				preElemHeaders[i] = pre
				break
			}
			pre = now
		}
	}
	len := 0
	for checkUp() {
		len++
	}
	element, elementOffset := newNode(list.arena, e.Key, e.Value, len)
	for i := 0; i <= len; i++ {
		element.levels[i] = preElemHeaders[i].levels[i]
		preElemHeaders[i].levels[i] = elementOffset
	}
}
