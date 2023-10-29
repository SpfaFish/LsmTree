package lsm

import (
	"CoreKV/Utils"
	"CoreKV/file"
	"os"
)

type Builder struct {
	buf              []byte
	maxSize          uint64
	blockMaxSize     uint64
	currentBlockSize uint64
	table            *SSTable
	file             *os.File
}

func (this *Builder) Add(entry Utils.Entry) {
	this.table.Add(entry)
	this.currentBlockSize += uint64(entry.CoderSize())
	if this.currentBlockSize >= this.blockMaxSize {
		this.table.Blocks = append(this.table.Blocks, NewBlock())
		this.currentBlockSize = 0
		this.table.BlockCount++
	}
}
func (this *Builder) flush(opt *file.Options) error {
	this.buf = make([]byte, this.table.CoderSize())
	coder := Utils.Coder{}
	coder.EncodeSSTable(this.buf, this.table)
	return file.WriteFile(opt, this.buf)
}
func (this *Builder) Search(opt *file.Options, key []byte, maxVs uint64) (*Utils.ValueStruct, error) {
	f, err := os.Open(opt.Path)
	if err != nil {
		return nil, err
	}
	return this.table.Search(key, maxVs, f)
}
