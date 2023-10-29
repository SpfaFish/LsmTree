package file

import (
	"os"
	"sync"
)

const (
	ManifestFileName = "ManifestFile"
)

type TableManifest struct {
	level    uint8
	checksum []byte
}
type Manifest struct {
	levels    []map[uint64]*struct{}
	tableMap  map[uint64]TableManifest
	Creations int
	Deletions int
}
type ManifestFile struct {
	opt      *Options
	file     *os.File
	lock     sync.Mutex
	manifest *Manifest
}

func newManifest() *Manifest {
	return &Manifest{
		levels:   make([]map[uint64]*struct{}, 0),
		tableMap: map[uint64]TableManifest{},
	}
}
func OpenMainfestFile(opt *Options) (*ManifestFile, error) {
	path := opt.Dir + "/" + ManifestFileName
	res := &ManifestFile{
		lock: sync.Mutex{},
		opt:  opt,
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil { // 检查文件是否存在
		if !os.IsNotExist(err) {
			return res, err
		}
		res.manifest = newManifest()
		file, err = os.Create(path)
		if err != nil {
			return res, err
		}
		res.file = file
		return res, nil
	}
	// 若有manifest文件则进行重放
	//to do
}
