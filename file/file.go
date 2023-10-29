package file

import (
	"os"
	"path/filepath"
)

type Options struct {
	FID      uint64
	FileName string
	Dir      string //文件夹
	Path     string //路径
	Flag     int
	MaxSz    int
}
func OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir}){

}
func WriteFile(opt *Options, buf []byte) error {
	filePath := filepath.Join(opt.Path, opt.FileName)
	file, err := os.Create(filePath)
	defer file.Close()
	if err != nil{
		return err
	}
	_, err = file.Write(buf)
	return err
}