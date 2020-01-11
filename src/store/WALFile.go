package store

import (
	"fmt"
	"os"
	"path"
	// "path/filepath"

	"myutil"
)

type WALFile struct{
	fp *File
}

// create if not exists
func OpenWALFile(filename string) *WALFile{
	dirname := path.Dir(filename)
	if !myutil.IsDir(dirname) {
		os.MkdirAll(dirname, 0755)
	}
	if !myutil.IsDir(dirname) {
		return nil
	}

	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	_, err := fp.Seek(0, os.SEEK_END)
	if err != nil {
		fp.Close()
		return nil
	}

	ret := new(WALFile)
	ret.fp = fp

	return ret
}

func (wal *WALFile)Close(){
	wal.fp.Close()
}

// seek to n-th record, return actual position of x-th record
func (wal *WALFile)Seek(n int) int {
	_, err := fp.Seek(0, os.SEEK_SET)
	if err != nil {
		return -1
	}
}

func (wal *WALFile)Append(record string){

}
