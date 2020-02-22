package store

import (
	"fmt"
	"os"
	"path"
	"bufio"

	"myutil"
)

type WALFile struct{
	fp *os.File
	Filename string
	scanner *bufio.Scanner
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

	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	_, err = fp.Seek(0, os.SEEK_END)
	if err != nil {
		fp.Close()
		return nil
	}

	ret := new(WALFile)
	ret.fp = fp
	ret.Filename = filename

	return ret
}

func (wal *WALFile)Close(){
	wal.fp.Close()
}

func (wal *WALFile)SeekToEnd() {
	wal.SeekTo(1 << 31)
}

// seek to n-th(0 based) record
func (wal *WALFile)SeekTo(n int) bool {
	_, err := wal.fp.Seek(0, os.SEEK_SET)
	if err != nil {
		return false
	}

	wal.scanner = bufio.NewScanner(wal.fp)
	for i := 0; i < n; i ++ {
		if !wal.scanner.Scan() {
			return false
		}
	}

	return true
}

func (wal *WALFile)Next() bool{
	return wal.scanner.Scan()
}

func (wal *WALFile)Item() string {
	return wal.scanner.Text()
}

func (wal *WALFile)Read() string{
	wal.Next()
	return wal.Item()
}

func (wal *WALFile)ReadLast() string{
	wal.SeekTo(0)
	var last string
	for wal.Next() {
		last = wal.Item()
	}
	return last
}

func (wal *WALFile)Append(record string) bool{
	record += "\n"
	buf := []byte(record)
	n, _ := wal.fp.Write(buf)
	return n == len(buf)
}
