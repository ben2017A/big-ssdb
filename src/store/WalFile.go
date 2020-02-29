package store

import (
	"fmt"
	"os"
	"path"
	"bufio"

	"util"
)

type WalFile struct{
	fp *os.File
	Filename string
	scanner *bufio.Scanner
}

// create if not exists
func OpenWalFile(filename string) *WalFile{
	dirname := path.Dir(filename)
	if !util.IsDir(dirname) {
		os.MkdirAll(dirname, 0755)
	}
	if !util.IsDir(dirname) {
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

	ret := new(WalFile)
	ret.fp = fp
	ret.Filename = filename

	return ret
}

func (wal *WalFile)Close(){
	wal.fp.Close()
}

func (wal *WalFile)SeekToEnd() {
	wal.SeekTo(1 << 31)
}

// seek to n-th(0 based) record
func (wal *WalFile)SeekTo(n int) bool {
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

func (wal *WalFile)Next() bool{
	return wal.scanner.Scan()
}

func (wal *WalFile)Item() string {
	return wal.scanner.Text()
}

func (wal *WalFile)Read() string{
	wal.Next()
	return wal.Item()
}

func (wal *WalFile)ReadLast() string{
	wal.SeekTo(0)
	var last string
	for wal.Next() {
		last = wal.Item()
	}
	return last
}

// record.indexOf('\n') == false
func (wal *WalFile)Append(record string) bool{
	record += "\n"
	buf := []byte(record)
	n, _ := wal.fp.Write(buf)
	return n == len(buf)
}
