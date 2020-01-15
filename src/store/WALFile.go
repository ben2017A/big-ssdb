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

	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0744)
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

func (wal *WALFile)Read() string{
	if !wal.scanner.Scan() {
		return ""
	}
	return wal.scanner.Text()
}

func (wal *WALFile)ReadLast() string{
	wal.SeekTo(0)
	var last string
	for {
		b := wal.Read()
		if b == "" {
			break;
		}
		last = b
	}
	return last
}

func (wal *WALFile)Append(record string) bool{
	record += "\n"
	buf := []byte(record)
	n, _ := wal.fp.Write(buf)
	return n == len(buf)
}
