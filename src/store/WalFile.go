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
func OpenWalFile(filename string) *WalFile {
	dir := path.Dir(filename)
	if !util.IsDir(dir) {
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

func (wal *WalFile)Close() {
	wal.fp.Close()
}

func (wal *WalFile)Clean() error {
	if err := wal.fp.Truncate(0); err != nil {
		return err
	}
	if _, err := wal.fp.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	if err := wal.Fsync(); err != nil {
		return err
	}
	return nil
}

func (wal *WalFile)SeekToEnd() {
	wal.SeekTo(1 << 31)
}

// seek to *BEFORE* n-th(0 based) record
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

// must call Next() before calling Item()
func (wal *WalFile)Item() string {
	bs := decode([]byte(wal.scanner.Text()))
	return string(bs)
}

func (wal *WalFile)Read() string {
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

func (wal *WalFile)Fsync() error {
	return wal.fp.Sync()
}

func (wal *WalFile)Append(record string) bool {
	buf := encode([]byte(record))
	n, _ := wal.fp.Write(buf)
	return n == len(buf)
}

func encode(s []byte) []byte {
	buf := util.BytesEscape(s)
	buf = append(buf, '\n')
	return buf
}

func decode(s []byte) []byte {
	return util.BytesUnescape(s)
}
