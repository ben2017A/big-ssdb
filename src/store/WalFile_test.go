package store

import (
	"testing"
	"os"
	"path"
	"util"
)

func TestWalFile(t *testing.T){
	filename := "tmp/a.wal"
	os.Remove(filename)

	dir := path.Dir(filename)
	if !util.IsDir(dir) {
		os.MkdirAll(dir, 0755)
	}

	wal := OpenWalFile(filename)
	defer wal.Close()

	wal.Append("0")
	wal.Append("1")
	wal.Append("2")

	var s string
	wal.SeekTo(1)
	s = wal.Read()
	if s != "1" {
		t.Fatal("")
	}

	wal.SeekTo(0)
	s = wal.Read()
	if s != "0" {
		t.Fatal("")
	}

	wal.SeekTo(2)
	s = wal.Read()
	if s != "2" {
		t.Fatal("")
	}
}

