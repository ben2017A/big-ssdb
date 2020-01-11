package store

import (
	"testing"
	"os"
)

func TestWALFile(t *testing.T){
	filename := "tmp/a.wal"
	os.Remove(filename)
	wal := OpenWALFile(filename)
	defer wal.Close()

	wal.Append("0")
	wal.Append("1")
	wal.Append("2")

	var s string
	wal.Seek(1)
	s = wal.Read()
	if s != "1" {
		t.Fatal("")
	}

	wal.Seek(0)
	s = wal.Read()
	if s != "0" {
		t.Fatal("")
	}

	wal.Seek(2)
	s = wal.Read()
	if s != "2" {
		t.Fatal("")
	}
}

