package store

import (
	"testing"
)

func TestWALFile(t *testing.T){
	a := OpenWALFile("tmp/a.wal")
	t.Log(a)
}

