package store

import (
	"testing"
	// "os"
)

func TestSSTFile(t *testing.T){
	filename := "tmp/a.sst"
	// os.Remove(filename)

	sst := OpenSSTFile(filename)
	defer sst.Close()

	// kvs := make(map[string]string)
	// kvs["a"] = "1"
	// kvs["c"] = "3"
	// kvs["b"] = "2"
	// t.Log(kvs)
	// sst.Save(kvs)

	t.Log(sst.Get("a"))
	t.Log(sst.Get("c"))
	t.Log(sst.Get("b"))
	t.Log("")

	sst.Seek("")
	for sst.Valid() {
		k, v := sst.Read()
		t.Log(k, v)
	}
	t.Log("")

	sst.Seek("a")
	for sst.Valid() {
		k, v := sst.Read()
		t.Log(k, v)
	}
	t.Log("")

	sst.Seek("b")
	for sst.Valid() {
		k, v := sst.Read()
		t.Log(k, v)
	}
	t.Log("")

	sst.Seek("c")
	for sst.Valid() {
		k, v := sst.Read()
		t.Log(k, v)
	}
	t.Log("")
}
