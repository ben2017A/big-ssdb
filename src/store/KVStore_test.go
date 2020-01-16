package store

import (
	"log"
	"testing"
	// "os"
)

func TestKVStore(t *testing.T){
	db := OpenKVStore("./tmp/kvdb")
	defer db.Close()

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")
	db.Set("d", "4")

	log.Println(db.Get("b"))
	db.Del("b")
	log.Println(db.Get("b"))
	db.Del("x")
}
