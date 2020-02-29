package ssdb

import (
	"log"
	// "fmt"
	"testing"
	// "os"
)

func TestDb(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	db := OpenDb("./tmp")
	defer db.Close()
	
	idx := int64(0)

	idx ++
	db.Set(idx, "a", "1")
	db.Set(idx, "b", "2")
	db.Set(idx, "c", "3")
	db.Set(idx, "d", "4")

	log.Println(db.Get("b"))
	db.Del(0, "b")
	log.Println(db.Get("b"))
	db.Del(0, "x")

}
