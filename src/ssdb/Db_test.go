package ssdb

import (
	"log"
	// "fmt"
	"testing"
	// "os"
)

func TestDb(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	db := OpenDb("./tmp/db")
	defer db.Close()
	
	idx := db.CommitIndex()
	log.Println("commit index", idx)

	// idx ++
	// db.Set(idx, "a", "1")
	// idx ++
	// db.Set(idx, "b", "2")
	// idx ++
	// db.Set(idx, "c", "3")
	// idx ++
	// db.Set(idx, "d", "4")

	// log.Println(db.Get("b"))
	// idx ++
	// db.Del(idx, "b")
	// log.Println(db.Get("b"))
	// idx ++
	// db.Del(idx, "x")

	
	db.MakeFileSnapshot("./tmp/snapshot.db")
}
