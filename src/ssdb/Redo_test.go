package ssdb

import (
	"log"
	"testing"
	"fmt"
	"util"
	"os"
)

func TestRedolog(t *testing.T){
	if !util.IsDir("tmp") {
		os.MkdirAll("tmp", 0755)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	rd := OpenRedoManager("tmp/redo.log")
	log.Println(rd)
	defer rd.Close()
	
	log.Println("commit index", rd.CommitIndex())

	ents := rd.GetAllUncheckedEntries()
	for _, ent := range ents {
		log.Println("    redo", ent.Encode())
	}
	
	idx := rd.CommitIndex() + 1
	for i :=0; i < 3; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("%d", i+1)
		rd.Set(idx+int64(i), key, val)
	}
		
	log.Println("last index", rd.CommitIndex())
}
