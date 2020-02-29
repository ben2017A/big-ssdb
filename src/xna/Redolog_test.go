package xna

import (
	"log"
	"testing"
	"fmt"
)

func TestRedolog(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	rd := OpenRedolog("tmp/redo.log")
	defer rd.Close()
	
	log.Println("last index", rd.LastIndex())

	rd.SeekAfterLastCheckpoint()
	for {
		tx := rd.NextTransaction()
		if tx == nil {
			break
		}
		for _, ent := range tx.Entries() {
			log.Println("redo", ent.Encode())
		}
	}
	
	return
	
	idx := rd.LastIndex() + 1
	tx := NewTransaction()
	for i :=0; i < 3; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("%d", i+1)
		tx.AddEntry(NewSetEntry(idx+int64(i), key, val))
	}
	tx.AddEntry(NewDelEntry(idx+int64(4), "a"))

	rd.WriteTransaction(tx)
	
	log.Println("last index", rd.LastIndex())
}
