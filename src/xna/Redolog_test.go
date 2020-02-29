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
			log.Println(ent)
		}
	}
	
	return
	
	tx := NewTransaction()
	for i :=0; i < 3; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("%d", i+1)
		idx := rd.LastIndex() + 1 + int64(i)
		tx.AddEntry(Entry{idx, EntryTypeSet, key, val})
	}
	
	rd.WriteTransaction(tx)
	rd.WriteCheckpoint()
	
	log.Println(rd.LastIndex())
}
