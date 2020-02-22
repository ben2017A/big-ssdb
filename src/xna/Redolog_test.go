package xna

import (
	"testing"
	"fmt"
)

func TestRedolog(t *testing.T){
	rd := OpenRedolog("tmp/redo.log")
	defer rd.Close()
	
	fmt.Println(rd.CommitIndex())

	rd.SeekToLastCheckpoint()
	for {
		tx := rd.NextTransaction()
		if tx == nil {
			break
		}
		fmt.Println(tx)
	}
	
	tx := NewTransaction()
	for i :=0; i < 3; i++ {
		key := fmt.Sprintf("k-%d", i)
		val := fmt.Sprintf("%d", i+1)
		idx := rd.CommitIndex() + 1 + int64(i)
		tx.AddEntry(&Entry{idx, EntryTypeSet, key, val})
	}
	fmt.Println(tx.BeginEntry(), tx.CommitEntry())
	
	rd.WriteTransaction(tx)
	rd.WriteCheckpoint()
	
	fmt.Println(rd.CommitIndex())
}
