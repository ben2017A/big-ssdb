package xna

import (
	"testing"
)

func TestRedolog(t *testing.T){
	rd := OpenRedolog("tmp/redo.log")
	t.Log(rd, rd.CommitIndex())
	
	tx := NewTransaction()
	tx.AddEntry(&Entry{2, EntryTypeSet, "a", "1"})
	
	rd.WriteTransaction(tx)
	
	t.Log(rd, rd.CommitIndex())
}
