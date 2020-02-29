package xna

import (
	"log"
)

// 内存中的事务
type Transaction struct {
	status int
	index int64
	mm map[string]*Entry
}

func NewTransaction(idx int64) *Transaction {
	tx := new(Transaction)
	tx.status = 0
	tx.index = idx
	tx.mm = make(map[string]*Entry)
	return tx
}

func (tx *Transaction)Committed() bool {
	return tx.status == 1
}

func (tx *Transaction)Empty() bool {
	return len(tx.mm) == 0
}

func (tx *Transaction)Index() int64 {
	return tx.index
}

func (tx *Transaction)Entries() []*Entry {
	arr := make([]*Entry, len(tx.mm))
	n := 0
	for _, v := range tx.mm {
		arr[n] = v
		n ++
	}
	return arr
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return tx.mm[key]
}

func (tx *Transaction)AddEntry(ent *Entry) {
	if ent.Type == EntryTypeSet || ent.Type == EntryTypeDel {
		tx.mm[ent.Key] = ent
	} else {
		log.Fatalf("invalid entry: %s", ent.Encode())
	}
}
