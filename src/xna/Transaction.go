package xna

import (
	"myutil"
)

// 内存中的事务
type Transaction struct {
	status int
	MinIndex int64
	MaxIndex int64
	entries map[string]*Entry
}

func NewTransaction() *Transaction {
	ret := new(Transaction)
	ret.status = 0
	ret.MinIndex = 0
	ret.MaxIndex = 0
	ret.entries = make(map[string]*Entry)
	return ret
}

func (tx *Transaction)Committed() bool {
	return tx.status == 1
}

func (tx *Transaction)Entries() map[string]*Entry {
	return tx.entries
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return tx.entries[key]
}

func (tx *Transaction)AddEntry(ent *Entry) {
	if tx.MinIndex == 0 {
		tx.MinIndex = ent.Index
	} else {
		tx.MinIndex = myutil.MinInt64(tx.MinIndex, ent.Index)
	}
	tx.MaxIndex = myutil.MaxInt64(tx.MaxIndex, ent.Index)
	tx.entries[ent.Key] = ent
}
