package xna

import (
	"myutil"
)

// 内存中的事务
type Transaction struct {
	status int
	minIndex int64
	maxIndex int64
	entries map[string]*Entry
}

func NewTransaction() *Transaction {
	ret := new(Transaction)
	ret.status = 0
	ret.minIndex = 0
	ret.maxIndex = 0
	ret.entries = make(map[string]*Entry)
	return ret
}

func (tx *Transaction)Committed() bool {
	return tx.status == 1
}

func (tx *Transaction)MinIndex() int64 {
	return tx.minIndex
}

func (tx *Transaction)MaxIndex() int64 {
	return tx.maxIndex
}

func (tx *Transaction)Entries() map[string]*Entry {
	return tx.entries
}

func (tx *Transaction)BeginEntry() *Entry {
	return NewBeginEntry(tx.minIndex)
}

func (tx *Transaction)CommitEntry() *Entry {
	return NewCommitEntry(tx.maxIndex)
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return tx.entries[key]
}

func (tx *Transaction)AddEntry(ent *Entry) {
	if ent.Index > 0 {
		if tx.minIndex == 0 {
			tx.minIndex = ent.Index
		} else {
			tx.minIndex = myutil.MinInt64(tx.minIndex, ent.Index)
		}
	}
	tx.maxIndex = myutil.MaxInt64(tx.maxIndex, ent.Index)

	if ent.Type == EntryTypeSet || ent.Type == EntryTypeDel {
		tx.entries[ent.Key] = ent
	}
}
