package xna

import (
	"util"
	"sort"
)

// 内存中的事务
type Transaction struct {
	minIndex int64
	maxIndex int64
	mm map[string]*Entry
}

func NewTransaction() *Transaction {
	tx := new(Transaction)
	tx.mm = make(map[string]*Entry)
	return tx
}

func (tx *Transaction)MinIndex() int64 {
	return tx.minIndex
}

func (tx *Transaction)MaxIndex() int64 {
	return tx.maxIndex
}

func (tx *Transaction)Entries() []*Entry {
	arr := make([]*Entry, len(tx.mm))
	n := 0
	for _, v := range tx.mm {
		arr[n] = v
		n ++
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Index < arr[j].Index
	})
	return arr
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return tx.mm[key]
}

func (tx *Transaction)AddEntry(ent *Entry) {
	if tx.minIndex == 0 {
		tx.minIndex = ent.Index
	} else {
		tx.minIndex = util.MinInt64(tx.minIndex, ent.Index)
	}
	tx.maxIndex = util.MaxInt64(tx.maxIndex, ent.Index)

	if ent.Type == EntryTypeSet || ent.Type == EntryTypeDel {
		tx.mm[ent.Key] = ent
	} else if ent.Type == EntryTypeRollback {
		tx.mm = make(map[string]*Entry)
	}
}
