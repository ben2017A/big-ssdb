package xna

import (
	"sort"
	"util"
)

// 内存中的事务
type Transaction struct {
	status int
	minIndex int64
	maxIndex int64
	mm map[string]*Entry
}

func NewTransaction() *Transaction {
	ret := new(Transaction)
	ret.Reset()
	return ret
}

func (tx *Transaction)Reset() {
	tx.status = 0
	tx.minIndex = 0
	tx.maxIndex = 0
	tx.mm = make(map[string]*Entry)
}

func (tx *Transaction)Committed() bool {
	return tx.status == 1
}

func (tx *Transaction)Empty() bool {
	return len(tx.mm) == 0
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
	sort.Slice(arr, func(i, j int) bool{
		return arr[i].Index < arr[j].Index
	})
	return arr
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return tx.mm[key]
}

func (tx *Transaction)AddEntry(ent *Entry) {
	if ent.Index > 0 {
		if tx.minIndex == 0 {
			tx.minIndex = ent.Index
		} else {
			tx.minIndex = util.MinInt64(tx.minIndex, ent.Index)
		}
	}
	tx.maxIndex = util.MaxInt64(tx.maxIndex, ent.Index)

	if ent.Type == EntryTypeSet || ent.Type == EntryTypeDel {
		tx.mm[ent.Key] = ent
	}
}
