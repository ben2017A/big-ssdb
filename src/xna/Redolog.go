package xna

import (
	"fmt"
	"log"
	"store"
)

type Redolog struct{
	wal *store.WALFile
	commitIndex int64
}

func OpenRedolog(filename string) *Redolog {
	ret := new(Redolog)
	ret.wal = OpenWALFile(filename)
	ret.commitIndex = 0
	
	ret.wal.SeekTo(0)
	for ret.wal.HasNext() {
		r := ret.wal.Next()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCommit {
			ret.commitIndex = ent.Index
		}
	}
	
	return ret
}

func (rd *Redolog)CommitIndex() int64 {
	return rd.commitIndex
}

func (rd *Redolog)SeekToLastCheckpoint() {
	rd.wal.SeekTo(0)
	
	num := 0
	last_check := 0
	for ret.wal.HasNext() {
		r := ret.wal.Next()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCheck {
			last_check = num
		}
		num += 1
	}
	
	rd.wal.SeekTo(last_check)
}

// 返回下一个事务, 如果文件中的事务不完整, 则忽略
func (rd *Redolog)NextTransaction() *Transaction {
	tx := new(Transaction)
	for ret.wal.HasNext() {
		r := ret.wal.Next()
		ent := DecodeEntry(r)
		tx.AddEntry(ent)
		if ent.Type == EntryTypeCommit {
			break
		}
	}
	return tx
}

// 增加 checkpoint
func (rd *Redolog)WriteCheckpoint() {
	rd.wal.Append(NewCheckEntry().Encode())
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(tx *Transaction){
	if tx.MinIndex() < rd.commitIndex || tx.MaxIndex() < rd.commitIndex {
		log.Fatalln("bad transaction, minIndex: %d, maxIndex: %d", tx.MinIndex(), tx.MaxIndex())
		return
	}
	
	rd.wal.Append(tx.BeginEntry().Encode())
	for _, ent := range tx.Entries {
		rd.wal.Append(ent.Encode())
	}
	rd.wal.Append(tx.CommitEntry().Encode())
	
	rd.commitIndex = tx.MaxIndex()
}

