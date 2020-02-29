package xna

import (
	"log"
	"store"
)

type Redolog struct{
	wal *store.WalFile
	commitIndex int64
}

func OpenRedolog(filename string) *Redolog {
	ret := new(Redolog)
	ret.wal = store.OpenWalFile(filename)
	ret.commitIndex = 0
	
	ret.wal.SeekTo(0)
	for ret.wal.Next() {
		r := ret.wal.Item()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCommit {
			ret.commitIndex = ent.Index
		}
	}
	
	return ret
}

func (rd *Redolog)Close(){
	rd.wal.Close()
}

func (rd *Redolog)CommitIndex() int64 {
	return rd.commitIndex
}

func (rd *Redolog)SeekToLastCheckpoint() {
	rd.wal.SeekTo(0)
	
	num := 0
	lineno := 0
	for rd.wal.Next() {
		r := rd.wal.Item()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCheck {
			lineno = num
		}
		num += 1
	}
	
	rd.wal.SeekTo(lineno)
}

// 返回下一个事务, 如果文件中的事务不完整, 则忽略
func (rd *Redolog)NextTransaction() *Transaction {
	tx := NewTransaction()
	for rd.wal.Next() {
		r := rd.wal.Item()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCheck {
			continue
		}
		
		tx.AddEntry(*ent)
		if ent.Type == EntryTypeCommit {
			return tx
		}
	}
	return nil
}

// 增加 checkpoint
func (rd *Redolog)WriteCheckpoint() {
	rd.wal.Append(NewCheckEntry(rd.commitIndex).Encode())
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(tx *Transaction){
	if tx.MinIndex() <= rd.commitIndex || tx.MaxIndex() <= rd.commitIndex {
		log.Fatalf("bad transaction, minIndex: %d, maxIndex: %d, when commitIndex: %d\n",
			tx.MinIndex(), tx.MaxIndex(), rd.commitIndex)
		return
	}
	
	rd.wal.Append(tx.BeginEntry().Encode())
	for _, ent := range tx.Entries() {
		rd.wal.Append(ent.Encode())
	}
	rd.wal.Append(tx.CommitEntry().Encode())
	
	rd.commitIndex = tx.MaxIndex()
}

