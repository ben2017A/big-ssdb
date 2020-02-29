package xna

import (
	"log"
	"store"
)

type Redolog struct{
	wal *store.WalFile
	lastIndex int64
	path string
}

func OpenRedolog(path string) *Redolog {
	ret := new(Redolog)
	ret.path = path
	
	if !ret.recover() {
		return nil
	}

	return ret
}

func (rd *Redolog)Close() {
	rd.wal.Close()
}

func (rd *Redolog)recover() bool {
	rd.wal = store.OpenWalFile(rd.path)
	if rd.wal == nil {
		return false
	}
	
	rd.lastIndex = 0
	
	after_begin := false
	rd.wal.SeekTo(0)
	for rd.wal.Next() {
		r := rd.wal.Item()
		ent := DecodeEntry(r)

		switch ent.Type {
		case EntryTypeCheck:
			if after_begin {
				log.Fatalf("invalid '%s' after 'begin': %s", ent.Type, r)
				return false
			}
		case EntryTypeBegin:
			after_begin = true
		case EntryTypeCommit:
			after_begin = false
			rd.lastIndex = ent.Index
		case EntryTypeRollback:
			after_begin = false
		default:
			if !after_begin {
				log.Fatalf("invalid '%s' before 'begin': %s", ent.Type, r)
				return false
			}
		}
	}
	
	if after_begin {
		ent := NewRollbackEntry()
		rd.wal.Append(ent.Encode())
	}

	return true
}

func (rd *Redolog)LastIndex() int64 {
	return rd.lastIndex
}

func (rd *Redolog)SeekAfterLastCheckpoint() {
	lineno := 0
	rd.wal.SeekTo(0)
	for num := 0; rd.wal.Next(); num ++ {
		r := rd.wal.Item()
		ent := DecodeEntry(r)
		if ent.Type == EntryTypeCheck {
			lineno = num + 1
		}
	}
	rd.wal.SeekTo(lineno)
}

// 返回下一个事务, 如果文件中的事务不完整, 则忽略
func (rd *Redolog)NextTransaction() *Transaction {
	var tx *Transaction = nil
	for rd.wal.Next() {
		r := rd.wal.Item()
		ent := DecodeEntry(r)
		if ent == nil {
			log.Fatalf("bad entry: %s", r)
			break
		}
		
		switch ent.Type {
		case EntryTypeCheck:
			//
		case EntryTypeBegin:
			tx = NewTransaction()
		case EntryTypeCommit:
			return tx
		case EntryTypeRollback:
			tx = nil
		default:
			if tx == nil {
				log.Fatalf("bad entry before begin: %s", r)
				return nil
			}
			tx.AddEntry(ent)
		}
	}
	return nil
}

// 增加 checkpoint
func (rd *Redolog)WriteCheckpoint() {
	rd.wal.Append(NewCheckEntry(rd.lastIndex).Encode())
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(tx *Transaction){
	if tx.MinIndex() <= rd.lastIndex || tx.MaxIndex() <= rd.lastIndex {
		log.Fatalf("bad transaction, minIndex: %d, maxIndex: %d, when lastIndex: %d\n",
			tx.MinIndex(), tx.MaxIndex(), rd.lastIndex)
		return
	}
	
	rd.wal.Append(NewBeginEntry(tx.MinIndex()).Encode())
	for _, ent := range tx.Entries() {
		rd.wal.Append(ent.Encode())
	}
	rd.wal.Append(NewCommitEntry(tx.MaxIndex()).Encode())
	
	rd.lastIndex = tx.MaxIndex()
}

