package xna

import (
	"log"
	"store"
)

type Redolog struct{
	wal *store.WalFile
	path string
	lastIndex int64
	checkpoint int64
}

func OpenRedolog(path string) *Redolog {
	ret := new(Redolog)
	ret.path = path
	ret.checkpoint = 0
	
	if !ret.recover() {
		return nil
	}

	return ret
}

func (rd *Redolog)Close() {
	if 	rd.checkpoint != rd.lastIndex {
		rd.WriteCheckpoint()
	}
	rd.wal.Close()
}

func (rd *Redolog)recover() bool {
	rd.wal = store.OpenWalFile(rd.path)
	if rd.wal == nil {
		return false
	}
	
	rd.lastIndex = 0
	
	var begin int64 = 0
	rd.wal.SeekTo(0)
	for rd.wal.Next() {
		r := rd.wal.Item()
		ent := DecodeEntry(r)
		if ent == nil {
			log.Fatalf("invalid entry: %s", r)
			return false
		}

		switch ent.Type {
		case EntryTypeCheck:
			if begin > 0 {
				log.Fatalf("invalid '%s' after 'begin': %s", ent.Type, r)
				return false
			}
			rd.checkpoint = ent.Index
		case EntryTypeBegin:
			if begin > 0 {
				log.Fatalf("invalid '%s' after 'begin': %s", ent.Type, r)
				return false
			}
			begin = ent.Index
		case EntryTypeCommit:
			begin = 0
			rd.lastIndex = ent.Index
		case EntryTypeRollback:
			begin = 0
		default:
			if begin == 0 {
				log.Fatalf("invalid '%s' before 'begin': %s", ent.Type, r)
				return false
			}
		}
	}
	if begin > 0 {
		ent := NewRollbackEntry(begin)
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
	rd.checkpoint = rd.lastIndex
}

func (rd *Redolog)WriteTransaction(tx *Transaction) bool {
	if tx.MinIndex() <= rd.lastIndex {
		log.Fatalf("bad transaction, MinIndex: %d, when lastIndex: %d", tx.MinIndex(), rd.lastIndex)
		return false
	}
	
	// 如果出错, 可能无法将完整的 transaction 完全写入, 所以 NextTransaction 会处理这种情况
	rd.wal.Append(NewBeginEntry(tx.MinIndex()).Encode())
	for _, ent := range tx.Entries() {
		rd.wal.Append(ent.Encode())
	}
	rd.wal.Append(NewCommitEntry(tx.MaxIndex()).Encode())
	
	rd.lastIndex = tx.MaxIndex()
	return true
}

