package xna

import (
	"fmt"
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
		ps := strings.Split(r, " ")
		if ps[0] == "commit" {
			ret.commitIndex = myutil.Atoi64(ps[1])
		}
	}
	
	return ret
}

func (rd *Redolog)CommitIndex int64 {
	return rd.commitIndex
}

func (rd *Redolog)SeekToLastCheckpoint() {
	rd.wal.SeekTo(0)
	
	num := 0
	last_check := 0
	for ret.wal.HasNext() {
		r := ret.wal.Next()
		ps := strings.Split(r, " ")
		if ps[0] == "check" {
			last_check = num
		}
		num += 1
	}
	
	rd.wal.SeekTo(last_check)
}

// 返回下一个事务, 如果文件中的事务不完整, 则忽略
func (rd *Redolog)NextTransaction() *Transaction {
}

// 增加 checkpoint
func (rd *Redolog)WriteCheckpoint() {
	rd.wal.Append("check")
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(tx *Transaction){
	if tx.MinIndex < rd.commitIndex || tx.MaxIndex < rd.commitIndex {
		return
	}
	
	rd.wal.Append("begin")
	for _, ent := range tx.Entries {
		line := fmt.Sprintf("%s %s %s", ent.Type, ent.Key, ent.Val)
		rd.wal.Append(line)
	}
	rd.wal.Append(fmt.Sprintf("commit %d", ent.MaxIndex))
	
	rd.commitIndex = tx.MaxIndex
}

