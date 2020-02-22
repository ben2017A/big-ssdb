package xna

import (
	"store"
)

// begin, commit, set, del, check
type Redolog struct{
	wal *store.WALFile
	commitIndex int64
}

func (rd *Redolog)SeekToLastCheckPoint() {
	
}

// 返回下一个事务, 如果文件中的事务不完整, 则忽略
func (rd *Redolog)NextTransaction() *Transaction {
}

func (rd *Redolog)Fsync() {
}

// 增加 checkpoint
func (rd *Redolog)WriteCheckPoint() {
	
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(trx *Transaction){
	
}

