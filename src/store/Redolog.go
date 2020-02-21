package store

// begin, commit, set, del, check
type Redolog struct{
	wal *WALFile
	commitIndex int64
}

func (rd *Redolog)SeekToLastCheckpoint() {
	
}

func (rd *Redolog)Fsync() {
}

// 增加 checkpoint
func (rd *Redolog)Check() {
	
}

// 如果出错, 可能无法将完整的 transaction 完全写入
func (rd *Redolog)WriteTransaction(trx *Transaction){
	
}

