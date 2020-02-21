package store

type Redolog struct{
	wal *WALFile
}

func (rd *Redolog)SeekToLastCheckpoint() {
	
}

func (rd *Redolog)Fsync() {
}

// 增加 checkpoint
func (rd *Redolog)Check() {
	
}

func (rd *Redolog)Begin(idx int64) {
}

func (rd *Redolog)Commit(idx int64) {
}

func (rd *Redolog)Set(idx int64, key string, val string) {
	
}

func (rd *Redolog)Del(idx int64, key string) {
	
}
