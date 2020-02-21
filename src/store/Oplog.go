package store

// 幂等的操作
type Oplog struct{
	Index int64
	Type string // begin, commit, set, del, check
	Key string
	Val string
}