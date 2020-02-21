package store

// 内存中的事务
type Transaction struct {
	minIndex int64
	maxIndex int64
}
