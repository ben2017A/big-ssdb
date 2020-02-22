package xna

// 内存中的事务
type Transaction struct {
	status int
	MinIndex int64
	MaxIndex int64
}

func (tx *Transaction)Committed() bool {
	return tx.status == 1
}

func (tx *Transaction)GetEntry(key string) *Entry {
	return nil
}
