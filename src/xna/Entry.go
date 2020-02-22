package xna

type EntryType string

// begin, commit, set, del, check
const(
	EntrySet = "set"
	EntryDel = "del"
)

// Index 是指对应的 Binlog 的 Index, 所以两条 Entry 可能有相同的 Index
type Entry struct {
	Index int64
	Type EntryType
	Key string
	Val string
}
