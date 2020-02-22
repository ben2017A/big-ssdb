package xna

type EntryType string

const(
	EntrySet = "set"
	EntryDel = "del"
)

type Entry struct {
	Index int64
	Type EntryType
	Key string
	Val string
}
