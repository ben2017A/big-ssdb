package xna

import (
	"fmt"
	"strings"
	"util"
)

type EntryType string

// begin, commit, rollbak, set, del, check
const(
	EntryTypeSet      = "set"
	EntryTypeDel      = "del"
	EntryTypeCheck    = "check"
	EntryTypeBegin    = "begin"
	EntryTypeCommit   = "commit"
	EntryTypeRollback = "rollback"
)

// Index 是指对应的 Binlog 的 Index, 所以两条 Entry 可能有相同的 Index
type Entry struct {
	Index int64
	Type EntryType
	Key string
	Val string
}

func DecodeEntry(buf string) *Entry{
	m := new(Entry)
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

// TODO: better encoding/decoding
func (e *Entry)Encode() string{
	if e.Type != EntryTypeSet && e.Type != EntryTypeDel {
		e.Key = util.I64toa(e.Index)
	}
	return fmt.Sprintf("%s %s %s", e.Type, e.Key, e.Val)
}

// TODO: better encoding/decoding
func (e *Entry)Decode(buf string) bool{
	ps := strings.SplitN(buf, " ", 3)
	if len(ps) != 3 {
		return false
	}
	e.Type = EntryType(ps[0])
	e.Key  = ps[1]
	e.Val  = ps[2]
	if e.Type != EntryTypeSet && e.Type != EntryTypeDel {
		e.Index = util.Atoi64(e.Key)
	}
	return true
}

func NewSetEntry(idx int64, key string, val string) *Entry {
	return &Entry{idx, EntryTypeSet, key, val}
}

func NewDelEntry(idx int64, key string) *Entry {
	return &Entry{idx, EntryTypeDel, key, "#"}
}
func NewCheckEntry(idx int64) *Entry {
	return &Entry{idx, EntryTypeCheck, "#", "#"}
}

func NewBeginEntry(idx int64) *Entry {
	return &Entry{idx, EntryTypeBegin, "#", "#"}
}

func NewCommitEntry(idx int64) *Entry {
	return &Entry{idx, EntryTypeCommit, "#", "#"}
}

func NewRollbackEntry(idx int64) *Entry {
	return &Entry{idx, EntryTypeRollback, "#", "#"}
}

