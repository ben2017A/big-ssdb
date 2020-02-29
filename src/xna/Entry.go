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
	Type EntryType
	Key string
	Val string
}

func DecodeEntry(buf string) *Entry{
	m := new(Entry);
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

// TODO: better encoding/decoding
func (e *Entry)Encode() string{
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
	return true
}

func (e *Entry)Index() int64 {
	return util.Atoi64(e.Key)
}

func NewSetEntry(key string, val string) *Entry {
	return &Entry{EntryTypeSet, key, val}
}

func NewDelEntry(key string) *Entry {
	return &Entry{EntryTypeDel, key, "#"}
}
func NewCheckEntry(idx int64) *Entry {
	return &Entry{EntryTypeCheck, util.I64toa(idx), "#"}
}

func NewBeginEntry(idx int64) *Entry {
	return &Entry{EntryTypeBegin, util.I64toa(idx), "#"}
}

func NewCommitEntry(idx int64) *Entry {
	return &Entry{EntryTypeCommit, util.I64toa(idx), "#"}
}

func NewRollbackEntry(idx int64) *Entry {
	return &Entry{EntryTypeRollback, util.I64toa(idx), "#"}
}

