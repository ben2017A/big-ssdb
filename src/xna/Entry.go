package xna

import (
	"fmt"
	"strings"
	"util"
)

type EntryType string

// begin, commit, set, del, check
const(
	EntryTypeSet = "set"
	EntryTypeDel = "del"
	EntryTypeCheck = "check"
	EntryTypeBegin = "begin"
	EntryTypeCommit = "commit"
)

// Index 是指对应的 Binlog 的 Index, 所以两条 Entry 可能有相同的 Index
type Entry struct {
	Index int64
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

func (e *Entry)Encode() string{
	return fmt.Sprintf("%d %s %s %s", e.Index, e.Type, e.Key, e.Val)
}

func (e *Entry)Decode(buf string) bool{
	ps := strings.Split(buf, " ")
	e.Index = util.Atoi64(ps[0])
	e.Type = EntryType(ps[1])
	e.Key = ps[2]
	e.Val = ps[3]
	return true
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
