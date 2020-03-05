package ssdb

import (
	"fmt"
	"strings"
	"util"
)

type RedoType string

// begin, commit, rollback, set, del, check
const(
	RedoTypeSet      = "set"
	RedoTypeDel      = "del"
	RedoTypeCheck    = "check"
	RedoTypeBegin    = "begin"
	RedoTypeCommit   = "commit"
	RedoTypeRollback = "rollback"
)

// Index 是指对应的 Binlog 的 Index, 所以两条 RedoEntry 可能有相同的 Index
type RedoEntry struct {
	Index int64
	Type RedoType
	Key string
	Val string
}

func DecodeRedoEntry(buf string) *RedoEntry{
	m := new(RedoEntry)
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

// TODO: better encoding/decoding
func (e *RedoEntry)Encode() string{
	if e.Type != RedoTypeSet && e.Type != RedoTypeDel {
		e.Key = util.I64toa(e.Index)
	}
	return fmt.Sprintf("%s %s %s", e.Type, e.Key, e.Val)
}

// TODO: better encoding/decoding
func (e *RedoEntry)Decode(buf string) bool{
	ps := strings.SplitN(buf, " ", 3)
	if len(ps) != 3 {
		return false
	}
	e.Type = RedoType(ps[0])
	e.Key  = ps[1]
	e.Val  = ps[2]
	if e.Type != RedoTypeSet && e.Type != RedoTypeDel {
		e.Index = util.Atoi64(e.Key)
	}
	return true
}

func NewRedoSetEntry(idx int64, key string, val string) *RedoEntry {
	return &RedoEntry{idx, RedoTypeSet, key, val}
}

func NewRedoDelEntry(idx int64, key string) *RedoEntry {
	return &RedoEntry{idx, RedoTypeDel, key, "#"}
}
func NewRedoCheckEntry(idx int64) *RedoEntry {
	return &RedoEntry{idx, RedoTypeCheck, "#", "#"}
}

func NewRedoBeginEntry(idx int64) *RedoEntry {
	return &RedoEntry{idx, RedoTypeBegin, "#", "#"}
}

func NewRedoCommitEntry(idx int64) *RedoEntry {
	return &RedoEntry{idx, RedoTypeCommit, "#", "#"}
}

func NewRedoRollbackEntry(idx int64) *RedoEntry {
	return &RedoEntry{idx, RedoTypeRollback, "#", "#"}
}

