package raft

import (
	"fmt"
	"strings"

	"util"
)

type EntryType string

const(
	EntryTypeBeat      = "Beat"
	EntryTypeNoop      = "Noop"
	EntryTypeConf      = "Conf"
	EntryTypeData      = "Data"
)

type Entry struct{
	Term int32
	Index int64
	Commit int64
	Type EntryType
	Data string
}

func DecodeEntry(buf string) *Entry {
	m := new(Entry);
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

func (e *Entry)Encode() string{ 
	return fmt.Sprintf("%d %d %d %s %s", e.Term, e.Index, e.Commit, e.Type, e.Data)
}

func (e *Entry)Decode(buf string) bool {
	ps := strings.SplitN(buf, " ", 5)
	if len(ps) != 5 {
		return false
	}

	e.Term = util.Atoi32(ps[0])
	e.Index = util.Atoi64(ps[1])
	e.Commit = util.Atoi64(ps[2])
	e.Type = EntryType(ps[3])
	e.Data = ps[4]
	return true
}

func NewHearteatEntry(commitIndex int64) *Entry {
	ent := new(Entry)
	ent.Type = EntryTypeBeat
	ent.Term = 0
	ent.Index = 0
	ent.Commit = commitIndex
	return ent
}
