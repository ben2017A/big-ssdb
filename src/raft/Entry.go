package raft

import (
	"fmt"
	"strings"
)

type Entry struct{
	Type string // Heartbeat, AddMember, DelMember, Entry
	CommitIndex uint64
	Index uint64
	Term uint32
	Data string
}

func DecodeEntry(buf string) *Entry{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 5)
	if len(ps) != 5 {
		return nil
	}

	e := new(Entry);
	e.Type = ps[0]
	e.CommitIndex = Atou64(ps[1])
	e.Index = Atou64(ps[2])
	e.Term = Atou(ps[3])
	e.Data = ps[4]
	return e
}

func (e *Entry)Encode() string{
	return fmt.Sprintf("%s %d %d %d %s", e.Type, e.CommitIndex, e.Index, e.Term, e.Data)
}
