package raft

import (
	"fmt"
	"strings"

	"myutil"
)

type Entry struct{
	Type string // Heartbeat, AddMember, DelMember, Write, Commit
	Index uint64
	Term uint32
	CommitIndex uint64
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
	e.Index = myutil.Atou64(ps[1])
	e.Term = myutil.Atou(ps[2])
	e.CommitIndex = myutil.Atou64(ps[3])
	e.Data = ps[4]
	return e
}

func (e *Entry)Encode() string{
	return fmt.Sprintf("%s %d %d %d %s", e.Type, e.Index, e.Term, e.CommitIndex, e.Data)
}
