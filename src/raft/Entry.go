package raft

import (
	"fmt"
	"strings"

	"myutil"
)

type Entry struct{
	Index uint64
	Term uint32
	CommitIndex uint64
	Type string // Heartbeat, AddMember, DelMember, Write, Commit
	Data string
}

func DecodeEntry(buf string) *Entry{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 5)
	if len(ps) != 5 {
		return nil
	}

	e := new(Entry);
	e.Index = myutil.Atou64(ps[0])
	e.Term = myutil.Atou(ps[1])
	e.CommitIndex = myutil.Atou64(ps[2])
	e.Type = ps[3]
	e.Data = ps[4]
	return e
}

func (e *Entry)Encode() string{
	return fmt.Sprintf("%d %d %d %s %s", e.Index, e.Term, e.CommitIndex, e.Type, e.Data)
}

func NewHeartbeatEntry(commitIndex uint64) *Entry{
	ent := new(Entry)
	ent.Type = "Heartbeat"
	ent.Index = 0
	ent.Term = 0
	ent.CommitIndex = commitIndex
	return ent
}

func NewCommitEntry(commitIndex uint64) *Entry{
	ent := new(Entry)
	ent.Type = "Commit"
	ent.Index = 0
	ent.Term = 0
	ent.CommitIndex = commitIndex
	return ent
}
