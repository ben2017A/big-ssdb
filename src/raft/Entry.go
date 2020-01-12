package raft

import (
	"fmt"
	"strings"

	"myutil"
)

// Heartbeat: tell follower to commit
// Commit: tell storage to commit

type Entry struct{
	Index uint64
	Term uint32
	CommitIndex uint64
	Type string // Heartbeat, AddMember, DelMember, Noop, Write, Commit
	Data string
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
	return fmt.Sprintf("%d %d %d %s %s", e.Index, e.Term, e.CommitIndex, e.Type, e.Data)
}

func (e *Entry)Decode(buf string) bool{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 5)
	if len(ps) != 5 {
		return false
	}

	e.Index = myutil.Atou64(ps[0])
	e.Term = myutil.Atou(ps[1])
	e.CommitIndex = myutil.Atou64(ps[2])
	e.Type = ps[3]
	e.Data = ps[4]
	return true
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
