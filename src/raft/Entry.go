package raft

import (
	"fmt"
	"strings"
)

type Entry struct{
	Index uint64
	Term uint32
	Data string
}

func DecodeEntry(buf string) *Entry{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 3)
	if len(ps) != 3 {
		return nil
	}

	e := new(Entry);
	e.Index = Atou64(ps[0])
	e.Term = Atou(ps[1])
	e.Data = ps[2]
	return e
}

func (e *Entry)Encode() string{
	return fmt.Sprintf("%d %d %s\n", e.Index, e.Term, e.Data)
}
