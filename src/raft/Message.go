package raft

import (
	"fmt"
	"strings"
)

type Message struct{
	Cmd string
	Src string
	Dst string
	Term uint32
	PrevIndex uint64
	PrevTerm  uint32
	Data string
}

func (m *Message)Encode() string{
	ps := []string{m.Cmd, m.Src, m.Dst, Utoa(m.Term),
		Utoa64(m.PrevIndex), Utoa(m.PrevTerm), m.Data}
	return strings.Join(ps, " ")
}

func DecodeMessage(buf string) *Message{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 7)
	if len(ps) != 7 {
		return nil
	}
	msg := new(Message);
	msg.Cmd = ps[0]
	msg.Src = ps[1]
	msg.Dst = ps[2]
	msg.Term = Atou(ps[3])
	msg.PrevIndex = Atou64(ps[4])
	msg.PrevTerm = Atou(ps[5])
	msg.Data = ps[6]
	return msg
}

/* ################################################### */

type AppendEntry struct{
	Type string
	CommitIndex uint64

	Entry *Entry
}

func (ae *AppendEntry)Encode() string{
	if ae.Entry != nil {
		return fmt.Sprintf("Entry %d %s\n", ae.CommitIndex, ae.Entry.Encode())
	}else{
		return fmt.Sprintf("%s %d\n", ae.Type, ae.CommitIndex)
	}
}

func DecodeAppendEntry(buf string) *AppendEntry{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 3)
	if len(ps) < 2 {
		return nil
	}

	ae := new(AppendEntry)
	ae.Type = ps[0]
	ae.CommitIndex = Atou64(ps[1])

	if ae.Type == "Heartbeat" {
		return ae
	}else if ae.Type == "Entry" {
		if len(ps) == 3 {
			ae.Entry = DecodeEntry(ps[2])
			return ae
		}
	}
	return nil
}

