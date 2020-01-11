package raft

import (
	// "fmt"
	"strings"

	"myutil"
)

type Message struct{
	Cmd string
	Src string
	Dst string
	Term uint32
	PrevIndex uint64 // LastIndex for RequestVote
	PrevTerm  uint32 // LastTerm for RequestVote
	Data string
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
	msg.Term = myutil.Atou(ps[3])
	msg.PrevIndex = myutil.Atou64(ps[4])
	msg.PrevTerm = myutil.Atou(ps[5])
	msg.Data = ps[6]
	return msg
}

func (m *Message)Encode() string{
	ps := []string{m.Cmd, m.Src, m.Dst, myutil.Utoa(m.Term),
		myutil.Utoa64(m.PrevIndex), myutil.Utoa(m.PrevTerm), m.Data}
	return strings.Join(ps, " ")
}
