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

func NewRequestVoteMsg() *Message{
	msg := new(Message)
	msg.Cmd = "RequestVote"
	msg.Data = "please vote me"
	return msg
}

func NewRequestVoteAck(voteFor string, success bool) *Message{
	msg := new(Message)
	msg.Cmd = "RequestVoteAck"
	msg.Dst = voteFor
	if success {
		msg.Data = "true"
	}else{
		msg.Data = "false"
	}
	return msg
}

func NewAppendEntryMsg(dst string, ent *Entry, prev *Entry) *Message{
	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Dst = dst
	if prev != nil {
		msg.PrevIndex = prev.Index
		msg.PrevTerm = prev.Term
	}
	msg.Data = ent.Encode()
	return msg
}

func NewAppendEntryAck(dst string, success bool) *Message{
	msg := new(Message)
	msg.Cmd = "AppendEntryAck"
	msg.Dst = dst
	if success {
		msg.Data = "true"
	}else{
		msg.Data = "false"
	}
	return msg
}
