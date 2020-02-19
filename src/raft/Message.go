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
	Term int32
	PrevIndex int64 // LastIndex for RequestVote
	PrevTerm  int32 // LastTerm for RequestVote
	Data string
}

func DecodeMessage(buf string) *Message{
	m := new(Message);
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

func (m *Message)Encode() string{
	ps := []string{m.Cmd, m.Src, m.Dst, myutil.Itoa32(m.Term),
		myutil.Itoa64(m.PrevIndex), myutil.Itoa32(m.PrevTerm), m.Data}
	return strings.Join(ps, " ")
}

func (m *Message)Decode(buf string) bool{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 7)
	if len(ps) != 7 {
		return false
	}
	m.Cmd = ps[0]
	m.Src = ps[1]
	m.Dst = ps[2]
	m.Term = myutil.Atoi32(ps[3])
	m.PrevIndex = myutil.Atoi64(ps[4])
	m.PrevTerm = myutil.Atoi32(ps[5])
	m.Data = ps[6]
	return true
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
