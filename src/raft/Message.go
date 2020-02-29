package raft

import (
	// "fmt"
	"strings"

	"util"
)

const(
	MessageCmdRequestVote    = "RequestVote"
	MessageCmdRequestVoteAck = "RequestVoteAck"
	MessageCmdAppendEntry    = "AppendEntry"
	MessageCmdAppendEntryAck = "AppendEntryAck"
	MessageCmdInstallConfig  = "InstallConfig"
)

type Message struct{
	Cmd string
	Src string
	Dst string
	Term int32
	PrevTerm  int32 // LastTerm for RequestVote
	PrevIndex int64 // LastIndex for RequestVote
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
	ps := []string{m.Cmd, m.Src, m.Dst, util.Itoa32(m.Term),
		util.Itoa32(m.PrevTerm), util.I64toa(m.PrevIndex), m.Data}
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
	m.Term = util.Atoi32(ps[3])
	m.PrevTerm = util.Atoi32(ps[4])
	m.PrevIndex = util.Atoi64(ps[5])
	m.Data = ps[6]
	return true
}

func NewRequestVoteMsg() *Message{
	msg := new(Message)
	msg.Cmd = MessageCmdRequestVote
	msg.Data = "please vote me"
	return msg
}

func NewRequestVoteAck(voteFor string, success bool) *Message{
	msg := new(Message)
	msg.Cmd = MessageCmdRequestVoteAck
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
	msg.Cmd = MessageCmdAppendEntry
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
	msg.Cmd = MessageCmdAppendEntryAck
	msg.Dst = dst
	if success {
		msg.Data = "true"
	}else{
		msg.Data = "false"
	}
	return msg
}
