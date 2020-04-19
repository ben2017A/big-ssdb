package raft

import (
	// "fmt"
	"strings"

	"util"
)

type MessageType string

const(
	MessageTypeGossip          = "Gossip"
	MessageTypePreVote         = "PreVote"
	MessageTypePreVoteAck      = "PreVoteAck"
	MessageTypeRequestVote     = "RequestVote"
	MessageTypeRequestVoteAck  = "RequestVoteAck"
	MessageTypeAppendEntry     = "AppendEntry"
	MessageTypeAppendEntryAck  = "AppendEntryAck"
	MessageTypeInstallSnapshot = "InstallSnapshot" // install raft state, not service state
)

type Message struct{
	Type MessageType
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
	ps := []string{string(m.Type), m.Src, m.Dst, util.I32toa(m.Term),
		util.I32toa(m.PrevTerm), util.I64toa(m.PrevIndex), m.Data}
	return strings.Join(ps, " ")
}

func (m *Message)Decode(buf string) bool{
	buf = strings.Trim(buf, "\r\n")
	ps := strings.SplitN(buf, " ", 7)
	if len(ps) != 7 {
		return false
	}
	m.Type = MessageType(ps[0])
	m.Src = ps[1]
	m.Dst = ps[2]
	m.Term = util.Atoi32(ps[3])
	m.PrevTerm = util.Atoi32(ps[4])
	m.PrevIndex = util.Atoi64(ps[5])
	m.Data = ps[6]
	return true
}

func NewGossipMsg(dst string) *Message{
	msg := new(Message)
	msg.Type = MessageTypeGossip
	msg.Dst = dst
	return msg
}

func NewPreVoteMsg() *Message{
	msg := new(Message)
	msg.Type = MessageTypePreVote
	return msg
}

func NewPreVoteAck(dst string) *Message{
	msg := new(Message)
	msg.Type = MessageTypePreVoteAck
	msg.Dst = dst
	return msg
}

func NewRequestVoteMsg() *Message{
	msg := new(Message)
	msg.Type = MessageTypeRequestVote
	msg.Data = "please vote me"
	return msg
}

func NewRequestVoteAck(dst string, grant bool) *Message{
	msg := new(Message)
	msg.Type = MessageTypeRequestVoteAck
	msg.Dst = dst
	if grant {
		msg.Data = "grant"
	} else {
		msg.Data = "reject"
	}
	return msg
}

func NewAppendEntryMsg(dst string, ent *Entry, prev *Entry) *Message{
	msg := new(Message)
	msg.Type = MessageTypeAppendEntry
	msg.Dst = dst
	if prev == nil {
		msg.PrevTerm = 0
		msg.PrevIndex = 0
	} else {
		msg.PrevTerm = prev.Term
		msg.PrevIndex = prev.Index
	}
	msg.Data = ent.Encode()
	return msg
}

func NewAppendEntryAck(dst string, success bool) *Message{
	msg := new(Message)
	msg.Type = MessageTypeAppendEntryAck
	msg.Dst = dst
	if success {
		msg.Data = "true"
	}else{
		msg.Data = "false"
	}
	return msg
}

func NewInstallSnapshotMsg(dst string, data string) *Message{
	msg := new(Message)
	msg.Type = MessageTypeInstallSnapshot
	msg.Dst = dst
	msg.Data = data
	return msg
}