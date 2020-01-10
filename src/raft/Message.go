package raft

import (
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
	return EncodeMessage(m)
}

func EncodeMessage(msg *Message) string{
	ps := []string{msg.Cmd, msg.Src, msg.Dst, Utoa(msg.Term),
		Utoa64(msg.PrevIndex), Utoa(msg.PrevTerm), msg.Data}
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
