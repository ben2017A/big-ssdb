package raft

import (
	"fmt"
	"strings"
	"strconv"
)

type Message struct{
	Cmd string
	Src string
	Dst string
	Term uint32
	PrevLogIndex uint64
	PrevLogTerm  uint32
	Data string
}

func (m *Message)Encode() []byte{
	return EncodeMessage(m)
}

func atou(s string) uint32{
	n, _ := strconv.ParseUint(s, 10, 32)
	return uint32(n)
}

func utoa(u uint32) string{
	return fmt.Sprintf("%d", u)
}

func atou64(s string) uint64{
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func utoa64(u uint64) string{
	return fmt.Sprintf("%d", u)
}

func EncodeMessage(msg *Message) []byte{
	ps := []string{msg.Cmd, msg.Src, msg.Dst, utoa(msg.Term),
		utoa64(msg.PrevLogIndex), utoa(msg.PrevLogTerm), msg.Data}
	return []byte(strings.Join(ps, " "))
}

func DecodeMessage(buf []byte) *Message{
	s := string(buf)
	s = strings.Trim(s, "\r\n")
	ps := strings.SplitN(s, " ", 7)
	if len(ps) != 7 {
		return nil
	}
	msg := new(Message);
	msg.Cmd = ps[0]
	msg.Src = ps[1]
	msg.Dst = ps[2]
	msg.Term = atou(ps[3])
	msg.PrevLogIndex = atou64(ps[4])
	msg.PrevLogTerm = atou(ps[5])
	msg.Data = ps[6]
	return msg
}
