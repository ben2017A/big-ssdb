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
	Data string
}

func (m *Message)Encode() []byte{
	return EncodeMessage(m)
}

func atou(s string) uint32{
	n, _ := strconv.ParseUint(s, 10, 32)
	return n
}

func utoa(u uint32) string{
	return fmt.Sprintf("%d", u)
}

func EncodeMessage(msg *Message) []byte{
	ps := []string{msg.Cmd, msg.Src, msg.Dst, utoa(msg.Term), msg.Data}
	return []byte(strings.Join(ps, " "))
}

func DecodeMessage(buf []byte) *Message{
	s := string(buf)
	s = strings.Trim(s, "\r\n")
	ps := strings.SplitN(s, " ", 5)
	if len(ps) != 5 {
		return nil
	}
	msg := new(Message);
	msg.Cmd = ps[0]
	msg.Src = ps[1]
	msg.Dst = ps[2]
	msg.Term = atou(ps[3])
	msg.Data = ps[4]
	return msg
}
