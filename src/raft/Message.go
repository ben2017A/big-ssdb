package raft

import (
	"strings"
	"strconv"
)

type Message struct{
	Cmd string
	Src string
	Dst string
	Idx uint64
	Term uint32
	Data string
}

func atou(s string) uint64{
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func DecodeMessage(data []byte) (*Message){
	s := string(data)
	s = strings.Trim(s, " \t\n")
	ps := strings.SplitN(s, " ", 6)
	if len(ps) != 6 {
		return nil
	}
	msg := new(Message);
	msg.Cmd = ps[0]
	msg.Src = ps[1]
	msg.Dst = ps[2]
	msg.Idx = atou(ps[3])
	msg.Term = uint32(atou(ps[4]))
	msg.Data = ps[5]
	return msg
}
