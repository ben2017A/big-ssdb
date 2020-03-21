package server

import (
	"fmt"
	"strings"
	"link"
)

type Request struct{
	Src int
	Term int32

	ps []string
	msg *link.Message
}

func NewRequest(m *link.Message) *Request {
	ret := new(Request)
	ret.msg = m
	return ret
}

func (req *Request)Decode(buf string) bool {
	req.ps = strings.SplitN(buf, " ", 3)
	return true
}

func (req *Request)Encode() string {
	cmd := req.Cmd()
	key := req.Key()
	val := req.Val()
	
	var s string
	switch cmd {
	case "set":
		s = fmt.Sprintf("%s %s %s", cmd, key, val)
	case "del":
		s = fmt.Sprintf("%s %s", cmd, key)
	case "incr":
		if val == "" {
			val = "1"
		}
		s = fmt.Sprintf("%s %s %s", cmd, key, val)
	}
	return s
}

func (req *Request)Cmd() string {
	if len(req.ps) > 0 {
		return strings.ToLower(req.ps[0])
	}
	return strings.ToLower(req.msg.Cmd())
}

func (req *Request)Key() string {
	return req.Arg(0)
}

func (req *Request)Val() string {
	return req.Arg(1)
}

func (req *Request)Arg(idx int) string {
	var args []string
	if len(req.ps) > 0 {
		args = req.ps[1 : ]
	} else {
		args = req.msg.Args()
	}
	if len(args) <= idx {
		return ""
	}
	return args[idx]
}
