package link

import (
	// "bytes"
	// "strings"
)

type Message struct {
	Src int
	ps []string
}

func NewMessage(ps []string) *Message {
	ret := new(Message)
	ret.ps = ps
	return ret
}

func NewResponse(src int, ps []string) *Message {
	ret := new(Message)
	ret.Src = src
	ret.ps = ps
	return ret
}

func NewErrorResponse(src int, desc string) *Message {
	ret := new(Message)
	ret.Src = src
	ret.ps = []string{"error", desc}
	return ret
}

func (m *Message)Data() []string {
	return m.ps
}

func (m *Message)Cmd() string {
	if len(m.ps) > 0 {
		return m.ps[0]
	}
	return ""
}

func (m *Message)Code() string {
	return m.Cmd()
}

func (m *Message)Args() []string {
	if len(m.ps) > 0 {
		return m.ps[1 : ]
	}
	return make([]string, 0)
}
