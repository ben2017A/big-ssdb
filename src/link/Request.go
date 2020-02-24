package link

import (
	"fmt"
	"strings"
)

type Request struct{
	Src int
	cmd string
	args []string
}

func (req *Request)Decode(buf string) bool {
	ps := strings.Split(buf, " ")
	if len(ps) != 2 {
		return false
	}
	req.cmd = ps[0]
	req.args = ps[1:]
	return true
}

func (req *Request)Encode() string {
	key := req.Key()
	val := req.Val()
	
	var s string
	switch req.cmd {
	case "set":
		s = fmt.Sprintf("%s %s %s", req.cmd, key, val)
	case "del":
		s = fmt.Sprintf("%s %s", req.cmd, key)
	case "incr":
		if val == "" {
			val = "1"
		}
		s = fmt.Sprintf("%s %s %s", req.cmd, key, val)
	}
	return s
}

func (req *Request)Cmd() string {
	return req.cmd
}

func (req *Request)Key() string {
	if len(req.args) > 0 {
		return req.args[0]
	}
	return ""
}

func (req *Request)Val() string {
	if len(req.args) > 1 {
		return req.args[1]
	}
	return ""	
}

func (req *Request)Arg(idx int) string {
	if len(req.args) <= idx {
		return ""
	}
	return req.args[idx]
}
