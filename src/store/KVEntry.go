package store

import (
	"fmt"
	"strings"
)

type KVEntry struct {
	Cmd string
	Key string
	Val string
}

func (ent *KVEntry)Decode(buf string) bool {
	ps := strings.SplitN(buf, " ", 3)
	switch ps[0] {
	case "set":
		if len(ps) != 3 {
			return false;
		}
		ent.Cmd = ps[0]
		ent.Key = ps[1]
		ent.Val = ps[2]
	case "del":
		if len(ps) != 2 {
			return false;
		}
		ent.Cmd = ps[0]
		ent.Key = ps[1]
		ent.Val = ""
	default:
		return false
	}
	return true
}

func (ent *KVEntry)Encode() string {
	var s string
	switch ent.Cmd {
	case "set":
		s = fmt.Sprintf("%s %s %s", ent.Cmd, ent.Key, ent.Val)
	case "del":
		s = fmt.Sprintf("%s %s", ent.Cmd, ent.Key)
	}
	return s
}
