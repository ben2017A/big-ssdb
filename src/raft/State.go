package raft

import (
	"encoding/json"
)

type State struct{
	Id string
	Addr string
	Term uint32
	VoteFor string
	Members map[string]string
}

func NewStateFromNode(node *Node) *State{
	s := State{node.Id, node.Addr, node.Term, node.VoteFor, make(map[string]string)}
	for _, m := range node.Members {
		s.Members[m.Id] = m.Addr
	}
	return &s
}

func DecodeState(buf string) *State {
	m := new(State)
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

func (s *State)Encode() string{
	b, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(b)
}

func (s *State)Decode(buf string) bool{
	err := json.Unmarshal([]byte(buf), s)
	return err == nil
}
