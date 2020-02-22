package raft

import (
	"encoding/json"
)

type State struct{
	Id string
	Addr string
	Term int32
	VoteFor string
	Members map[string]string
}

func DecodeState(buf string) *State {
	m := new(State)
	if m.Decode(buf) {
		return m
	} else {
		return nil
	}
}

func (s *State)LoadFromNode(node *Node) {
	s.Id = node.Id
	s.Addr = node.Addr
	s.Term = node.Term
	s.VoteFor = node.VoteFor
	s.Members = make(map[string]string)
	for _, m := range node.Members {
		s.Members[m.Id] = m.Addr
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
