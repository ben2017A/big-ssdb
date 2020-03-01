package raft

import (
	"encoding/json"
)

type State struct{
	Term int32
	VoteFor string
	Members map[string]string
}

func NewState() *State {
	s := new(State)
	s.Members = make(map[string]string)
	return s
}

func (s *State)CopyFrom(f *State) {
	s.Term = f.Term
	s.VoteFor = f.VoteFor
	s.Members = make(map[string]string)
	for k,v := range f.Members {
		s.Members[k] = v
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
