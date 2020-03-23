package raft

type Group struct {
	status string
	members map[string]*Member
}
