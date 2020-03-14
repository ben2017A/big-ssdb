package raft

// 由 Node 和 Container 共同维护
type Group struct {
	status string
	members map[string]*Member
}
