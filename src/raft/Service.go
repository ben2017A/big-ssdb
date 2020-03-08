package raft

type Service interface{
	// Last checkpoint of applied entries within service
	LastApplied() int64
	// If entry is not idempotent, service must apply entry
	// and update lastApplied in a transaction for atomicity
	ApplyEntry(ent *Entry)
	
	InstallSnapshot()
	
	// RaftIsUp()
	// RaftIsDown()
	
	// RaftCanBecomeLeader() bool
	// RaftDidBecomeLeader()
	// RaftCanBecomeFollower() bool
	// RaftDidBecomeFollower()
}
