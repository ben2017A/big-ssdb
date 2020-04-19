package raft

type Service interface{
	// Last checkpoint of applied entries within service
	LastIndex() int64
	// If entry is not idempotent, service must apply entry
	// and update lastApplied in a transaction for atomicity
	ApplyEntry(ent *Entry)
	
	// TODO: rename to RaftApplyBroken()
	InstallSnapshot()
	
	// RaftIsUp()
	// RaftIsDown()
	
	// RaftCanBecomeLeader() bool
	// RaftDidBecomeLeader()
	// RaftCanBecomeFollower() bool
	// RaftDidBecomeFollower()
}
