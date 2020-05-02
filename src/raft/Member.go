package raft

type Member struct{
	Id string
	Role RoleType

	// sliding window
	NextIndex int64   // next_send
	MatchIndex int64  // last_ack, -1: never received from remote

	HeartbeatTimer int
	ReplicateTimer int

	ReceiveTimeout int // increase on tick(), reset on ApplyEntryAck
}

func NewMember(id string) *Member{
	ret := new(Member)
	ret.Role = RoleFollower
	ret.Id = id
	ret.Reset()
	return ret
}

func (m *Member)Reset() {
	m.Role = RoleFollower
	m.NextIndex = 0
	m.MatchIndex = -1
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	m.ReceiveTimeout = 0
}
