package raft

type Member struct{
	Id string
	Addr string
	Role RoleType

	// sliding window
	NextIndex int64   // next_send
	MatchIndex int64  // last_ack

	HeartbeatTimer int
	ReplicateTimer int

	ReceiveTimeout int // increase on tick(), reset on ApplyEntryAck
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Role = RoleFollower
	ret.Id = id
	ret.Addr = addr
	ret.Reset()
	return ret
}

func (m *Member)Reset() {
	m.Role = RoleFollower
	m.NextIndex = 0
	m.MatchIndex = 0
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	m.ReceiveTimeout = 0
}
