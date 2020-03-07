package raft

type Member struct{
	Id string
	Role string
	Addr string

	// sliding window
	NextIndex int64   // next_send
	MatchIndex int64  // last_ack
	SendWindow int64  // 

	HeartbeatTimer int
	ReplicateTimer int

	ReceiveTimeout int // increase on tick(), reset on ApplyEntryAck
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Role = RoleFollower
	ret.Id = id
	ret.Addr = addr
	ret.SendWindow = 3
	return ret
}

func (m *Member)Reset() {
	m.NextIndex = 0
	m.MatchIndex = 0
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	m.ReceiveTimeout = 0
}
