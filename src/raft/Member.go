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
	ReplicationTimer int

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
