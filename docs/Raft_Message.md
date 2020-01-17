	Message
		cmd
		src
		dst
		term
		data

	ClientMessage(RPC)
		JoinGroup
		AddMember(for testing)
	RaftMessage(UDP/unreliable messaging)
		ErrorAck(not used)
			wrong srcId
			wrong dstId
		Noop
		RequestVote
		RequestVoteAck
		AppendEntry
		AppendEntryAck
