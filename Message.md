	Message
		cmd
		src
		dst
		idx
		term
		data

	ClientMessage(RPC)
		JoinGroup
		AddMember(for testing)
	RaftMessage(UDP/unreliable messaging)
		ErrorAck(not used)
			wrong srcId
			wrong dstId
		UpdateTerm
		RequestVote
		RequestVoteAck
		AppendEntry
			AddMember
			OtherCommand
		AppendEntryAck
