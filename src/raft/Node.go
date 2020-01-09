package raft

import (
	"log"
	"math/rand"
)

const ElectionTimeout = 5 * 1000
const HeartBeatTimeout = (ElectionTimeout - 200)/2
const RequestVoteTimeout = ElectionTimeout
const ResendTimeout = 2000

type LogEntry struct{
	Index uint64
	Term uint32
	Data string
}

type Node struct{
	Id string
	Role string

	Index uint64
	Term uint32

	Members map[string]*Member

	voteFor string
	votesReceived map[string]string

	electionTimeout int
	heartBeatTimeout int

	replications map[uint64]*Replication
	entries map[uint64]*LogEntry

	Transport Transport
}

func NewNode() *Node{
	node := new(Node)
	node.Term = 0
	node.Index = 0
	node.Members = make(map[string]*Member)
	node.replications = make(map[uint64]*Replication)
	node.entries = make(map[uint64]*LogEntry)

	node.electionTimeout = ElectionTimeout + rand.Intn(100)

	return node
}

func (node *Node)Broadcast(msg *Message){
	for _, m := range node.Members {
		msg.Src = node.Id
		msg.Dst = m.Id
		if msg.Index == 0 {
			msg.Index = node.Index
		}
		if msg.Term == 0 {
			msg.Term = node.Term;
		}
		node.Transport.Send(msg)
	}
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "candidate" {
		if len(node.votesReceived) + 1 > (len(node.Members) + 1)/2 {
			log.Println("convert to leader")
			node.Role = "leader"
			node.voteFor = ""
			node.heartBeatTimeout = 0

			for _, m := range node.Members {
				m.NextIndex = node.Index + 1
				m.MatchIndex = 0
			}
		}
	}
	if node.Role == "candidate" || node.Role == "follower" {
		node.electionTimeout -= timeElapse
		if node.electionTimeout <= 0 {
			node.electionTimeout = RequestVoteTimeout + rand.Intn(ElectionTimeout/2)
			node.onElectionTimeout()
		}
	}
	if node.Role == "leader" {
		node.heartBeatTimeout -= timeElapse
		if node.heartBeatTimeout <= 0 {
			node.heartBeatTimeout = HeartBeatTimeout
			node.onHeartBeatTimeout()
		}

		for _, rep := range node.replications {
			rep.ResendTimeout -= timeElapse
			if rep.ResendTimeout <= 0 {
				rep.ResendTimeout = ResendTimeout
				node.onReplicationResendTimeout(rep)
			}
		}
	}
}

func (node *Node)onElectionTimeout(){
	node.Role = "candidate"
	node.Term += 1
	log.Println("begin election for term", node.Term)

	node.votesReceived = make(map[string]string)
	// vote self
	node.voteFor = node.Id

	msg := new(Message)
	msg.Cmd = "RequestVote"
	msg.Data = "please vote me"
	node.Broadcast(msg)
}

func (node *Node)onReplicationResendTimeout(rep *Replication){
	for _, m := range node.Members {
		if rep.AckReceived[m.Id] == "" {
			msg := new(Message)
			msg.Cmd = "AppendEntry"
			msg.Src = node.Id
			msg.Dst = m.Id
			msg.Index = rep.Index
			msg.Term = rep.Term
			msg.Data = rep.Data
			log.Println("log retransmission", m.Id, rep.AckReceived)
			node.Transport.Send(msg)
		}
	}
}

func (node *Node)onHeartBeatTimeout(){
	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Data = "HeartBeat"
	node.Broadcast(msg)
}

/* ############################################# */

func (node *Node)HandleRaftMessage(msg *Message){
	if msg.Term > node.Term {
		node.Term = msg.Term
		if node.Role != "follower" {
			log.Println("convert to follower from", node.Role)
			node.Role = "follower"
			node.voteFor = ""
		}
	}

	if node.Role == "leader" {
		if msg.Cmd == "AppendEntryAck" {
			node.handleAppendEntryAck(msg)
		}
	}
	if node.Role == "candidate" {
		if msg.Cmd == "RequestVoteAck" {
			node.handleRequestVoteAck(msg)
		}
	}
	if node.Role == "follower" {
		if msg.Cmd == "RequestVote" {
			node.handleRequestVote(msg)
		}
		if msg.Cmd == "AppendEntry" {
			node.handleAppendEntry(msg)
		}
	}
}

func (node *Node)handleRequestVote(msg *Message){
	if msg.Term < node.Term {
		// TODO: false Ack
		return
	}

	// node.voteFor == msg.Src: retransimitted/duplicated RequestVote
	if node.voteFor == "" && msg.Term == node.Term && msg.Index >= node.Index {
		log.Println("vote for", msg.Src)
		node.voteFor = msg.Src
		node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

		ack := new(Message)
		ack.Cmd = "RequestVoteAck"
		ack.Src = node.Id
		ack.Dst = msg.Src
		ack.Index = msg.Index
		ack.Term = msg.Term;
		ack.Data = ""

		node.Transport.Send(ack)
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	if msg.Index == node.Index && msg.Term == node.Term {
		log.Println("receive ack from", msg.Src)
		node.votesReceived[msg.Src] = "ok"
	}
}

func (node *Node)handleAppendEntry(msg *Message){
	if msg.Term < node.Term {
		// TODO: false Ack
		return
	}

	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

	prevIndex := msg.Index - 1
	prevTerm := msg.Term - 1
	if prevIndex > 0 && prevTerm > 0 {
		prev := node.entries[prevIndex]
		if prev == nil || prev.Term != prevTerm {
			// send false Ack
			ack := new(Message)
			ack.Cmd = "AppendEntryAck"
			ack.Src = node.Id
			ack.Dst = msg.Src
			ack.Index = msg.Index
			ack.Term = msg.Term;
			ack.Data = "false"
			node.Transport.Send(ack)
			return
		}
	}

	old := node.entries[msg.Index]
	if old != nil && old.Term != msg.Term {
		// entry conflicts
		delete(node.entries, msg.Index)
	}

	entry := new(LogEntry)
	entry.Index = msg.Index
	entry.Term = msg.Term
	entry.Data = msg.Data
	node.entries[entry.Index] = entry

	// send true Ack
	ack := new(Message)
	ack.Cmd = "AppendEntryAck"
	ack.Src = node.Id
	ack.Dst = msg.Src
	ack.Index = msg.Index
	ack.Term = msg.Term;
	ack.Data = "true"
	node.Transport.Send(ack)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	rep := node.replications[msg.Index]
	if rep == nil {
		return
	}
	if rep.Term != msg.Term {
		return
	}
	rep.AckReceived[msg.Src] = "ok"

	if len(rep.AckReceived) + 1 > (len(node.Members) + 1)/2 {
		// 
		log.Println("commit", rep.Index)
	}
}

/* ############################################# */

func (node *Node)Write(data string){
	node.Index += 1
	log.Println("WAL.write", node.Index, node.Term, data)

	entry := new(LogEntry)
	entry.Index = node.Index
	entry.Term = node.Term
	entry.Data = data

	node.entries[entry.Index] = entry
	node.replicateEntries()
}

func (node *Node)replicateEntries(){
	for _, m := range node.Members {
		for{
			entry := node.entries[m.NextIndex]
			if entry == nil {
				break;
			}
			m.NextIndex += 1

			msg := new(Message)
			msg.Cmd = "AppendEntry"
			msg.Src = node.Id
			msg.Dst = m.Id
			msg.Index = entry.Index
			msg.Term = entry.Term
			msg.Data = entry.Data
			node.Transport.Send(msg)
		}
	}
}
