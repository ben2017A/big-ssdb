package raft

import (
	"log"
	"math/rand"
)

const ElectionTimeout = 5 * 1000
const HeartBeatTimeout = (ElectionTimeout - 200)/2
const RequestVoteTimeout = ElectionTimeout
const ResendTimeout = 2000

type Node struct{
	Id string
	Role string

	Term uint32
	Index uint64
	CommitIndex uint64

	Members map[string]*Member

	VoteFor string
	VotesReceived map[string]string

	ElectionTimeout int
	HeartBeatTimeout int

	Replications map[uint64]*Replication

	Transport Transport
}

func NewNode() *Node{
	node := new(Node)
	node.Term = 0
	node.Index = 0
	node.Members = make(map[string]*Member)
	node.Replications = make(map[uint64]*Replication)

	node.ElectionTimeout = ElectionTimeout + rand.Intn(100)

	return node
}

func (node *Node)Broadcast(msg *Message){
	for _, m := range node.Members {
		msg.Src = node.Id
		msg.Dst = m.Id
		if msg.Idx == 0 {
			msg.Idx = node.Index
		}
		if msg.Term == 0 {
			msg.Term = node.Term;
		}
		node.Transport.Send(msg)
	}
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "candidate" {
		if len(node.VotesReceived) > (len(node.Members) + 1)/2 {
			log.Println("convert to leader")
			node.Role = "leader"
			node.HeartBeatTimeout = 0
		}
	}
	if node.Role == "candidate" || node.Role == "follower" {
		node.ElectionTimeout -= timeElapse
		if node.ElectionTimeout <= 0 {
			node.ElectionTimeout = RequestVoteTimeout + rand.Intn(ElectionTimeout/2)
			node.onElectionTimeout()
		}
	}
	if node.Role == "leader" {
		node.HeartBeatTimeout -= timeElapse
		if node.HeartBeatTimeout <= 0 {
			node.HeartBeatTimeout = HeartBeatTimeout
			node.onHeartBeatTimeout()
		}

		for _, rep := range node.Replications {
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

	node.VotesReceived = make(map[string]string)
	// vote self
	node.VoteFor = node.Id
	node.VotesReceived[node.Id] = ""

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
			msg.Idx = rep.Index
			msg.Term = rep.Term
			msg.Data = rep.Data
			log.Println("log retransmission")
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
	if msg.Term < node.Term {
		// TODO: tell sender to update term
		return
	}
	if msg.Term > node.Term {
		node.Term = msg.Term
		if node.Role != "follower" {
			log.Println("convert to follower from", node.Role)
			node.Role = "follower"
			node.VoteFor = ""
		}
	}

	if node.Role == "leader" {
		if msg.Cmd == "AppendEntryAck" {
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
	// node.VoteFor == msg.Src: retransimitted/duplicated RequestVote
	if node.VoteFor == "" && msg.Term == node.Term && msg.Idx >= node.Index {
		log.Println("vote for", msg.Src)
		node.VoteFor = msg.Src
		node.ElectionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

		ack := new(Message)
		ack.Cmd = "RequestVoteAck"
		ack.Src = node.Id
		ack.Dst = msg.Src
		ack.Idx = msg.Idx
		ack.Term = msg.Term;
		ack.Data = ""

		node.Transport.Send(ack)
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	if msg.Idx == node.Index && msg.Term == node.Term {
		log.Println("receive ack from", msg.Src)
		node.VotesReceived[msg.Src] = ""
	}
}

func (node *Node)handleAppendEntry(msg *Message){
	node.ElectionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
}

/* ############################################# */

func (node *Node)Write(data string){
	node.Index += 1
	log.Println("WAL.write", node.Index, node.Term, data)

	rep := NewReplication()
	rep.Term = node.Term
	rep.Index = node.Index
	rep.Data = data
	rep.ResendTimeout = ResendTimeout
	node.Replications[rep.Index] = rep

	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Data = data
	node.Broadcast(msg)
}
