package raft

import (
	"log"
	"math/rand"
)

const ElectionTimeout = 5 * 1000
const HeartBeatTimeout = (ElectionTimeout - 200)/3
const RequestVoteTimeout = ElectionTimeout

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

	Transport Transport
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "candidate" {
		if len(node.VotesReceived) > (len(node.Members) + 1)/2 {
			log.Println("convert to leader")
			node.Role = "leader"
			node.HeartBeatTimeout = 0
		}
	}
	if node.Role == "leader" {
		node.HeartBeatTimeout -= timeElapse
		if node.HeartBeatTimeout <= 0 {
			node.HeartBeatTimeout = HeartBeatTimeout

			for _, m := range node.Members {
				msg := new(Message)
				msg.Cmd = "AppendEntry"
				msg.Src = node.Id
				msg.Dst = m.Id
				msg.Idx = node.Index
				msg.Term = node.Term;
				msg.Data = "I am leader"

				node.Transport.Send(msg)
			}
		}
	}
	if node.Role != "leader" {
		node.ElectionTimeout -= timeElapse
		if node.ElectionTimeout <= 0 {
			node.ElectionTimeout = RequestVoteTimeout + rand.Intn(ElectionTimeout/2)

			node.Role = "candidate"
			node.Term += 1
			log.Println("begin election for term", node.Term)

			node.VotesReceived = make(map[string]string)
			// vote self
			node.VoteFor = node.Id
			node.VotesReceived[node.Id] = ""

			for _, m := range node.Members {
				msg := new(Message)
				msg.Cmd = "RequestVote"
				msg.Src = node.Id
				msg.Dst = m.Id
				msg.Idx = node.Index
				msg.Term = node.Term;
				msg.Data = "please vote me"

				node.Transport.Send(msg)
			}
		}
	}
}

func (node *Node)HandleMessage(msg *Message){
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
			if msg.Idx == node.Index && msg.Term == node.Term {
				log.Println("receive ack from", msg.Src)
				node.VotesReceived[msg.Src] = ""
			}
		}
	}
	if node.Role == "follower" {
		if msg.Cmd == "RequestVote" {
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
		if msg.Cmd == "AppendEntry" {
			node.ElectionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
		}
	}
}
