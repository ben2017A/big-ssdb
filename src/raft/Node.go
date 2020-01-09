package raft

import (
	"log"
	"math/rand"
)

const ElectionTimeout = 2 * 1000
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

	Transport Transport
}

func (node *Node)Tick(timeElapse int){
	if node.Role != "leader" {
		node.ElectionTimeout -= timeElapse
		if node.ElectionTimeout <= 0 {
			node.ElectionTimeout = RequestVoteTimeout + rand.Intn(100)

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

				node.Transport.SendTo([]byte("abc\n"), msg.Dst)
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
				node.VotesReceived[msg.Src] = ""
				if len(node.VotesReceived) > len(node.Members)/2 {
					log.Printf("convert to leader")
					node.Role = "leader"
				}
			}
		}
	}
	if node.Role == "follower" {
		if msg.Cmd == "RequestVote" {
			// node.VoteFor == msg.Src: retransimitted/duplicated RequestVote
			if node.VoteFor == "" && msg.Term == node.Term && msg.Idx >= node.Index {
				// send ack
				node.VoteFor = msg.Src
			}
		}
		if msg.Cmd == "AppendEntry" {
			node.ElectionTimeout = ElectionTimeout + rand.Intn(100)
		}
	}
}
