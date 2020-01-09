package main

import (
	// "fmt"
	"raft"
	"time"
	"log"
	"math/rand"
)


const TimerInterval = 20

func main(){
	transport := raft.NewUdpTransport("127.0.0.1", 8001)
	defer transport.Stop()

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	node := new(raft.Node)
	node.Id = "n1"
	node.Role = "follower"
	node.Term = 0
	node.Index = 0
	node.Members = make(map[string]*raft.Member)

	{
		node.Members["n2"] = raft.NewMember("n2", "127.0.0.1:8002")
		node.Members["n3"] = raft.NewMember("n3", "127.0.0.1:8003")
		transport.Connect("n2", "127.0.0.1:8002")
		transport.Connect("n3", "127.0.0.1:8003")
	}

	node.VoteFor = ""
	node.ElectionTimeout = raft.ElectionTimeout + rand.Intn(100)

	node.Transport = transport

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case buf := <-transport.C:
			msg := raft.DecodeMessage(buf);
			if msg == nil {
				continue
			}
			log.Printf("%#v\n", msg)
			node.HandleMessage(msg)
		}
	}
}
