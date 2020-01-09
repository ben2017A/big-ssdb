package main

import (
	// "fmt"
	"raft"
	"time"
	"log"
	"math/rand"
	"os"
	"strconv"
)


const TimerInterval = 20

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}

	log.Println("server started at", port)

	transport := raft.NewUdpTransport("127.0.0.1", port)
	defer transport.Stop()

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	node := new(raft.Node)
	node.Role = "follower"
	node.Term = 0
	node.Index = 0
	node.Members = make(map[string]*raft.Member)

	if port == 8001 {
		node.Id = "n1"
		node.Members["n2"] = raft.NewMember("n2", "127.0.0.1:8002")
		transport.Connect("n2", "127.0.0.1:8002")
	}else{
		node.Id = "n2"
		node.Members["n1"] = raft.NewMember("n1", "127.0.0.1:8001")
		transport.Connect("n1", "127.0.0.1:8001")
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
				log.Println("decode error:", buf)
				continue
			}
			log.Printf("receive %s", string(msg.Encode()))
			node.HandleMessage(msg)
		}
	}
}
