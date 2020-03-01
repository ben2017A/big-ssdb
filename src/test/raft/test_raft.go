package main

import (
	// "time"
	"log"
	"fmt"
	
	"raft"
)

const TimerInterval = 100

var n1, n2 *raft.Node

func setup() {
	bus := NewBus()
	t1 := bus.MakeTransport("n1", "addr1")
	t2 := bus.MakeTransport("n2", "addr2")
	s1 := NewFakeStorage()
	s2 := NewFakeStorage()
	n1 = raft.NewNode("n1", s1, t1)
	n2 = raft.NewNode("n2", s2, t2)

	log.Println("init:\n" + n1.Info())
	n1.Tick(raft.ElectionTimeout * 2)
	log.Println("\n" + n1.Info())
	if n1.Role != "leader" {
		log.Fatal("error")
	}

	n1.AddMember("n1", "addr1")
	n1.AddMember("n2", "addr2")
	n1.Step()
	log.Println("\n" + n1.Info())
	if n1.InfoMap()["commitIndex"] != "3" {
		log.Fatal("error")
	}
}

func test_follow() {
	n1.Tick(raft.HeartbeatTimeout + 1) // send Heartbeat
	
	n2.JoinGroup("n1", "addr1")
	n2.Step() // send Heartbeat Ack[false]
	if n2.Role != "follower" {
		log.Fatal("error")
	}
	
	n1.Step() // send ApplyEntry
	
	n2.Step() // recv, commit, send Ack
	log.Println("\n" + n2.Info())
	
	n1.Step() // recv Ack, send Heartbeat
	log.Println("\n" + n1.Info())
	if n1.Members["n2"].NextIndex != 4 || n1.Members["n2"].MatchIndex != 3 {
		log.Fatal("error")		
	}

	if n2.InfoMap()["commitIndex"] != "3" {
		log.Fatal("error")
	}
	
	n2.Step() // recv, send ack
	n1.Step() // recv ack
}
	
func test_quorum_write() {	
	n1.Write("a")
	n1.Write("b")
	n1.Write("c")
	// n1.Write("d")
	n1.Step() // send ApplyEntry with 3(send window size) entries
	log.Println("\n" + n1.Info())
	
	n2.Step() // recv, send Ack
	log.Println("\n" + n2.Info())

	n1.Step() // recv Ack, send Heartbeat
	log.Println("\n" + n1.Info())
	if n1.InfoMap()["commitIndex"] != "6" {
		log.Fatal("error")
	}

	n2.Step() // recv Heartbeat, send Ack
	log.Println("\n" + n2.Info())
	if n2.InfoMap()["commitIndex"] != "6" {
		log.Fatal("error")
	}
	
	n1.Step() // recv Ack
	n1.Step() // nothing	
}

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	// setup()
	// test_follow()
	// test_quorum_write()
	
	fmt.Println("\n---------------------------------------------------\n")
	
	setup()
	test_follow()

	fmt.Println("\n---------------------------------------------------\n")
	
	n2.Tick(raft.ElectionTimeout * 2)
	log.Println("\n" + n2.Info())
	

	fmt.Println("\n---------------------------------------------------\n")
}