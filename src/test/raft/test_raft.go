package main

import (
	// "time"
	"log"
	"fmt"
	
	"raft"
)

const TimerInterval = 100

var n1, n2 *raft.Node
var bus *Bus

func setup_master() {
	bus = NewBus()
	t1 := bus.MakeTransport("n1", "addr1")
	s1 := NewFakeStorage()
	n1 = raft.NewNode("n1", s1, t1)

	t2 := bus.MakeTransport("n2", "addr2")
	s2 := NewFakeStorage()
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

	n1.Tick(raft.HeartbeatTimeout + 1) // send Heartbeat
}

func setup_follower() {
	n2.JoinGroup("n1", "addr1")
	n2.Step() // recv Heartbeat, send Ack[false]
	if n2.Role != "follower" {
		log.Fatal("error")
	}
	
	n1.Step() // send InstallSnapshot
	
	n2.Step() // InstallSnapshot
	log.Println("\n" + n2.Info())
	
	n1.Step() // recv Ack, send Heartbeat
	log.Println("\n" + n1.Info())
	if n1.Members["n2"].NextIndex != 4 || n1.Members["n2"].MatchIndex != 3 {
		log.Fatal("error")		
	}

	if n2.InfoMap()["commitIndex"] != "3" {
		log.Fatal("error")
	}
}
	
func test_quorum_write() {	
	n1.Write("a")
	n1.Write("b")
	n1.Write("c")
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
}

func test_new_leader() {
	n2.Tick(raft.ElectionTimeout) // start new vote
	log.Println("\n" + n2.Info())
	if n2.Role != "candidate" || n2.Term != 2 {
		log.Fatal("error")
	}

	n1.Tick(raft.ElectionTimeout) // receive follower timeout
	n1.Step() // become follower. recv RequestVote, send ack
	log.Println("\n" + n1.Info())
	if n1.Role != "follower" || n1.Term != 2 {
		log.Fatal("error")
	}

	n2.Step() // become leader, send Noop
	log.Println("\n" + n2.Info())
	if n2.Role != "leader" {
		log.Fatal("error")
	}
	
	n1.Step()
	log.Println("\n" + n1.Info())
	if n1.Members["n2"].Role != "leader" {
		log.Fatal("error")
	}
}

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	setup_master()
	setup_follower()
	test_quorum_write()
	fmt.Println("\n---------------------------------------------------\n")
		
	setup_master()
	setup_follower()
	test_new_leader()
	fmt.Println("\n---------------------------------------------------\n")
	
	setup_master()
	setup_follower()
	log.Println("\n" + n2.Info())
	fmt.Println("\n---------------------------------------------------\n")
}