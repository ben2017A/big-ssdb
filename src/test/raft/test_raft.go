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
	n1.Tick(raft.ElectionTimeout + 1)
	log.Println("\n" + n1.Info())
	if n1.Role != "leader" {
		log.Fatal("error")
	}
	n1.AddMember("n1", "addr1")
	n1.AddMember("n2", "addr2")
	n1.Step()
	log.Println("\n" + n1.Info())
}

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	setup()
	
	n1.Tick(raft.HeartbeatTimeout + 1) // send Heartbeat
	
	n2.JoinGroup("n1", "addr1")
	n2.Step() // reply ApplyEntryAct[false]
	if n2.Role != "follower" {
		log.Fatal("error")
	}
	
	n1.Step() // send ApplyEntry
	
	n2.Step() // send ApplyEntryAck
	log.Println("\n" + n2.Info())
	if n2.InfoMap()["lastIndex"] != n1.InfoMap()["lastIndex"] {
		log.Fatal("error")
	}
	
	n1.Step() // recv Ack

	n1.Write("a")
	n1.Write("b")
	n1.Write("c")
	n1.Step() // send ApplyEntry
	
	n2.Step() // send ApplyEntryAck
	log.Println("\n" + n2.Info())

	n1.Step() // recv Ack, send Commit
	n2.Step() // recv commit
	log.Println("\n" + n2.Info())
	if n2.InfoMap()["lastIndex"] != n1.InfoMap()["lastIndex"] {
		log.Fatal("error")
	}

	fmt.Println("\n---------------------------------------------------\n")

}