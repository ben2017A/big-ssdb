package main

import (
	"time"
	"log"
	"fmt"
	
	"raft"
)

const TimerInterval = 100

var n1, n2 *raft.Node

func init2() {
	db1 := NewFakeDb()
	n1 = raft.NewNode("n1", "addr1", db1)
	db2 := NewFakeDb()
	n2 = raft.NewNode("n2", "addr2", db2)

	go func() {
		for {
			var msg *raft.Message = nil
			select {
			case msg = <- n1.SendC():
				log.Println(" send > " + msg.Encode())
			case msg = <- n2.SendC():
				log.Println(" send > " + msg.Encode())
			}
			if msg.Dst == "n1" {
				n1.RecvC() <- msg
			} else {
				n2.RecvC() <- msg
			}
			// log.Println(" receive > " + msg.Encode())
		}
	}()
}

func setup_master() {
	log.Println("init:\n" + n1.Info())
	n1.Tick(raft.ElectionTimeout * 2)
	if n1.Role != raft.RoleFollower {
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
	n1.Step()
}

func setup_new_follower() {
	n2.JoinGroup("n1", "addr1")
	n2.Step() // recv Heartbeat, send Ack[false]
	if n2.Role != raft.RoleFollower {
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

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	init2()

	setup_master()
	fmt.Printf("\n---------------------------------------------------\n\n")
	setup_new_follower()
	fmt.Printf("\n---------------------------------------------------\n\n")
	
	time.Sleep(100 * time.Millisecond)
}