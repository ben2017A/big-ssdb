package raft

import (
	"testing"
	"log"
	"fmt"
	"time"
	"sync"
)

var n1 *Node
var n2 *Node
var mutex sync.Mutex
var nodes map[string]*Node

func TestNode(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	nodes = make(map[string]*Node)

	go networking()

	testSnapshot()

	fmt.Printf("\n=========================================================\n")
	// testOneNode()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testTwoNodes()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testOneNode()
	// testJoin()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testTwoNodes()
	// testQuit()
	// clean_nodes()

	sleep(0.01)
	log.Println("end")
}

func testSnapshot() {
	testOneNode()
	n1.Propose("a")
	n1.Propose("b")
	// n1.Propose("c")
	sleep(0.01)

	testJoin()
	sleep(0.01)
}

func testOneNode() {
	mutex.Lock()
	{
		n1 = NewNode(NewConfig("n1", []string{"n1"}))
		nodes[n1.Id()] = n1
	}
	mutex.Unlock()

	n1.Start()
	sleep(0.01) // wait startup
	if n1.role != RoleLeader {
		log.Fatal("error")
	}
	if n1.commitIndex != 1 {
		log.Fatal("error")
	}
}

func testTwoNodes() {
	mutex.Lock()
	{
		members := []string{"n1", "n2"}
		n1 = NewNode(NewConfig("n1", members))
		n2 = NewNode(NewConfig("n2", members))
		nodes[n1.Id()] = n1
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n1.Start()
	n2.Start()
	sleep(0.01) // wait startup

	n1.Tick(HeartbeatTimeout) // n1 start election
	sleep(0.01)   // wait network

	if n1.role != RoleLeader {
		log.Fatal("error")
	}
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.commitIndex != n2.commitIndex {
		log.Fatal("error")	
	}
}

func testJoin() {
	n1.ProposeAddMember("n2")
	sleep(0.01)
	if n1.conf.members["n2"] == nil {
		log.Println("error")
	}

	mutex.Lock()
	{
		n2 = NewNode(NewConfig("n2", []string{"n1"}))
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n2.Start()
	sleep(0.01) // wait startup
	if n2.role != RoleFollower {
		log.Fatal("error")
	}

	n1.Tick(HeartbeatTimeout) // send ping
	sleep(0.01)

	if n1.commitIndex != n2.commitIndex {
		log.Fatal("error")	
	}
}

func testQuit() {
	n1.ProposeDelMember("n2")
	sleep(0.01)
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	sleep(0.01)
}
//////////////////////////////////////////////////////////////////

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}

func clean_nodes(){
	sleep(0.01) // wait proceed commit

	mutex.Lock()
	defer mutex.Unlock()

	for id, n := range nodes {
		n.Close()
		delete(nodes, id)
		log.Printf("%s stopped", id)
	}
}

func dispatch(id string){
	mutex.Lock()
	defer mutex.Unlock()

	n := nodes[id]
	if n == nil {
		return
	}
	if len(n.SendC()) == 0 {
		return
	}

	msg := <- n.SendC()
	log.Println("    send > " + msg.Encode())

	if nodes[msg.Dst] != nil {
		nodes[msg.Dst].RecvC() <- msg
	}
}

func networking() {
	for {
		mutex.Lock()
		for id, n := range nodes {
			n.Tick(1)
			go dispatch(id)
		}
		mutex.Unlock()

		sleep(0.001)
	}
}
