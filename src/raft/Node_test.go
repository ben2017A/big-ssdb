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

	fmt.Printf("\n=========================================================\n")
	testStartup()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	test2Node()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testQuit()
	clean_nodes()

	sleep(0.01)
	log.Println("end")
}

func testStartup() {
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

	if n1.conf.members["n2"].MatchIndex != 2 || n1.conf.members["n2"].NextIndex != 3 {
		log.Fatal("error")	
	}

	n1.Propose("set a 1")
	sleep(0.01)
}

func test2Node() {
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

	t, i := n1.Propose("set a 1")
	log.Println("Propose", t, i)
	t, i = n1.Propose("set b 2")
	log.Println("Propose", t, i)
	sleep(0.01) // wait Tick to send ping
	// log.Println("\n" + n1.Info() + "\n" + n2.Info())
}

func testQuit() {
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
	n1.Tick(HeartbeatTimeout) // n1 start election
	sleep(0.01) // wait startup

	n1.ProposeDelMember("n2")
	sleep(0.01)
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	if n2.role != RoleFollower {
		log.Fatal("error")
	}

	mutex.Lock()
	defer mutex.Unlock()
	n2.CleanAll() // clean all data
	n2.Close()
	log.Printf("%s stopped", n2.Id())
	delete(nodes, n2.Id())
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
