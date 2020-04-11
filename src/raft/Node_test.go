package raft

import (
	"testing"
	"log"
	"fmt"
	"time"
	"sync"
)

var mutex sync.Mutex
var nodes map[string]*Node

func TestNode(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	nodes = make(map[string]*Node)

	go networking()

	fmt.Printf("\n=========================================================\n")
	testSingleNode()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	test2Node()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testJoin()
	clean_nodes()

	sleep(0.01)
	log.Println("end")
}

func testSingleNode() {
	var n1 *Node

	mutex.Lock()
	{
		n1 = NewNode(NewConfig("n1", []string{"n1"}))
		nodes[n1.Id()] = n1
	}
	mutex.Unlock()

	n1.Start()
	if n1.role != RoleLeader {
		log.Fatal("error")
	}
}

func test2Node() {
	var n1 *Node
	var n2 *Node
	members := []string{"n1", "n2"}

	mutex.Lock()
	{
		n1 = NewNode(NewConfig("n1", members))
		n2 = NewNode(NewConfig("n2", members))
		nodes[n1.Id()] = n1
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n1.Start()
	n2.Start()

	n1.Tick(5000)
	sleep(0.01)

	if n1.role != RoleLeader {
		log.Fatal("error")
	}

	t, i := n1.Propose("set a 1")
	log.Println("Propose", t, i)
	t, i = n1.Propose("set b 2")
	log.Println("Propose", t, i)
	sleep(0.1) // wait Tick to send ping
	// log.Println("\n" + n1.Info() + "\n" + n2.Info())
}

func testJoin(){
	var n1 *Node
	var n2 *Node

	mutex.Lock()
	{
		n1 = NewNode(NewConfig("n1", []string{"n1"}))
		n2 = NewNode(NewConfig("n2", []string{"n2"}))
		nodes[n1.Id()] = n1
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n1.Start()
	n2.Start()

	var t int32
	var i int64
	t, i = n1.ProposeAddMember("n2")
	log.Println("Propose", t, i)

	sleep(0.01)
	if n1.conf.members["n2"] == nil {
		log.Println("error")
	}

	n1.Tick(5000) // send ping
	log.Printf(n1.Info())

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
	// go func(n *Node){
	// 	for {
	// 		n.Tick(100)
	// 		sleep(0.1)
	// 	}
	// }(n)
	// go func(n *Node){
	// 	for {
	// 		n.Step()
	// 		sleep(0.001)
	// 	}
	// }(n)
}
