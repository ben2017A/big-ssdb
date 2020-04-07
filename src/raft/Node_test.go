package raft

import (
	"testing"
	"log"
	"fmt"
	"time"
)

var n1 *Node
var n2 *Node

func TestNode(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	fmt.Printf("\n=========================================================\n")
	testSingleNode()
	sleep(0.01)
	fmt.Printf("\n=========================================================\n")
	test2Node()
	sleep(0.01)

	sleep(0.01)
	log.Println("end")
}

func testSingleNode() {
	members := make(map[string]string)
	members["8001"] = "127.0.0.1:8001"
	conf := NewConfig("8001", members["8001"], members)
	n := NewNode(conf)
	n.Start()
	
	if n.role != RoleLeader {
		log.Fatal("error")
	}
	n.Close()
}

func test2Node() {
	members := make(map[string]string)
	members["n1"] = "127.0.0.1:8001"
	members["n2"] = "127.0.0.1:8002"

	conf1 := NewConfig("n1", members["n1"], members)
	n1 = NewNode(conf1)

	conf2 := NewConfig("n2", members["n2"], members)
	n2 = NewNode(conf2)

	n1.Start()
	n2.Start()
	networking()
	sleep(0.01)

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

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}

func networking() {
	ns := map[string]*Node{n1.Id():n1, n2.Id():n2}
	for _, n := range ns {
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
		go func(n *Node){
			for {
				msg := <- n.SendC()
				log.Println("    send > " + msg.Encode())
				ns[msg.Dst].RecvC() <- msg
				// sleep(0.001)
			}
		}(n)
	}
}
