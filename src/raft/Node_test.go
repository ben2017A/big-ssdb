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

	sleep(0.1)
	log.Println("end")
}

func testSingleNode() {
	members := make(map[string]string)
	members["8001"] = "127.0.0.1:8001"
	conf := NewConfig("8001", members["8001"], members)
	n := NewNode(conf)
	n.Tick(5000)
	
	if n.Role != RoleLeader {
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

	go xport()

	n1.Tick(5000)
	sleep(0.1)

	if n1.Role != RoleLeader {
		log.Fatal("error")
	}

	t, i := n1.Propose("set a 1")
	t, i = n1.Propose("set b 2")
	log.Println("Propose", t, i)
	sleep(0.1)
	log.Println("\n" + n1.Info() + "\n" + n2.Info())
}

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}

func xport() {
	ns := []*Node{n1, n2}
	for {
		count := 0
		for _, n := range ns {
			n.Step()
			n.Tick(1)
		}
		for _, n := range ns {
			for len(n.SendC()) > 0 {
				count ++
				msg := <- n.SendC()
				log.Println("    send > " + msg.Encode())
				for _, dst := range ns {
					if dst.Id() == msg.Dst {
						dst.RecvC() <- msg
						// log.Println("    recv < " + msg.Encode())
					}
				}
			}
		}
		if count == 0 {
			sleep(0.001)
			// break
		}
	}
}
