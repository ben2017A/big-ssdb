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
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("\n=========================================================\n")
	test2Node()
	time.Sleep(10 * time.Millisecond)

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
	time.Sleep(3000 * time.Millisecond)
}

func xport() {
	ns := []*Node{n1, n2}
	for {
		count := 0
		for _, n := range ns {
			if len(n.SendC()) > 0 {
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
		for _, n := range ns {
			if len(n.RecvC()) > 0 {
				count ++
				n.Step()
			}
		}
		if count == 0 {
			time.Sleep(1 * time.Millisecond)
			// break
		}
	}
}
