package main

import (
	"time"
	"log"
	"fmt"
	
	"raft"
)

const TimerInterval = 100

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	
	bus := NewBus()
	t1 := bus.MakeTransport("n1", "addr1")
	t2 := bus.MakeTransport("n2", "addr2")

	s1 := NewFakeStorage()
	s2 := NewFakeStorage()

	n1 := raft.NewNode("n1", s1, t1)
	n2 := raft.NewNode("n2", s2, t2)
	// n1.Start()
	// n2.Start()
	// log.Println("started")
	log.Println("\n" + n1.Info())
	n1.Step()
	log.Println("\n" + n1.Info())
	
	n1.AddMember("n1", "addr1")
	n1.Step()
	log.Println("\n" + n1.Info())
	
	n1.AddMember("n2", "addr2")
	n1.Step()
	log.Println("\n" + n1.Info())
	n1.Step()
	log.Println("\n" + n1.Info())

	
	n2.JoinGroup("n1", "addr1")
	n2.Step()
	log.Println("\n" + n2.Info())

	
	n1.Step()
	log.Println("\n" + n1.Info())
	n2.Step()
	log.Println("\n" + n2.Info())

	return
	
	i := 0
	for ; i<2; i++ {
		s := fmt.Sprintf("%d", i)
		log.Println("client request:", s)
		n1.Write(s)
	}
	
	time.Sleep(4200 * time.Millisecond)
	for ; i<9; i++ {
		s := fmt.Sprintf("%d", i)
		log.Println("client request:", s)
		n1.Write(s)
	}

	for{
		time.Sleep(10 * time.Millisecond)
	}
}