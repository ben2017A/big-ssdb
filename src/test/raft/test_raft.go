package main

import (
	"time"
	"log"
	
	"raft"
)

const TimerInterval = 100

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	s1 := NewFakeStorage()
	s2 := NewFakeStorage()
	xport := NewFakeTransport()

	n1 := raft.NewNode("n1", s1, xport)
	n1.Start()

	n2 := raft.NewNode("n2", s2, xport)
	n2.Start()

	log.Println("started")
	
	go func(){
		time.Sleep(6000 * time.Millisecond)
		n1.AddMember("n1", "addr1")
		n1.AddMember("n2", "addr2")
	}()

	for{
		select{
		case <-ticker.C:
			n1.Tick(TimerInterval)
			n2.Tick(TimerInterval)
		case msg := <-xport.C:
			log.Println(msg)
			n1.HandleRaftMessage(msg)
		}
	}
}