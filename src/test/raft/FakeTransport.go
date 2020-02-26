package main

import (
	"log"
	"time"
	"math/rand"

	"raft"
)

type FakeTransport struct {
	C chan *raft.Message
	dns map[string]string
	send_queues map[string]chan *raft.Message
}

func NewFakeTransport() *FakeTransport {
	log.Println("New")
	t := new(FakeTransport)
	t.C = make(chan *raft.Message, 3)
	t.dns = make(map[string]string)
	t.send_queues = make(map[string]chan *raft.Message)
	return t
}

func (t *FakeTransport)Addr() string {
	return "addr"
}
	
func (t *FakeTransport)Close() {
	log.Println("Close")
}

func (t *FakeTransport)Connect(nodeId string, addr string) {
	t.dns[nodeId] = addr
	C := make(chan *raft.Message, 3)
	t.send_queues[nodeId] = C
	go func(){
		for {
			msg := <- C
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			t.C <- msg
		}
	}()
}

func (t *FakeTransport)Disconnect(nodeId string) {
	delete(t.dns, nodeId)
}

func (t *FakeTransport)Send(msg *raft.Message) bool {
	log.Println("  send > ", msg.Encode())
	go func(){
		t.send_queues[msg.Dst] <- msg
	}()
	return true
}
