package main

import (
	"log"
	"raft"
)

type FakeTransport struct {
	C chan *raft.Message
	dns map[string]string
}

func NewFakeTransport() *FakeTransport {
	log.Println("New")
	t := new(FakeTransport)
	t.C = make(chan *raft.Message)
	t.dns = make(map[string]string)
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
}

func (t *FakeTransport)Disconnect(nodeId string) {
	delete(t.dns, nodeId)
}

func (t *FakeTransport)Send(msg *raft.Message) bool {
	log.Println("  send > ", msg.Encode())
	return true
}
