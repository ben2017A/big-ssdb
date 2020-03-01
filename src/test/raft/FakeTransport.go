package main

import (
	"log"
	"raft"
)

type FakeTransport struct {
	addr string
	bus *Bus
	c chan *raft.Message
}

type Bus struct {
	clients map[string]*FakeTransport
}

func NewBus() *Bus {
	b := new(Bus)
	b.clients = make(map[string]*FakeTransport)
	return b
}

func (b *Bus)MakeTransport(id string, addr string) *FakeTransport {
	t := newFakeTransport(b, addr)
	b.clients[id] = t
	return t
}

func (b *Bus)Send(msg *raft.Message) {
	log.Println(" send > ", msg.Encode())
	t := b.clients[msg.Dst]
	if t == nil {
		log.Printf("client %s not found", msg.Dst)
		return
	}
	t.C() <- msg
}

////////////////////////////////////////

func newFakeTransport(b *Bus, addr string) *FakeTransport {
	t := new(FakeTransport)
	t.bus = b
	t.addr = addr
	t.c = make(chan *raft.Message, 10)
	return t
}

func (t *FakeTransport)C() chan *raft.Message {
	return t.c
}

func (t *FakeTransport)Send(msg *raft.Message) bool {
	t.bus.Send(msg)
	return true
}

func (t *FakeTransport)Addr() string {
	return t.addr
}
	
func (t *FakeTransport)Close() {
	log.Println("Close")
}

func (t *FakeTransport)Connect(nodeId string, addr string) {
}

func (t *FakeTransport)Disconnect(nodeId string) {
}
