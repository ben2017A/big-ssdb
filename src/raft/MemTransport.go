package raft

import (
	"sync"
	log "glog"
)

/*
Usage:
t := NewMemTransport()
t.Listen("")
t.AddNode(n)
...
t.Close()
*/
type MemTransport struct {
	sync.Mutex
	nodes map[string]*Node
	send_c chan *Message
}

func NewMemTransport() *MemTransport {
	t := new(MemTransport)
	t.send_c = make(chan *Message, 100)
	t.nodes = make(map[string]*Node)
	return t
}

func (t *MemTransport)Addr() string {
	panic("not implemented")
}
	
func (t *MemTransport)Listen(addr string) error {
	mq := make(chan *Message, 100)
	go func() {
		for {
			msg := <- t.send_c
			if msg == nil {
				close(mq)
				return
			}
			mq <- msg
		}
	}()
	go func() {
		for {
			msg := <- mq
			if msg == nil {
				return
			}
			t.Lock()
			node := t.nodes[msg.Dst]
			t.Unlock()
			if node != nil {
				node.RecvC() <- msg
			}
		}
	}()
	return nil
}

func (t *MemTransport)Close() {
	t.Lock()
	defer t.Unlock()
	for _, n := range t.nodes {
		n.Close()
	}
	close(t.send_c)
}

func (t *MemTransport)AddNode(node *Node) {
	t.Lock()
	defer t.Unlock()
	t.nodes[node.Id()] = node
}

func (t *MemTransport)DelNode(node *Node) {
	t.Lock()
	defer t.Unlock()
	node.Close()
	delete(t.nodes, node.Id())
}

func (t *MemTransport)Connect(nodeId string, addr string) error {
	panic("use AddNode(node)")
}

func (t *MemTransport)Disconnect(nodeId string) {
	t.Lock()
	defer t.Unlock()
	delete(t.nodes, nodeId)
}

// 如果发送失败, 不应立即重试
func (t *MemTransport)Send(msg *Message) bool {
	t.Lock()
	defer t.Unlock()

	node := t.nodes[msg.Dst]
	if node == nil {
		return false
	}
	log.Info("    send > " + msg.Encode())
	t.send_c <- msg
	return true
}

func (t *MemTransport)Recv() chan *Message {
	panic("Not allowed to call Recv(), message is automatically dispatched")
}
