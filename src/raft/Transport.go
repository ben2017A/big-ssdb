package raft

type Transport interface{
	Addr() string
	
	Close()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	C() chan *Message
	// thread safe
	Send(msg *Message) bool
}
