package raft

type Transport interface{
	Addr() string
	
	Close()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	Send(msg *Message) bool
}
