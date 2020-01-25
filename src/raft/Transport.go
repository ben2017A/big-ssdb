package raft

type Transport interface{
	Close()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	Send(msg *Message) bool
}
