package raft

type Transport interface{
	Stop()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	Send(msg *Message) bool
}
