package raft

type Transport interface{
	Stop()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	SendTo(buf []byte, nodeId string) int
}
