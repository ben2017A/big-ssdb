package raft

// 基于报文(消息)的非可靠传输, 带有流量控制
type Transport interface{
	Addr() string
	
	Listen(addr string) error
	Close()

	Connect(nodeId string, addr string) error
	Disconnect(nodeId string)

	// 如果发送失败, 不应立即重试
	Send(msg *Message) bool
	Recv() chan *Message
}
