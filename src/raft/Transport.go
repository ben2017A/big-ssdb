package raft

// 各节点之间的通信是全异步的, 而不是请求响应模式
type Transport interface{
	Addr() string
	
	Close()
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	C() chan *Message
	// thread safe
	Send(msg *Message) bool
}
