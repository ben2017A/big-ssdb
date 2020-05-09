package server

import (
	"raft"
)

// 基于报文(消息)的非可靠传输, 带有流量控制
type Transport interface{
	Addr() string
	
	Close()

	// 每一个 node 使用不同的的 addr
	Connect(nodeId string, addr string)
	Disconnect(nodeId string)

	Recv() chan *raft.Message
	// 如果返回 false, 表示消息暂时无法投递出去, 原因可能是未连接, 或者发送队列已满
	Send(msg *raft.Message) bool
}
