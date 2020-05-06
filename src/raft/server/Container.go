package server

type Container struct {

}

// 在容器内创建一个组实例
func (c *Container)NewGroup(groupId int) {
}

// 自主决定加入组(无 Raft), 收到 leader 消息后, 将组实例设置成正常服务状态
func (c *Container)JoinGroup(groupId int) {
}

// 自主决定退出组(无 Raft), 并将组实例设置成停止状态
func (c *Container)QuitGroup(groupId int) {
}

// 走 Raft 流程改变组配置, 要求当前节点是指定组的 leader
func (c *Container)ProposeAddPeer(groupId int, nodeId string) {
}

// 走 Raft 流程改变组配置, 要求当前节点是指定组的 leader
func (c *Container)ProposeDelPeer(groupId int, nodeId string) {
}
