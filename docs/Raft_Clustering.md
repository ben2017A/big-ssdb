# 启动流程

Container 是处理请求的第一层关口, 它为集群的每一个组都创建一个组实例, 每一个组实例会有最多一个 Peer. 当组实例有 Peer 时, 表示当前 Container 可提供该组的服务(组是正常状态时). 若无 Peer, 它只是一个占位符, 不用点内存空间, 也不提供该组的服务.


## Membership

### InitPeer(groupId, raftSnapshot=null, serviceSnapshot=null)

使用相应的快照初始化 Peer, 状态为"Prepared". 在此之前, Container 并没有对应组的 Peer.

### JoinGroup(groupId)

收到此命令的 Container 将对应组的 Peer 变更为"Joining"状态. 收到 leader 的消息后, Peer 变更为"Following"状态.

### Group(groupId).AddMember(nodeId)

Container 将此命令转给指定组的 Peer 处理.


## Cluster Management

### NewGroup(range, members)

集群创建一个新组, 组的状态为"New", 不提供服务. 成员列表中的成员(即 ContainerId)需要创建新组的 Peer, 然后这个新组会自动选主.

### StartGroup(groupId)

将指定组变更为"Active", 正常提供服务. 要求指定组的 range 与当前正常提供服务的所有组的 range 无重叠.

### Group(oldGroupId).SplitInto(newGroupId1, newGroupId2)

### Group(srcGroupId).MergeInto(dstGroupId)



## 关于快照

如果 leader 发现 follower 落后太多, 它会告诉 follower 应该重新安装快照, 但不会把快照内容发给 follower. Follower 用自己的方式安装 Raft 快照和 Service 快照, 这不属于 Raft 的职责.

Group 初始化时要求 Raft 数据库为空, 但不要求 Service 数据库为空. 因为所有节点可以用同一个基准业务数据库来启动.
