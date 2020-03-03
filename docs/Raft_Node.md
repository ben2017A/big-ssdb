# Raft Node

## 状态

* freeze: 未初始化
* logger: 接收 log, 但不 apply, 参与投票但不参加竞选
* follow: 接收 log, 并 apply 到 Service, 参与投票但不参加竞选
* normal: 可参加竞选

freeze 状态不响应任何 Raft 请求.

### 状态变更

* freeze -> logger: Raft 安装完 Snapshot 后
* logger -> freeze: 收到 InstallSnapshot 消息后, 立刻回退
* follow -> freeze: 收到 InstallSnapshot 消息后, 立刻回退
* logger -> follow: Service 安装完 Snapshot 后
* follow -> logger: 发现 Service 的 lastApplied 落后太多, 或者 lastApplied 对应的日志不存在
* follow -> normal: 由外部使用者发起

## 指令设计

### AddMember

* 客户端给 leader 发 AddMember 指令后, 新节点被集群接受
* 新节点启动后进入 freeze 状态
* 客户端给新节点发送 JoinGroup 指令

TODO: Service Database.

### DelMember

给 leader 发 DelMember 指令后, 节点被从集群中剔除. 被删节点可能收到 DelMember log 也可能收不到, 可能收到 commit 也可能收不到.

所以, 需要给被删节点发送 QuitGroup 指令.

### JoinGroup

删除 Raft Database, 然后 Connect leader. Leader 会给节点发 Install Snapshot 指令.

### QuitGroup

删除 Raft Database, 但不删除 Service Database. 进入 freeze 状态.

