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

### node.join_group

删除 Raft Database, 然后 Connect leader. Leader 会在 Heartbeat 时发现其落后, 然后发 Install Snapshot 指令.

需要先 group_add_member 接受节点后, 其 join_group 才会生效, 否则其它节点会忽略它.

### node.quit_group

不删除数据, 进入 freeze 状态.

### group.add_member

* 客户端给 leader 发 AddMember 指令后, 新节点被集群接受
* 新节点启动后进入 freeze 状态
* 客户端给新节点发送 JoinGroup 指令

TODO: Service Database.

### group.del_member

给 leader 发 DelMember 指令后, 节点被从集群中剔除. 被删节点可能收到 DelMember log 也可能收不到, 可能收到 commit 也可能收不到.

所以, 需要给被删节点发送 quit_group 指令.

### container.split_group

组成员分成两批, 分别去组建新的 Group(fork 出不同的新组), 而旧组转成"销毁中"状态继续运行, 不对外提供服务, 仅执行垃圾回收任务.

TODO:

### container.merge_group

* 生成新组, initializing 状态
* 给旧组发送 prepare destroy 指令
* 旧组 promised destroy 状态, 不提供服务
	* 所有节点的日志必须全部 applied
* 新组 active 状态
* 旧组 destroying 状态

操作过程中, 新组和旧组指向的数据有重叠. 注意, 组的状态变更后, 不代表全部组成员都已变更, 而是经多数派共识.
