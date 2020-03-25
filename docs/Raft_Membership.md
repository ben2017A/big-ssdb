# Raft Membership

Multi 使用一个单独的 Raft Group 作为管理组, 以进行集群管理. 这样, 可以简化 Node 的代码, 让 Node 只专注于同步日志.

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

### multi.new_group

通过 add_member/join_group 流程创建新组.

### multi.split_group

* 创建两个新组
* 新组 set_range, "只读"
* 旧组转为"只读"
* 新组提供服务
* 旧组转成"销毁中"状态继续运行, 不对外提供服务, 仅执行垃圾回收任务.

### multi.merge_group

创建新组.

选定要合并的两个组中的其中一个, 向其发送 merge 操作, 经 raft 同步后即可生成新组, 然后 container 命令两个旧组转成"销毁中".
