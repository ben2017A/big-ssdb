# Raft Node

## 工作模式

* freeze: 未初始化
* logger: 接收 log, 但不 apply, 参与投票但不参加竞选
* follow: 接收 log, 并 apply 到 Service, 参与投票但不参加竞选
* normal: 可参加竞选

### 状态变更

* freeze -> logger: Raft 安装完 Snapshot 后.
* logger -> freeze: 收到 Snapshot 后, 立刻回退.
* follow -> freeze: 收到 Snapshot 后, 立刻回退.
* logger -> follow: Service 安装完 Snapshot 后.
* follow -> logger: 发现 Service 的 lastApplied 落后太多, 或者 lastApplied 对应的日志不存在.
* follow -> normal: 由外部使用者发起.

## Add Member

给 leader 发 AddMember 指令后, 新节点被集群接受.

给新节点发送 freeze 指令, 进入 freeze 模式. 不参与投票选主, 接收到日志后不回复 Ack, 即不响应任何请求.

给新节点发送 install-raft 指令, 它将复制 Raft Snapshot, 进入 logger 状态. 参与投票选主, 接收日志后加入 Ack. 但不接受业务请求, 因为业务数据是空的.

给新节点发送 intall-service 指令, 它将复制 Service Snapshot, 进入 follow 状态. 可以接受业务的非一致性读请求.

节点可在 follow 和 normal 两个状态之间转换, 但不能转成其它状态.