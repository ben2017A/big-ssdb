# Raft and Service Snapshot

**应该由 leader 主动推 snapshot, 还是由 follower 主动拉?**

Snapshot 分为 Raft Snapshot 和 Service Snapshot. 快照的安装过程因为不是原子操作, 所以要保存中间状态, 重试以最终完成.

## 流程

安装快照是一个复杂且耗时的操作, 涉及许多内存数据的修改, 以及持久化数据的修改, 需要设计一个可重试的流程, 以保证操作的原子性.

应该 Service 先安装快照, 还是 Raft 先安装? Raft 独立处理自己的快照, 有两种方式安装快照:

1. Leader 发现 follower 落后太多, 要求 follower 安装快照
2. 外界调用让 Raft 清空自己的数据库, 然后会触发流程 1

## Raft Snapshot

InstallSnapshot 报文带有 Raft 状态快照, 以及最近两条 committed 日志. 两条而不是一条日志, 是为了让 prev 校验能通过.

未来可以从配置中心拉取 Raft Snapshot.

## Service Snapshot

Raft 发现 Service 的 lastApplied 落后太多时, 调用其 `InstallSnapshot()` 方法. Service 改变自己的状态, 从其它 Service 拉取 Service 数据快照.
