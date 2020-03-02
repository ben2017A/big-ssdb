# Raft and Service Snapshot

Snapshot 分为 Raft Snapshot 和 Service Snapshot. 快照的安装过程因为不是原子操作, 所以要保存中间状态, 重试以最终完成.

## Raft Snapshot

Leader 发现 follower 落后太多时, 将向其发送 InstallSnapshot 报文, 带有 Raft 状态的快照, 以及最近两条 committed 日志. 两条而不是一条日志, 是为了让 prev 校验能通过.

Follower 收到后, 清除 Service Database 和 Raft Database, 然后安装 Raft Snapshot.

未来可以从配置中心拉取 Raft Snapshot.

## Service Snapshot

Raft 在 Apply 日志时, 发送 Service 的 lastApplied 落后太多, 所以告知 Service 安装快照. Service 用自己的方式复制并安装快照, 更新 lastApplied(也即 Service Database 的 commitIndex).