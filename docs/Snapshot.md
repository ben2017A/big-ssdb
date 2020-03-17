# Snapshot

## Raft Snapshot

Raft 快照数据量非常小, 只包括 currentTerm, index, 以及最新的几条日志. 所以, 由 Raft 自己来分发快照给新加入的节点或者同步失效的节点.

## Service Snapshot

Service 快照一般数量非常大, 例如几百 G 甚至数 T, 所以它的传输和安装不关 Raft 的事, 这一点和 Raft 协议的最初设计不同. 我认为 Raft 协议对 Service 快照有错误认知.

Service 快照可能存在独立的备份中心, 而且从工程实践经验上来看, 快照不应该由集群中的节点提供, 因为这将导致集群的节点的硬盘 IO 以及网络带宽被占用. 如果提供快照的节点是 leader, 那么整个集群可能会被带入不可用的状态.
