# Raft

## 设计原则

### 分层与解耦

Raft 只负责 log 同步, Subscriber/Service 只负责 log 重放. Service引入 Apply 和 Commit 两个概念, 让 Raft 与业务无关.

### Raft

当 Raft Commit(和 Service 的 Commit 无关) 了一条 log 之后, 它会同步地将该条 log Appply 到所有的 Service, 然后再 Commit 下一条(批) log.

### Service

Service 对 log 的 Apply 操作是幂等的, 因为, Apply 操作并不一定是真正的持久化, 也可能只是 Apply 到 Service 自己的 write buffer 中, 所以Service 重启后可能需要 Raft 重传某一条 log. 当 Service 真正地 Commit一条 log 之后, 才表示它持久化了此条 log.

### 讨论

日志同步和业务处理是异步的, Apply 提供了两者同步的可能性. 同时, 如果 Service能在 Apply 里 Commit log, 那么, Raft 就不需要持久化了(为了就对重传需求,最好还是持久化).

Raft 和 Service 完全独立的设计, 可能导致数据存在两个副本的情况: 一个副本由Raft 存储成 Raft log 的形式, 另一个副本是业务数据本身. 好处是解耦本身, 坏处也是显而易见的.

Snapshot 不应该属于 Raft 的职责范围.

Leader 选举的是多数派里日志最多的一个节点, 并非绝对意义上的日志最多最全的. 因为这样的节点即使投反对票, 也无济于事, 等待它的是在新 Leader 上台后最终被覆盖.
