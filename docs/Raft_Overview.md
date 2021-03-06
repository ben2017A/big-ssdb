## Raft Overview

### 分层与解耦

Raft 只负责 log 同步, Service 只负责 log 重放. 日志分为 Conf 和 Data, 对于 Conf 类型, Raft 自己会 apply, 对于 Data 类型, 则交给 Service 处理.

### Raft

当 Raft Commit 了一条 log 之后, 它会同步地将该条 log appply, 然后再 Commit 下一条(批) log.

### Service

Raft 异步地将 log 发送给 Service 进行 apply.

Service 对 log 的 Apply 操作是幂等的, 因为, Apply 操作并不一定是真正的持久化, 也可能只是 Apply 到 Service 自己的 write buffer 中, 所以 Service 重启后可能需要 Raft 重传某一条 log.

### 讨论

日志同步和业务处理是异步的, Apply 提供了两者同步的可能性. 同时, 如果 Service 能在 Apply 里 Commit log, 那么, Raft 就不需要持久化了(为了应对重传需求,还是要持久化).

Raft 和 Service 完全独立的设计, 可能导致数据存在两个副本的情况: 一个副本由Raft 存储成 Raft log 的形式, 另一个副本是业务数据本身. 好处是解耦本身, 坏处也是显而易见的.

Snapshot 不应该属于 Raft 的职责范围, 除非侵入 Service, 否则 Raft 不可能对 Raft log 做 compaction. 不同于 Paxos 知道 instance id, Raft 不知道业务的任何信息, 所知道的只有 log 的 term 和 index, 只有这两项信息根本不可能做 compaction. Raft 针对 log 唯一能做的只有淘汰太过久远的 log.

Leader 选举的是多数派里日志最多的一个节点, 而非绝对意义上的日志最多最全的. 因为这样的节点即使投反对票, 也无济于事, 等待它的是在新 Leader 上台后最终被覆盖.

### 一致性读

* 必须通过 Leader 读.
* Leader 在返回响应前, 需要向多数节点确认它确实是 Leader.
* Leader 租约(租约过期前, 其它节点不发起新投票或响应投票), 可以减少这种确认.

## 注意持久化的原子性

任何需要保存数据(即持久化)的地方, 都需要注意保存操作的原子性. 一般通过最后保存"完成"状态来实现, 如果最后的状态没有保存成功, 则需要重试整个流程.

### 相关链接

* [PDF: Raft - In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
* [PDF: Designing for Understandability: the Raft Consensus Algorithm](https://raft.github.io/slides/uiuc2016.pdf)
* [PDF: Raft: A Consensus Algorithm for Replicated Logs](https://raft.github.io/slides/raftuserstudy2013.pdf)
