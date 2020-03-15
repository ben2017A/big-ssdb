# Database

保证可靠传输的基础, 就是 TCP 的三大特性: 序号, 确认, 重传. 重传有一个隐含的要求, 那就是要求消息是幂等的, 但是在交互设计中, 交互(操作)一般不是幂等的, 所以这里不能生搬硬套 TCP. 但是, 重传又是必须的, 所以, 必须把非幂等的操作, 转换成幂等的操作.

## Binlog

Raft 状态机就是 binlog 的实现. Binlog 记录的是业务操作, 不一定是幂等, 例如 incr 操作. Binlog 不是幂等的, 但因为有 redolog 记录了 commit index, 所以 binlog 也可重放.

Binlog 持久化之后, 并不能给客户端返回执行结果, 需要由 Manager 执行后, 才能知道执行的结果. Manager 执行后, 不代表 Redolog 已经持久化, Redolog 可能仅仅放在内存中. 所以, Manager 记录了 lastApplied 和 lastCommitted. 只有 lastCommitted 之前的数据, Binlog 才能清除.

## Redolog

Redolog 记录的是 set 和 del 操作, 是幂等的. Redolog 先缓冲在内存中(默认事务), 记录最新的 index, 然后再写入 redolog 文件:

	begin
	set a=1
	del b
	commit

持久化, 接着刷新 db, 最后往 redolog 写入 checkpoint:

	check

故障重启时, 将最后一个 checkpoint 之后的完整的事务重新执行一遍更新 db. 如果出现 begin 而不出现 commit, 则忽略 begin 之后的 redolog.

**Redolog 放大效应:** 删除一个集合时, 会生成 n 条 redolog.
**Undolog 放大效应:** 删除 key 时, 会将其 value 保存, 而 value 可能很大.

Redolog 的放大效应比 Undolog 的放大效应更可控.

## Storage

Storage 是最终的业务数据的组织, 可以用 B+Tree, LSM-Tree 等结构来组织.

## Transaction

http://www.mathcs.emory.edu/~cheung/Courses/377/Syllabus/10-Transactions/redo-log.html

> (Transaction execution use in-place update/write operation) and (Transaction implementation uses an UNDO log )
> (Transaction execution use deferred update/write operation) and (Transaction implementation uses a REDO log )

### 事务的执行

对于同样的 binlog 序列, 各节点执行的结果应该是一样的.

如果 Transaction 执行成功, 则正常写入 redolog; 如果失败, 则将空事务写入 redolog, 因为需要记录 binlog 执行进度. 事务有 minIndex 和 maxIndex, 与其它事务无交集的才能写入 redolog, 因为 binlog 必须连续持久化.

事务 commit 之后, redolog 和 storage 可能都没有做更改, 只是内存中的事务变更了状态.


## 关于 LevelDB 的 WriteBatch 原子性

LevelDB 承诺, WriteBatch 是原子性操作(Atomic), 也就是 all or nothing. 我原以为是通过 write() 系统调用的"原子性"来保证, 事实上, write() 对于普通文件, 在极端情况并不是原子性的, 它可能会返回比你期望的少的 size. 也就是说, write() 只写了一部分数据到硬盘上.

这样的极端情况包括: 硬盘空间满, RLIMIT_FSIZE 限制. 见: http://man7.org/linux/man-pages/man2/write.2.html

所以, LevelDB 会做 crc checksum, 能发现这种数据损坏的情况, 这样保证写入的数据的完整性(Atomic).

## Service

levels:
	UXNA - uncommitted transactions
	MVCC - committed transactions uncompacted
	BASE - compacted committed transactions

Each level uses a separated storage.
Support versioning on a non-versioning storage.

read uncommitted
	read: BASE + MVCC(new) + UXNA(new)
read committed
	read: BASE + MVCC(new)
repeatable read
	read: BASE + MVCC(ver)


