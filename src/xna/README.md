# Persistence

保证可靠传输的基础, 就是 TCP 的三大特性: 序号, 确认, 重传. 重传有一个隐含的要求, 那就是要求消息是幂等的, 但是在交互设计中, 交互(操作)一般不是幂等的, 所以这里不能生搬硬套 TCP. 但是, 重传又是必须的, 所以, 必须把非幂等的操作, 转换成幂等的操作.

## Binlog

Raft 状态机就是 binlog 的实现. Binlog 记录的是业务操作, 不一定是幂等, 例如 incr 操作.

## Redolog

Redolog 记录的是 set 和 del 操作, 是幂等的. Redolog 先缓冲在内存中(默认事务), 记录最新的 index, 然后再写入 redolog 文件:

	begin
	set a=1
	del b
	commit

Fsync(), 接着刷新 db, 最后往 redolog 写入 checkpoint:

	check

故障重启时, 将最后一个 checkpoint 之后的完整的事务重新执行一遍更新 db. 如果出现 begin 而不出现 commit, 则忽略 begin 之后的 redolog.

## Transaction

当 binlog 中出现 begin 时, 新建一个内存中的 Transaction.

每一个 Transaction 有最小 index 和最大 index. 将 committed 的事务合并, 如果与 uncommitted 事务无 index 交集, 则可作为一个新的事务写入 redolog.

http://www.mathcs.emory.edu/~cheung/Courses/377/Syllabus/10-Transactions/redo-log.html

> (Transaction execution use in-place update/write operation) and (Transaction implementation uses an UNDO log )
> (Transaction execution use deferred update/write operation) and (Transaction implementation uses a REDO log )

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


