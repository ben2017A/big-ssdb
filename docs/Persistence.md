# Persistence


## Binlog

Raft 状态机就是 binlog 的实现. Binlog 记录的是业务操作, 不一定是幂等, 例如 incr 操作.

## Redolog

Redolog 记录的是 set 和 del 操作, 是幂等的. Redolog 先缓冲在内存中(默认事务), 记录最新的 index, 然后再写入 redolog 文件:

	begin #index
	set a=1
	del b
	commit #index

Fsync(), 接着刷新 db, 最后往 redolog 写入 checkpoint:

	check

故障重启时, 将最后一个 checkpoint 之后的 log 重新执行一遍更新 db. 如果出现 begin 而不出现 commit, 则忽略 begin 之后的 redolog.

## Transaction

当 binlog 中出现 begin 时, 新建一个内存中的 Transaction.

每一个 Transaction 有最小 index 和最大 index. 将 committed 的事务合并, 如果与 uncommitted 事务无 index 交集, 则可作为一个新的事务写入 redolog.

http://www.mathcs.emory.edu/~cheung/Courses/377/Syllabus/10-Transactions/redo-log.html

(Transaction execution use in-place update/write operation) and (Transaction implementation uses an UNDO log )
(Transaction execution use deferred update/write operation) and (Transaction implementation uses a REDO log )

# Service

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


