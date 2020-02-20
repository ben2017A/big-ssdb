# Persistence

## Binlog

Raft 状态机就是 binlog 的实现. Binlog 记录的是业务操作, 不一定是幂等, 例如 incr 操作.

## Redolog

Redolog 记录的是 set 和 del 操作, 是幂等的. 每一条 redolog 都隐式地 commit 一条 binlog.

Binlog 转换成幂等操作, 然后写入 redolog. 写入成功之后, 等待更新 db. 如果 db 更新成功, 往 binlog 中追加 checkpoint.

	set a=1
	checkpoint
	set b=2
	del a

故障重启时, 将最后一个 checkpoint 之后的 log 重新执行一遍更新 db.

仅依赖 redolog 即可实现原子性(Atomic), 只需要重放时, 忽略不完整的事务即可(只有 begin 和 commit 之间的 redolog 才是有效的).

**优化:** binlog 和 redolog 不必一一对应, 可以将 redolog 缓存起来, 再最终持久化, 最后一条 redolog 带有 commit index.

## Undolog

Undolog 是为了支持实现事务回滚, 如果不需要回滚, 只用 redolog 即可. 在事务提前交, 将即将被修改的数据进行备份, 还要记录即将被 insert 的新数据. 如果事务回滚或者执行失败, 将备份数据恢复回去即可.

Undolog 使用 set 和 del 两个操作, 和 Redolog 类似.

	...
	checkpoint
	set key=old_value
	del new_key
	...

事务回滚或者中断时, 或者故障重启时, 找到最后一个 checkpoint, 将其后面的语句执行一遍.

要求, 在前一个事务未提交前(checkpoint), 后一个事务不写 Undolog.

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


