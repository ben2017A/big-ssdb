# XNA: Transaction

begin
commit


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


# DXNA: Distributed Transaction

begin
prepare
commit

READ:
	if has uncommitted xna, query Coordinator
