# big-ssdb

每一个 Raft Group 只负责一个 Bucket 的数据.

Bucket: 单文件硬盘存储, 不采用 level, 不采用多 sst.


## raft

### Features

* Leader election
* Membership changes
* Log replication
* Built-in log management
	* Log persistency
	* Log compaction
* Built-in RPC support
* Pluggable Log management interface for log managments
* Pluggable RPC interface for RPC implements

TODO: leader lease

### NOs

Does not implement something that is strongly considered as not part of the Raft protocol.

* NO snapshot

