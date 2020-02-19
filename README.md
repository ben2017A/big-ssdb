# big-ssdb

每一个 Raft Group 只负责一个 Bucket 的数据.

Bucket: 单文件硬盘存储, 不采用 level, 不采用多 sst.

