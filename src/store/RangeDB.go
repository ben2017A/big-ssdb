package store

/*
RangeDB 是单文件存储数据库, 天生为分布式存储而设计. 无 Log, 而是依赖 Raft
或者其它机制, Commit 之后才持久化.
*/

