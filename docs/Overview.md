# Overview

所有操作都是两阶段, 先 Apply, 然后 Commit.

Apply 可能只是写到内存, 类似 write(). Commit 才是真正的持久化, 类似 sync(). 如果不需要持久化, Apply 和 Commit 是等效的. 但调用者仍然认为它们不等效, 由实现者自己实现.
