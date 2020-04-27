# 日志的生命周期

日志的生命周期: propose append commit/cancel apply

append 是异步的.

commit 只是一个标记.

对于写请求, commit 之后即可返回给客户端，但读请求要保证之前的都已 apply.
