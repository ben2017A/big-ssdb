# 集群

## 副本/Replica

Replica 管理着一个 Raft Node, 与同组的其它 Raft Node 交换共识.

状态:

* Active: 正常提供服务
* Stopped: 停止服务, 即将被销毁
* Preparing: 中间状态, 不提供服务
* SyncLost: 无法和其它副本保持同步, service.lastIndex 落后 binlog.firstIndex

## 容器/Container(进程)

容器是一个进程, 监听一个 ip:port 提供网络服务. 一个容器管理多个 Replica.

响应码:

* OK: 正常
* Failed: 处理请求过程出现错误, 保证请求不会在未来成功
* Pending: 请求在处理中, 其结果是未知的, 可能在未来成功, 也可能在未来失败
* Redirect: 服务正常, 客户端请求的副本不在此处, 应该重新路由
* Unavailable: 服务不可用, 客户端一般会将不可用的节点冷藏一段时间

前 3 个响应码反映了处理结果是三态的, 不同于常见的二态结果(成功或者失败), 这三态是: 成功, 失败, 未知.

### 管理接口

**组管理**

命令格式:

```
group #id #operate [arg0, arg1, ...]
```

命令 | 参数 | 说明
----|------|-----
info | 无 | 显示组实例的信息
join | 无 | 加入组
quit | 无 | 退出组
add_peer | node_id ip:port | 添加组成员
del_peer | node_id ip:port | 删除组成员
clean | 无 | 删除 raft log 和 replica
export | 无 | 生成快照
import | /path/to | 导入快照

注: node_id 是 raft.Node.id, 即使是不同容器上的 node_id, 也不能相同.

添加组成员和删除组成员需要进行两步操作, 一步操作是在被操作的成员上处理, 另一步操作是在 leader 上处理. 添加组成员必须两步操作都完成后才能生效. 而删除组成员, 只需要一步操作即可产生效果, 但两步完成后对于集群来说才算完整. 例如, 调用 quit 之后, 成员的 raft 模块就停止工作了; del_peer 之后, 被删除的成员就无法再收到 raft 消息.

