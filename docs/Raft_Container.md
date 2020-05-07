# Raft 容器

Raft 容器管理组实例和网络资源, 多个容器分布在多台机器上, 组成一个集群.

## 组实例

组实例是容器所管理的对象, 每一个组实例表示 Raft 组中的一个成员.

向一个 Container 发送命令, 初始化单节点集群:

```
group 0 init
```

```
group 0 join ip:port
group 0 quit
group 0 add_peer ip:port
group 0 del_peer ip:port
group 0 set_peers ip:port,ip:port // 仅初始化时用, 所有成员都要执行
```
