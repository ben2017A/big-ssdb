# Raft 集群启动

## 启动节点

* peers=[], 以空成员列表启动节点, 节点既不能自己 leader, 也无法加入其它集群, 成为"孤儿"节点
* peers=[self], 以自己作为唯一成员启动节点, 组建单节点集群, 节点将成为 leader
* peers=[leader], 以现有集群的 leader 作为唯一成员启动节点, 节点会尝试加入现有集群, 成为"申请者"

注意, "申请者"节点在经过现有集群同意前, 它的消息将被现有集群所有节点丢弃. 经现有集群同意后, 申请者会收到 leader 的心跳包.


## 1. 从单节点开始组建集群

此种方法先启动一个空的组, 再依次修改 Raft 配置将各节点加入到组当中.

### 启动第一个节点

* 以 peers=[A] 启动 A
* A 自动成为 leader
* A 成为 leader 后, 持久化集群成员列表, 即 peers=[A]

### 配置集群, 接受新节点

* 请求 A, A.ProposeAddMember(B)
* 现在持久化的成员列表是 [A, B]

### 新节点启动, 加入集群

* 以 peers=[A] 启动 B
* 因为 B 自己没有在成员列表, 所以无法成为 leader
* B 收到 A 的心跳包, 回复自己的 lastIndex
* A 将 B 所缺失的 log 同步给 B


## 2. 一次组建多节点集群

此方法让组的所有节点以完全相同的成员列表启动, 这样它们的配置是一致的. 启动后, leader 会将初始的配置写入 Raft 日志.

### 所有节点以相同的配置启动

* 以 peers=[A, B] 启动 A, B
* 两者会自动竞争 leader
* 成为 leader 者持久化成员列表(写入 raft log)
