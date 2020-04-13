# 启动集群

## 1. 渐进式

### 启动节点, 组建单节点集群

* 以 members=null 或者 members=[A] 启动 A
* A 自动成为 leader
* A 被自动添加到持久化的集群成员列表

### 配置集群, 接受新节点

* 请求 A, A.AddMember(B)
* 现在持久化的成员列表是 [A, B]

### 新节点启动

* 以 members=[A, B] 启动 B
* B 将认 A 为 leader
* B 从 A 同步 raft 日志


## 2. 默认配置式

### 所有节点以相同的配置启动

* 以 members=[A, B] 启动 A, B
* 两者会自动竞争 leader
