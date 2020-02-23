
```
nc 127.0.0.1 9001
AddMember 8001 127.0.0.1:8001
AddMember 8002 127.0.0.1:8002
AddMember 8003 127.0.0.1:8003

nc 127.0.0.1 9002
JoinGroup 8001 127.0.0.1:8001

nc 127.0.0.1 9002
JoinGroup 8001 127.0.0.1:8001

while [ 1 ]; do nc 127.0.0.1 9001; sleep 1; done
```

Raft 集群启动方式:
    各节点单独启动, 均自动成为 leader
    选定一个 leader 节点, 向其发送
        [AddMember selfId selfAddr] 添加自己
        [AddMember nodeId nodeAddr] 添加其它节点
    向其它节点发送
        [JoinGroup leaderId leaderAddr]

