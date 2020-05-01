# big-ssdb

```
export GOPATH=$(shell pwd)/
go test -cover -run Node
```

```
./test --redis_port 6379 --raft_port 8000 --init
./test --redis_port 6381 --raft_port 8001
```
