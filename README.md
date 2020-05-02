# big-ssdb

```
export GOPATH=$(shell pwd)/
go test -run Node
go test string.go string_test.go -test.bench .*
```

```
./test --redis_port 6379 --raft_port 8000 --init
./test --redis_port 6381 --raft_port 8001
```
