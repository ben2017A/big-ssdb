export GOPATH=$(shell pwd)/

all:
	@echo export GOPATH=$(shell pwd)/
	go build test.go

test:
	# 需要设置环境变量, 在项目根目录运行 export GOPATH=`pwd`
	export set GOPATH=`pwd`
	go test -cover -run Redolog
	# to test -coverprofile coverage.out -run Test
	# go tool cover -html=coverage.out -o coverage.html

clean:
	go clean

