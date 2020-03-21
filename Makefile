export GOPATH=$(shell pwd)/

all:
	@echo export GOPATH=$(shell pwd)/
	go build src/node-server.go

test:
	# 需要设置环境变量, 在项目根目录运行 export GOPATH=`pwd`
	export set GOPATH=`pwd`
	cd src/xna; go test -run Redolog; cd

clean:
	go clean

