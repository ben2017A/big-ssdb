export GOPATH=$(shell pwd)/

all:
	go build test.go

clean:
	go clean

