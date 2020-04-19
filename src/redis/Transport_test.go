package redis

import (
	"testing"
	"fmt"
)

func TestTransport(t *testing.T){
	tcp := NewTransport("127.0.0.1", 9000)
	defer tcp.Close()

	for {
		select {
		case msg := <- tcp.C:
			fmt.Println(msg.Array())
		}
	}
}
