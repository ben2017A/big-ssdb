package link

import (
	"testing"
	"fmt"
)

func TestServer(t *testing.T){
	tcp := NewServer("127.0.0.1", 9000)
	defer tcp.Close()

	for {
		select {
		case msg := <- tcp.C:
			fmt.Println(msg.Array())
		}
	}
}
