package link

import (
	"testing"
	"fmt"
)

func TestTcpServer(t *testing.T){
	tcp := NewTcpServer("127.0.0.1", 9000)
	defer tcp.Close()

	for {
		select {
		case msg := <- tcp.C:
			fmt.Println(msg.Data())
		}
	}
}
