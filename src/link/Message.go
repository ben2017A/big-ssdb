package link

import (
	"net"
)

type Message struct {
	Data string
	Addr net.Addr
}
