package redis

import (
	"log"
	"bytes"
	"strings"
	"testing"
)

func escape(s string) string {
	s = strings.Replace(s, "\r", "\\r", -1)
	s = strings.Replace(s, "\n", "\\n", -1)
	return s
}

func TestMessage(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	msg := new(Message)
	var buf bytes.Buffer

	buf.WriteString("  \t2\nab\n\r\nget a\r\n")
	buf.WriteString(" *2\n$1\na\n$1\nb\n\n")

	for buf.Len() > 0 {
		n := msg.Decode(buf.Bytes())
		if n == 0 {
			break
		}
		log.Println(n, msg.Array(), escape(msg.Encode()))
		buf.Next(n)
	}
}
