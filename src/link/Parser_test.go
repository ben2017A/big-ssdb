package link

import (
	"log"
	"testing"
)

func TestParser(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	p := new(Parser)

	p.AppendString("  \t2\nab\n\r\nget a\r\n")
	p.AppendString(" *2\n$1\na\n$1\nb\n\n")
	log.Println(p.Parse())
	log.Println(p.Parse())
	log.Println(p.Parse())

}
