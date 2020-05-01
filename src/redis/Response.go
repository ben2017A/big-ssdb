package redis

import (
	"fmt"
	"bytes"
)

const (
	TypeOK = iota
	TypeError
	TypeNull
	TypeInt
	TypeBulk
	TypeArray
)

type Response struct {
	Dst int
	_type int
	vals []string
}

func (r *Response)ReplyError(msg string) {
	r._type = TypeError
	r.vals = []string{"ERR", msg}
}

func (r *Response)ReplyError2(code string, msg string) {
	r._type = TypeError
	r.vals = []string{code, msg}
}

func (r *Response)ReplyNull() {
	r._type = TypeNull
}

func (r *Response)ReplyInt(num int64) {
	r._type = TypeInt
	r.vals = []string{fmt.Sprintf("%d", num)}
}

func (r *Response)ReplyBulk(b string) {
	r._type = TypeBulk
	r.vals = []string{b}
}

func (r *Response)ReplyArray(ps []string) {
	r._type = TypeArray
	r.vals = ps
}

func (r *Response)Encode() string {
	var buf bytes.Buffer
	switch r._type {
	case TypeOK:
		buf.WriteString("+OK\r\n");
	case TypeError:
		buf.WriteString("-");
		buf.WriteString(r.vals[0]);
		buf.WriteString(" ");
		buf.WriteString(r.vals[1]);
		buf.WriteString("\r\n");
	case TypeNull:
		buf.WriteString("$-1\r\n");
	case TypeInt:
        buf.WriteString(":");
        buf.WriteString(r.vals[0]);
        buf.WriteString("\r\n");
	case TypeBulk:
        buf.WriteString("$");
        buf.WriteString(fmt.Sprintf("%d", len(r.vals[0])));
        buf.WriteString("\r\n");
        buf.WriteString(r.vals[0]);
        buf.WriteString("\r\n");
	case TypeArray:
        buf.WriteString("*");
        buf.WriteString(fmt.Sprintf("%d", len(r.vals)));
		buf.WriteString("\r\n");
		for _, s := range r.vals {
			buf.WriteString("$");
			buf.WriteString(fmt.Sprintf("%d", len(s)));
			buf.WriteString("\r\n");
			buf.WriteString(s);
			buf.WriteString("\r\n");
		}
	}
	return buf.String()
}
