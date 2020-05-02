package util

import (
	"strings"
	"bytes"
	// "log"
)

func ReplaceBytes(s string, src []string, dst []string) string {
	for i, _ := range src {
		s = strings.Replace(s, src[i], dst[i], -1)
	}
	return s
}

func StringEscape(s string) string {
	return string(BytesEscape([]byte(s)))
}

func StringUnescape(s string) string {
	return string(BytesUnescape([]byte(s)))
}

func BytesEscape(bs []byte) []byte {
	var buf bytes.Buffer
	var s int = 0
	var e int = 0
	var c byte
	for e, c = range bs {
		var d string
		switch c {
		case '\\':
			d = "\\\\" 
		case '\r':
			d = "\\r"
		case '\n':
			d = "\\n"
		default:
			continue
		}
		buf.Write(bs[s : e])
		buf.WriteString(d)
		s = e + 1
	}
	if s == 0 && e == len(bs) - 1 {
		return bs // no copy
	}
	if s <= e {
		buf.Write(bs[s : e + 1])
	}
	return buf.Bytes()
}

func BytesUnescape(bs []byte) []byte {
	var buf bytes.Buffer
	var s int = 0
	var e int = 0
	var p byte = 0
	var c byte = 0
	for e, c = range bs {
		// log.Printf("%c", c)
		if p == '\\' {
			var d byte
			switch c {
			case '\\':
				d = '\\'
			case 'r':
				d = '\r'
			case 'n':
				d = '\n'
			default:
				p = c
				continue
			}
			// log.Println(s, e, len(bs))
			buf.Write(bs[s : e - 1])
			buf.WriteByte(d)
			s = e + 1
		}
		p = c
	}
	if s == 0 && e == len(bs) - 1 {
		return bs // no copy
	}
	if s <= e {
		buf.Write(bs[s : e + 1])
	}
	return buf.Bytes()
}
