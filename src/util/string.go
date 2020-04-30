package util

import (
	"strings"
	"bytes"
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

func BytesEscape(s []byte) []byte {
	var buf bytes.Buffer
	for _, c := range s {
		switch c {
		case '\\':
			buf.WriteString("\\\\")
		case '\r':
			buf.WriteString("\\r")
		case '\n':
			buf.WriteString("\\n")
		default:
			buf.WriteByte(c)
		}
	}
	return buf.Bytes()
}

func BytesUnescape(s []byte) []byte {
	var buf bytes.Buffer
	var p byte = 0
	for _, c := range s {
		if p == '\\' {
			switch c {
			case '\\':
				buf.WriteByte('\\')
			case 'r':
				buf.WriteByte('\r')
			case 'n':
				buf.WriteByte('\n')
			default:
				buf.WriteByte(p)
				buf.WriteByte(c)
			}
		} else {
			if c != '\\' {
				buf.WriteByte(c)
			}
		}
		p = c
	}
	return buf.Bytes()
}
