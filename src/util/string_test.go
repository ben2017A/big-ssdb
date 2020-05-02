package util

import (
	"testing"
	"fmt"
	"bytes"
)

// go test string.go string_test.go -test.bench .*

func TestStrings(t *testing.T){
	src := "a\\b\nc\n"
	dst := "a\\\\b\\nc\\n"
	if StringEscape(src) != dst {
		t.Fatal(dst, StringEscape(src))
	}
	rev := StringUnescape(dst)
	fmt.Println(src)
	fmt.Println(dst)
	fmt.Println(rev)
}

func Benchmark_Escape(b *testing.B) {
	src := "*2\r\n$3\r\nget\r\n$1\r\naaaaaaaaaaaaaaaaaaaaaaaaanaaaaaaaaaaaaaaaaaaaaaaaaa\r\n"
	for i := 0; i < b.N; i++ {
		StringEscape(src)
	}
}

func Benchmark_Escape2(b *testing.B) {
	src := "*2\r\n$3\r\nget\r\n$1\r\naaaaaaaaaaaaaaaaaaaaaaaaanaaaaaaaaaaaaaaaaaaaaaaaaa\r\n"
	// dst := "*2\\r\\n$3\\r\\nget\\r\\n$1\\r\\naaaaaaaaaaaaaaaaaaaaaaaaanaaaaaaaaaaaaaaaaaaaaaaaaa\\r\\n"
	for i := 0; i < b.N; i++ {
		StringEscape2(src)
	}
}

func StringEscape2(s string) string {
	return string(BytesEscape2([]byte(s)))
}

func BytesEscape2(s []byte) []byte {
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
