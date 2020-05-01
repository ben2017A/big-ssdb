package redis

import (
	"log"
	"bytes"
	"strings"
	"strconv"
)

type Request struct {
	Src int
	Dst int
	arr []string
}

func NewRequest(arr []string) *Request {
	ret := new(Request)
	ret.arr = arr
	return ret
}

func (m *Request)Array() []string {
	return m.arr
}

func (m *Request)Cmd() string {
	if len(m.arr) > 0 {
		return m.arr[0]
	}
	return ""
}

func (m *Request)Key() string {
	if len(m.arr) > 1 {
		return m.arr[1]
	}
	return ""
}

func (m *Request)Val() string {
	if len(m.arr) > 2 {
		return m.arr[2]
	}
	return ""
}

func (m *Request)Args() []string {
	if len(m.arr) > 0 {
		return m.arr[1 : ]
	}
	return []string{}
}

func (m *Request)Arg(n int) string {
	if len(m.arr) > 1 {
		return m.arr[1 + n]
	}
	return ""
}

func (m *Request)Encode() string {
	var buf bytes.Buffer
	count := len(m.arr)
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(count))
	buf.WriteString("\r\n")
	for _, p := range m.arr {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(p)))
		buf.WriteString("\r\n")
		buf.WriteString(p)
		buf.WriteString("\r\n")
	}
	return buf.String()
}

func (msg *Request)Decode(bs []byte) int {
	total := len(bs)
	if total == 0 {
		return 0
	}

	s := 0
	// skip leading white spaces
	for bs[s] == ' ' || bs[s] == '\t' || bs[s] == '\r' || bs[s] == '\n' {
		s ++
		if s == total {
			return 0
		}
	}

	var parsed int = 0
	msg.arr = make([]string, 0)

	if bs[s] >= '0' && bs[s] <= '9' {
		// ssdb
		parsed = msg.parseSSDBRequest(bs[s:])
	} else if bs[s] == '*' || bs[s] == '$' {
		// redis
		parsed = msg.parseRedisRequest(bs[s:])
	} else {
		parsed = msg.parseSplitRequest(bs[s:])
	}

	if parsed == -1 {
		return -1
	}
	return s + parsed
}

func (msg *Request)parseSSDBRequest(bs []byte) int {
	s := 0
	total := len(bs)

	for {
		idx := bytes.IndexByte(bs[s:], '\n')
		if idx == -1 {
			break
		}

		p := bs[s : s+idx]
		s += idx + 1
		if len(p) > 0 && p[0] == '\r' {
			p = p[0 : len(p)-1]
		}
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			// log.Printf("parse end")
			return s
		}
		// log.Printf("> size [%s]\n", p);

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return -1
		}
		end := s + size

		if end >= total { // not ready
			break
		}
		if bs[end] == '\r' {
			end += 1
			if end >= total { // not ready
				break
			}
		}
		if bs[end] != '\n' {
			return -1
		} else {
			p := string(bs[s : s + size])
			msg.arr = append(msg.arr, p)
			s = end + 1
			// log.Printf("> data %d %d [%s]\n", start, size, p);
		}
	}	
	return 0
}

func (msg *Request)parseRedisRequest(bs []byte) int {
	if len(bs) < 2 {
		return 0
	}

	const BULK  = 0;
	const ARRAY = 1;

	type_ := ARRAY
	bulks := 0

	if (bs[0] == '*') {
		type_  = ARRAY;
		bulks  = 0;
	} else if (bs[0] == '$') {
		type_  = BULK;
		bulks  = 1;
	}

	total := len(bs)

	s := 0
	for s < total {
		if type_ == ARRAY {
			if bs[s] != '*' {
				// log.Println("")
				return -1
			}
		} else if bs[s] != '$' {
			// log.Println("")
			return -1
		}
		s += 1

		idx := bytes.IndexByte(bs[s:], '\n')
		if idx == -1 {
			break
		}
		p := bs[s : s+idx]
		if len(p) > 0 && p[len(p)-1] == '\r' {
			p = p[0 : len(p)-1]
		}
		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			log.Println(err)
			return -1
		}
		s += idx + 1

		if (type_ == ARRAY) {
			bulks  = size
			type_  = BULK
			if bulks == 0 {
				// log.Println("")
				return s
			}
			continue
		}

		end := s + size
		if end >= total { // not ready
			break
		}
		if bs[end] == '\r' {
			end += 1
			if end >= total { // not ready
				break
			}
		}
		if bs[end] != '\n' {
			return -1
		} else {
			p := string(bs[s : s + size])
			msg.arr = append(msg.arr, p)
		}

		s = end + 1
		bulks --
		if bulks == 0 {
			return s
		}
	}

	return 0
}

func (msg *Request)parseSplitRequest(bs []byte) int {
	idx := bytes.IndexByte(bs, '\n')
	if idx == -1 {
		return 0
	}
	size := idx
	if size > 0 && bs[size-1] == '\r' {
		size -= 1
	}
	msg.arr = strings.Split(string(bs[0 : size]), " ")
	return idx + 1
}
