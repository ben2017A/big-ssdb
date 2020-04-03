package link

import (
	"log"
	"bytes"
	"strings"
	"strconv"
)

type Message struct {
	Src int
	ps []string
}

func NewMessage(ps []string) *Message {
	ret := new(Message)
	ret.ps = ps
	return ret
}

func NewResponse(src int, ps []string) *Message {
	ret := new(Message)
	ret.Src = src
	ret.ps = ps
	return ret
}

func NewErrorResponse(src int, desc string) *Message {
	ret := new(Message)
	ret.Src = src
	ret.ps = []string{"error", desc}
	return ret
}

func (m *Message)Array() []string {
	return m.ps
}

func (m *Message)Cmd() string {
	if len(m.ps) > 0 {
		return m.ps[0]
	}
	return ""
}

func (m *Message)Code() string {
	return m.Cmd()
}

func (m *Message)Args() []string {
	if len(m.ps) > 0 {
		return m.ps[1 : ]
	}
	return make([]string, 0)
}

func (m *Message)Encode() string {
	var buf bytes.Buffer
	count := len(m.ps)
	if count > 1 {
		buf.WriteString("*")
		buf.WriteString(strconv.Itoa(count))
		buf.WriteString("\r\n")
	}
	for _, p := range m.ps {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(p)))
		buf.WriteString("\r\n")
		buf.WriteString(p)
		buf.WriteString("\r\n")
	}
	return buf.String()
}

func (msg *Message)Decode(bs []byte) int {
	total := len(bs)
	if total == 0 {
		return 0
	}

	s := 0
	// skip leading spaces
	for bs[s] == ' ' || bs[s] == '\t' || bs[s] == '\r' || bs[s] == '\n' {
		s ++
		if s == total {
			return 0
		}
	}

	var parsed int = 0
	msg.ps = make([]string, 0)

	if bs[s] >= '0' && bs[s] <= '9' {
		// ssdb
		parsed = msg.parseSSDBMessage(bs[s:])
	} else if bs[s] == '*' || bs[s] == '$' {
		// redis
		parsed = msg.parseRedisMessage(bs[s:])
	} else {
		parsed = msg.parseSplitMessage(bs[s:])
	}

	if parsed == -1 {
		return -1
	}
	return s + parsed
}

func (msg *Message)parseSSDBMessage(bs []byte) int {
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
			msg.ps = append(msg.ps, p)
			s = end + 1
			// log.Printf("> data %d %d [%s]\n", start, size, p);
		}
	}	
	return 0
}

func (msg *Message)parseRedisMessage(bs []byte) int {
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
			msg.ps = append(msg.ps, p)
		}

		s = end + 1
		bulks --
		if bulks == 0 {
			return s
		}
	}

	return 0
}

func (msg *Message)parseSplitMessage(bs []byte) int {
	idx := bytes.IndexByte(bs, '\n')
	if idx == -1 {
		return 0
	}
	size := idx
	if size > 0 && bs[size-1] == '\r' {
		size -= 1
	}
	msg.ps = strings.Split(string(bs[0 : size]), " ")
	return idx + 1
}
