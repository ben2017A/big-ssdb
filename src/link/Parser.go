package link

import (
	// "log"
	"bytes"
	"errors"
	"strings"
	"strconv"
)

// 同时支持 SSDB, Redis, 空格分隔 3 种报文格式
type Parser struct {
	buf bytes.Buffer
}

func (p *Parser)Append(bs []byte) {
	p.buf.Write(bs)
}

func (p *Parser)AppendString(s string) {
	p.buf.WriteString(s)
}

func (p *Parser)Parse() (*Message, error) {
	if p.buf.Len() == 0 {
		return nil, nil
	}

	bs := p.buf.Bytes()
	len := p.buf.Len()

	s := 0
	// skip leading spaces
	for bs[s] == ' ' || bs[s] == '\t' || bs[s] == '\r' || bs[s] == '\n' {
		s ++
		if s == len {
			return nil, nil
		}
	}

	var parsed int = 0
	var msg *Message = nil

	if bs[s] >= '0' && bs[s] <= '9' {
		// ssdb
		msg, parsed = p.parseSSDBMessage(bs[s:])
	} else if bs[s] == '*' || bs[s] == '$' {
		// redis
		msg, parsed = p.parseRedisMessage(bs[s:])
	} else {
		msg, parsed = p.parseSplitMessage(bs[s:])
	}

	if parsed == -1 {
		return nil, errors.New("parse error")
	}
	if msg != nil {
		p.buf.Next(s + parsed)
	}

	return msg, nil
}

func (p *Parser)parseSSDBMessage(bs []byte) (*Message, int) {
	ps := make([]string, 0)
	s := 0
	total := len(bs)

	for {
		idx := bytes.IndexByte(bs[s:], '\n')
		if idx == -1 {
			break
		}

		p := string(bs[s : s+idx])
		s += idx + 1
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			// log.Printf("parse end")
			msg := NewMessage(ps)
			return msg, s
		}
		// log.Printf("> size [%s]\n", p);

		size, err := strconv.Atoi(p)
		if err != nil || size < 0 {
			return nil, -1
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
			return nil, -1
		} else {
			p := string(bs[s : s + size])
			ps = append(ps, p)
			s = end + 1
			// log.Printf("> data %d %d [%s]\n", start, size, p);
		}
	}	
	return nil, 0
}

func (p *Parser)parseRedisMessage(bs []byte) (*Message, int) {
	if len(bs) < 2 {
		return nil, 0
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

	ps := make([]string, 0)
	total := len(bs)

	s := 0
	for s < total {
		if type_ == ARRAY {
			if bs[s] != '*' {
				// log.Println("")
				return nil, -1
			}
		} else if bs[s] != '$' {
			// log.Println("")
			return nil, -1
		}
		s += 1

		idx := bytes.IndexByte(bs[s:], '\n')
		if idx == -1 {
			break
		}
		p := string(bs[s : s+idx])
		size, err := strconv.Atoi(p)
		if err != nil || size < 0 {
			// log.Println("")
			return nil, -1
		}
		s += idx + 1

		if (type_ == ARRAY) {
			bulks  = size
			type_  = BULK
			if bulks == 0 {
				// log.Println("")
				return nil, -1
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
			// log.Println("")
			return nil, -1
		} else {
			p := string(bs[s : s + size])
			ps = append(ps, p)
		}

		s = end + 1
		bulks --
		if bulks == 0 {
			msg := NewMessage(ps)
			return msg, s
		}
	}

	return nil, 0
}

func (p *Parser)parseSplitMessage(bs []byte) (*Message, int) {
	idx := bytes.IndexByte(bs, '\n')
	if idx == -1 {
		return nil, 0
	}
	size := idx
	if size > 0 && bs[size-1] == '\r' {
		size -= 1
	}
	ps := strings.Split(string(bs[0 : size]), " ")
	msg := NewMessage(ps)
	return msg, idx + 1
}
