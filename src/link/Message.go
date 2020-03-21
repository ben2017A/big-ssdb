package link

type Message struct {
	Src int
	Data string
	ps []string
}

func NewMessage(ps []string) *Message {
	ret := new(Message)
	ret.ps = ps
	return ret
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
