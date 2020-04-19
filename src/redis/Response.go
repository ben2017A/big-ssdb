package redis

const (
	TypeNull = iota
	TypeError
	TypeStatus
	TypeInt
	TypeBulk
	TypeArray
)

type Response struct {

}

func (r *Response)ReplyNull() {

}

func (r *Response)ReplyError(msg string) {

}

func (r *Response)ReplyStatus(code string, msg string) {

}

func (r *Response)ReplyInt(num int64) {

}

func (r *Response)ReplyBulk(b string) {

}

func (r *Response)ReplyArray(ps []string) {

}

func (r *Response)Encode() string {
	return ""
}
