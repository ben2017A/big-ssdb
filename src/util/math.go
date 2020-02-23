package util

import (
	"fmt"
	"strconv"
)

func Atoi(s string) int{
	n, _ := strconv.Atoi(s)
	return n
}

func Atoi32(s string) int32{
	n, _ := strconv.ParseInt(s, 10, 32)
	return int32(n)
}

func Itoa32(u int32) string{
	return fmt.Sprintf("%d", u)
}

func Atoi64(s string) int64{
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func Itoa64(u int64) string{
	return fmt.Sprintf("%d", u)
}

func MinInt(a, b int) int{
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt32(a, b int32) int32{
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a, b int64) int64{
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64{
	if a > b {
		return a
	} else {
		return b
	}
}
