package util

import (
	"strings"
)

func ReplaceBytes(s string, src []string, dst []string) string {
	for i, _ := range src {
		s = strings.Replace(s, src[i], dst[i], -1)
	}
	return s
}