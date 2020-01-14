package myutil

import (
	"os"
)

func IsDir(path string) bool{
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func FileExists(path string) bool{
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}
