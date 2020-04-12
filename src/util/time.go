package util

import (
	"time"
)

func Sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}
