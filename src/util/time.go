package util

import (
	"time"
)

// TODO: type Ticker struct

func Sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}
