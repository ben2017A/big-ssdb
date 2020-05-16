package util

import "time"
// import log "glog"

func StartThread(done_c chan bool, run_f func()) {
	start_c := make(chan bool)

	go func(){
		start_c <- true
		run_f()
		done_c <- true
	}()

	<- start_c
	close(start_c)
}

func StartSignalConsumerThread(done_c chan bool, sig_c chan bool, f func()) {
	StartThread(done_c, func(){
		for {
			t := <- sig_c
			if t == false {
				return
			}
			f()
		}
	})
}

// 不传入, 而是返回 stop_c
func StartTickerConsumerThread(stop_c chan bool, done_c chan bool, tickMs int, f func(tickMs int)) {
	StartThread(done_c, func(){
		ticker := time.NewTicker(time.Duration(tickMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select{
			case <- stop_c:
				return
			case <- ticker.C:
				f(tickMs)
			}
		}
	})
}
