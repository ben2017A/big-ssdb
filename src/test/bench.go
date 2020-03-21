package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	conn, err := net.Dial("tcp", "127.0.0.1:9001")
	if err != nil {
		log.Println(err)
		return
	}
	reader := bufio.NewReader(conn)

	log.Println("start")

	num := 1000
	start := time.Now()
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("k%04d", i)
		fmt.Fprintf(conn, "set %s %d\n", key, i)
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
			return
		}

		_ = line
		// fmt.Printf("%s", line)
	}
	elapsed := time.Now().Sub(start)
	qps := int(float64(num) / elapsed.Seconds())
	log.Printf("time: %f, num: %d, qps: %d", elapsed.Seconds(), num, qps)

	log.Println("end")
}
