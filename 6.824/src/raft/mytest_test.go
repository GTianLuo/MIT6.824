package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type status int32

func TestAtomic(t *testing.T) {
	var s status = 1
	var v atomic.Value
	v.Store(&s)
	fmt.Println(*v.Load().(*status))
}

func TestTime(t *testing.T) {
	lastAppend := time.Now()
	for true {
		var timeout int64 = 500
		interval := time.Now().UnixNano()/1e6 - lastAppend.UnixNano()/1e6
		fmt.Println(interval)
		if interval < timeout {
			time.Sleep(time.Duration(timeout-interval) * time.Millisecond)
		} else {
			return
		}
		//fmt.Println(now.UnixNano()/1e6 - time.Now().UnixNano()/1e6)
	}
}

type S struct {
	x int
	y int
}

type K struct {
	x string
	y []int
}

func A(x *S) {
	fmt.Println(x)
}

func TestLog(t *testing.T) {
	s := &S{
		x: 1,
		y: 2,
	}
	A(s)
}

func TestTicker(t *testing.T) {
	timer := time.NewTimer(10)
	for i := 0; i < 10; i++ {
		//timer.Reset(generateTimeOut())
		//time.Sleep(900 * time.Millisecond)
		<-timer.C
		fmt.Println("超时")
	}
}

func TestGob(t *testing.T) {
	s := S{x: 1, y: 2}
	data := bytes.NewBuffer([]byte{})
	encoder := labgob.NewEncoder(data)
	encoder.Encode(s)
}
