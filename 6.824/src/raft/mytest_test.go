package raft

import (
	"fmt"
	"math/rand"
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

type s struct {
	x int
	y int
}

func A(x *s) {
	fmt.Println(x)
}

func TestLog(t *testing.T) {
	s := &s{
		x: 1,
		y: 2,
	}
	A(s)
}

func generateTimeOut() time.Duration {
	return time.Duration(800+rand.Intn(150)) * time.Millisecond
}

func TestTicker(t *testing.T) {
	timer := time.NewTimer(10)
	for i := 0; i < 10; i++ {
		timer.Reset(generateTimeOut())
		time.Sleep(900 * time.Millisecond)
		select {
		case <-timer.C:
			fmt.Println("超时")
		default:
			fmt.Println("未超时")
		}
	}
}
