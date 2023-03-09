package raft

import (
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
