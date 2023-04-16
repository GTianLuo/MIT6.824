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
	X int
	Y int
}

type K struct {
	X string
	Y []int
	Z bool
}

func A(x *S) {
	fmt.Println(x)
}

func TestLog(t *testing.T) {
	s := &S{
		X: 1,
		Y: 2,
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
	s := &S{X: 1, Y: 2}
	k := &K{X: "aaaaaa", Y: []int{1, 2, 3}}
	data := bytes.NewBuffer([]byte{})
	encoder := labgob.NewEncoder(data)
	if err := encoder.Encode(s); err != nil {
		fmt.Println(err)
	}
	if err := encoder.Encode(k); err != nil {
		fmt.Println(err)
	}

	decoder := labgob.NewDecoder(data)

	var k2 K
	var s2 S
	if err := decoder.Decode(&s2); err != nil {
		fmt.Println(err)
	}
	if err := decoder.Decode(&k2); err != nil {
		fmt.Println(err)
	}
	fmt.Println(k2)
	fmt.Println(s2)
}
func add() {
	i := 1
	go func() {
		for {
			fmt.Println(i)
		}
	}()
}
