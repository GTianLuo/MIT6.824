package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "net/rpc"

type Coordinator struct {
	Tasks []string   //任务列表
	Index int        //执行进度
	mu    sync.Mutex //防止任务被多拿
}

type Task struct {
	TaskName string //任务名称(文件名)
}

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	//任务已经全部完成
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Index >= len(c.Tasks) {
		return errors.New("the task has been completed")
	}
	reply.T = &Task{TaskName: c.Tasks[c.Index]}
	c.Index++
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	fmt.Println("Success regist")
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//fmt.Println(sockname)
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	fmt.Println("Success listen")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	rpc.Accept(l)
	//go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	//	ret := false

	// Your code here.

	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Tasks: files,
	}

	// Your code here.

	c.server()
	return &c
}
