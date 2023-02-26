package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"

type TaskStatus int

const (
	Completed TaskStatus = iota
	NextMap
	DoingMap
	NextReduce
	DoingReduce
)

type Coordinator struct {
	Tasks      []*Task
	ReduceNums int
	mu         sync.Mutex //防止任务被多拿
}

type Task struct {
	Status     TaskStatus //任务状态
	TaskName   string     //任务名称(文件名)
	ReduceNums int        //每个map任务生成reduce任务的数量
}

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	//任务已经全部完成
	completed := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.Tasks {
		if task.Status != Completed {
			completed = false
		}
		if task.Status == NextMap || task.Status == NextReduce {
			reply.T = task
			task.Status++
			return nil
		}
	}
	if completed {
		return errors.New("All tasks have been completed")
	}
	//还有任务未完成，只不过都被拿走
	time.Sleep(100)
	return c.GetTask(args, reply)
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
	tasks := make([]*Task, 0)
	for _, file := range files {
		task := &Task{TaskName: file, Status: NextMap, ReduceNums: nReduce}
		tasks = append(tasks, task)
	}
	c := Coordinator{
		Tasks:      tasks,
		ReduceNums: nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
