package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"

type TaskStatus int

type CoordinatorStatus int

const (
	Completed TaskStatus = iota
	NextMap
	DoingMap
	NextReduce
	DoingReduce
)

const (
	MapStage CoordinatorStatus = iota
	ReduceStage
	AllTasksCompleted
)

//1.全部map执行完，添加reduce任务
//2.reduce挂掉，添加新的reduce

type Coordinator struct {
	UniqueTaskId         int
	Status               CoordinatorStatus
	Tasks                map[int]*Task
	DoingTasks           map[int]*TaskDup //正在执行的任务，只存储任务编号和状态
	ReduceTaskInputFiles map[int][]string
	ReduceNums           int
	taskMu               sync.Mutex //防止任务被多拿
	doingTaskMu          sync.Mutex
}

type Task struct {
	TaskId     int        // 任务Id
	Status     TaskStatus //任务状态
	InputFiles []string   //任务名称(文件名)
	ReduceNums int        //每个map任务生成reduce任务的数量
}

//任务副本，防止在检查任务是否崩溃时，任务被同时读写
type TaskDup struct {
	Status     TaskStatus
	StartTime  time.Time //任务开始时间
	InputFiles string    //任务名称(文件名)
}

//var count = 0

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {

	//	fmt.Println(args.Pid, "获取任务")
	c.taskMu.Lock()
	c.parseCoordinatorStatus()
	/*
		for _, task := range c.Tasks {
			fmt.Println(task.TaskId, " ", task.InputFiles)
		}
		fmt.Println()
		fmt.Println()*/
	switch c.getStatus() {
	case AllTasksCompleted:
		//删除所有临时文件
		os.Remove("mr-tmp-*")
		c.taskMu.Unlock()
		return errors.New("all task has completed")
	case MapStage:
		task, err := c.getMapTask()
		if err != nil {
			//没有空闲任务
			c.taskMu.Unlock()
			time.Sleep(500 * time.Millisecond)
			return c.GetTask(args, reply)
		} else {
			reply.T = task
			//	fmt.Println(args.Pid, "获取Map任务", task.TaskId)
		}
		c.doingTaskMu.Lock()
		c.DoingTasks[task.TaskId] = &TaskDup{
			Status:     task.Status,
			StartTime:  time.Now(),
			InputFiles: task.InputFiles[0],
		}
		c.doingTaskMu.Unlock()
		c.taskMu.Unlock()
		return nil
	case ReduceStage:
		task, err := c.getReduceTask()
		if err != nil {
			//没有空闲任务
			c.taskMu.Unlock()
			time.Sleep(500 * time.Millisecond)
			return c.GetTask(args, reply)
		} else {
			//	fmt.Println(args.Pid, "获取Reduce任务")
			reply.T = task
		}
		c.doingTaskMu.Lock()
		c.DoingTasks[task.TaskId] = &TaskDup{
			Status:    task.Status,
			StartTime: time.Now(),
		}
		c.doingTaskMu.Unlock()
		c.taskMu.Unlock()
		return nil
	}
	panic("coordinator:invaild coordinator status")
}

func (c *Coordinator) getStatus() CoordinatorStatus {
	return c.Status
}
func (c *Coordinator) TaskCompleted(args *CompletedRequest, reply *CompletedResponse) error {

	task := args.T
	//检查该任务是否存在(可能是因为超时，而被取消的任务)
	c.taskMu.Lock()
	if _, ok := c.Tasks[task.TaskId]; !ok {
		c.taskMu.Unlock()
		fmt.Println("被取消的任务完成")
		return errors.New("The task has been canceled")
	}

	//删除原先任务
	c.doingTaskMu.Lock()
	c.deleteTask(task.TaskId)
	c.taskMu.Unlock()
	c.doingTaskMu.Unlock()
	if task.Status == DoingMap {
		//fmt.Println(args.Pid, "完成Map任务")
	} else if task.Status == DoingReduce {
		//fmt.Println(args.Pid, "完成Reduce任务")
		//删除中间文件
		for _, file := range task.InputFiles {
			os.Remove(file)
		}
	}
	return nil
}

func (c *Coordinator) getTaskId() (rnt int) {
	rnt = c.UniqueTaskId
	c.UniqueTaskId++
	return
}

func (c *Coordinator) getMapTask() (*Task, error) {
	if c.Status != MapStage {
		panic("coordinator:error status")
	}
	for _, task := range c.Tasks {
		if task.Status == NextMap {
			task.Status = DoingMap
			return task, nil
		}
	}
	return nil, errors.New("coordinator:there are no idle map tasks")
}

func (c *Coordinator) addMapTask(inputFiles ...string) int {
	task := &Task{Status: NextMap, ReduceNums: c.ReduceNums}
	task.TaskId = c.getTaskId()
	task.InputFiles = append(task.InputFiles, inputFiles...)
	c.Tasks[task.TaskId] = task
	//	fmt.Println("添加任务", task.TaskId)
	return task.TaskId
}

func (c *Coordinator) getReduceTask() (*Task, error) {
	if c.Status != ReduceStage {
		panic("coordinator:error status")
	}
	for _, task := range c.Tasks {
		if task.Status == NextReduce {
			task.Status = DoingReduce
			return task, nil
		}
	}
	return nil, errors.New("coordinator:there are no idle reduce tasks")
}

func (c *Coordinator) addReduceTask(inputFiles ...string) int {
	task := &Task{Status: NextReduce, ReduceNums: c.ReduceNums}
	task.TaskId = c.getTaskId()
	task.InputFiles = append(task.InputFiles, inputFiles...)
	c.Tasks[task.TaskId] = task
	return task.TaskId
}

func (c *Coordinator) parseCoordinatorStatus() {
	if len(c.Tasks) == 0 && c.Status != MapStage {
		c.Status = AllTasksCompleted
		return
	}
	for _, task := range c.Tasks {
		if task.Status == NextMap || task.Status == DoingMap {
			c.Status = MapStage
			return
		}
	}
	//所有Map任务都已执行结束
	if c.Status == MapStage {
		//全部map任务结束，需要添加reduce任务
		tmpFilePath, _ := os.Getwd()
		files, _ := ioutil.ReadDir(tmpFilePath)
		for i := 0; i < c.ReduceNums; i++ {
			tmpFiles := make([]string, 0)
			for _, file := range files {
				if strings.HasPrefix(file.Name(), "mr-map-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
					tmpFiles = append(tmpFiles, file.Name())
				}
			}
			taskId := c.addReduceTask(tmpFiles...)
			c.ReduceTaskInputFiles[taskId] = tmpFiles
		}
	}
	c.Status = ReduceStage
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	//fmt.Println("Success regist")
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//fmt.Println(sockname)
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	//fmt.Println("Success listen")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go rpc.Accept(l)
	//go http.Serve(l, nil)
}

func (c *Coordinator) crashCheck() {
	for {
		time.Sleep(time.Second * 4)
		c.doingTaskMu.Lock()
		for taskId, task := range c.DoingTasks {
			//fmt.Println("crash check,", taskId)
			//fmt.Println("len(c.DoingTasks)", len(c.DoingTasks))
			if (time.Since(task.StartTime).Seconds()) < 10 {
				//fmt.Println(task.InputFiles, "开始了", time.Now().Second()-task.StartTime.Second(), "秒，任务状态：", task.Status)
				continue
			}
			//任务已经超时
			c.taskMu.Lock()
			//检查DoingMap中是否还有这个Task，防止在抢夺锁过程中，任务完成
			if _, ok := c.DoingTasks[taskId]; !ok {
				c.taskMu.Unlock()
				continue
			}
			//删除原先任务
			c.deleteTask(taskId)
			//	fmt.Println("len(c.DoingTasks)", len(c.DoingTasks))

			if task.Status == DoingMap {
				c.addMapTask(task.InputFiles)
				//	fmt.Println("任务", taskId, "超时已重新分配：", task.InputFiles)
				//	fmt.Println(c.ReduceTaskInputFiles)
				c.taskMu.Unlock()
				break
			} else if task.Status == DoingReduce {
				newTaskId := c.addReduceTask(c.ReduceTaskInputFiles[taskId]...)
				//更新InputFiles与TaskId的映射
				c.ReduceTaskInputFiles[newTaskId] = c.ReduceTaskInputFiles[taskId]
				delete(c.ReduceTaskInputFiles, taskId)
				//	fmt.Println("任务", taskId, "超时已重新分配：", c.ReduceTaskInputFiles[newTaskId])
				//	fmt.Println(c.ReduceTaskInputFiles)
				c.taskMu.Unlock()
				break
			}
			c.taskMu.Unlock()
		}
		c.doingTaskMu.Unlock()
	}
}

func (c *Coordinator) Done() bool {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	if c.Status == AllTasksCompleted {
		return true
	}
	return false
}

func (c *Coordinator) deleteTask(taskId int) {
	delete(c.Tasks, taskId)
	delete(c.DoingTasks, taskId)
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make(map[int]*Task, 0)
	c := &Coordinator{
		Tasks:                tasks,
		Status:               MapStage,
		ReduceNums:           nReduce,
		UniqueTaskId:         nReduce, //nReduce之前的Id留给RueduceTask，这样ReduceTask Id就是已知的
		DoingTasks:           make(map[int]*TaskDup),
		ReduceTaskInputFiles: make(map[int][]string),
	}
	for _, file := range files {
		c.addMapTask(file)
	}
	go c.crashCheck()
	c.server()
	return c
}
