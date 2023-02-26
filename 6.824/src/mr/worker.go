package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	WorkCompleted int //完成的工作数
	MapFunc       func(string, string) []KeyValue
	ReduceFunc    func(string, []string) string
}

func NewWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Worker {
	return &Worker{MapFunc: mapf, ReduceFunc: reducef}
}

func (w *Worker) Work() {
	for {
		//获取任务
		task := w.GetTask()
		//已经没有任务了
		if task == nil {
			fmt.Printf("work:failed to get task\n")
			return
		}
		fmt.Println(task.Status)
		switch task.Status {
		case DoingMap:
			if err := w.DoMap(task); err != nil {
				fmt.Println(err)
				return
			}
		case NextReduce:
		}
	}
}

func (w *Worker) DoMap(task *Task) error {
	fmt.Println("开始执行Map")
	file, err := os.Open(task.TaskName)
	if err != nil {
		return errors.New("worker DoMap: cannot open " + task.TaskName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return errors.New("worker DoMap: cannot read " + task.TaskName)
	}
	_ = file.Close()
	keyValues := w.MapFunc(task.TaskName, string(content))
	hashKV := make(map[int][]KeyValue)
	for _, keyValue := range keyValues {
		rNum := ihash(keyValue.Key) % task.ReduceNums
		hashKV[rNum] = append(hashKV[rNum], keyValue)
	}
	//将键值对写入中间文件
	oNamePrefix := "mr-tmp-"
	for i := 0; i < task.ReduceNums; i++ {
		oName := oNamePrefix + task.TaskName + strconv.Itoa(i)
		file, err := os.Create(oName)
		if err != nil {
			return errors.New("work doMap:" + err.Error())
		}
		for _, keyValue := range hashKV[i] {
			if _, err := fmt.Fprintf(file, "%v %v\n", keyValue.Key, keyValue.Value); err != nil {
				return errors.New("work doMap:" + err.Error())
			}
		}
		if err = file.Close(); err != nil {
			return errors.New("work doMap:" + err.Error())
		}
	}
	return nil
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//GetTask worker 获取任务
func (w *Worker) GetTask() *Task {
	args := &TaskRequest{X: w.WorkCompleted}
	reply := &TaskResponse{}
	err := call("Coordinator.GetTask", args, reply)
	if err == nil {
		fmt.Printf("worker:success get task:%s\n", reply.T.TaskName)
		return reply.T
	} else {
		fmt.Println("work:", err)
		return nil
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.Dial("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//fmt.Println(sockname)
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	fmt.Println("RPC 远程连接成功")
	err = c.Call(rpcname, args, reply)
	return err
}
