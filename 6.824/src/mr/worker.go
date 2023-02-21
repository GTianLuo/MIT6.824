package mr

import "fmt"
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
	//获取任务
	w.GetTask()
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
