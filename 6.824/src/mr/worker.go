package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
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
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Worker struct {
	Pid        int //完成的工作数
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

func NewWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Worker {
	return &Worker{MapFunc: mapf, ReduceFunc: reducef, Pid: os.Getpid()}
}

func (w *Worker) Work() {
	for {
		//获取任务
		task, err := w.GetTask()
		if err != nil {
			//fmt.Println("worker work:", err.Error())
			return
		} else if task.Status == DoingMap {
			//fmt.Println("获取map任务", "inputfiles:", task.InputFiles)
			if err := w.DoMap(task); err != nil {
				//	fmt.Println(err.Error())
			}
		} else {
			//fmt.Println("获取reduce任务", "inputfiles", task.InputFiles)
			if err := w.DoReduce(task); err != nil {
				//fmt.Println(err.Error())
			}
		}
		if err := w.TaskCompleted(task); err != nil {
			//fmt.Println(err)
			return
		}
	}
}

func (w *Worker) DoMap(task *Task) error {
	//fmt.Println("开始执行Map")
	file, err := os.Open(task.InputFiles[0])
	if err != nil {
		return errors.New("worker DoMap: cannot open " + task.InputFiles[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return errors.New("worker DoMap: cannot read " + task.InputFiles[0])
	}
	_ = file.Close()
	keyValues := w.MapFunc(task.InputFiles[0], string(content))
	hashKV := make(map[int][]KeyValue)
	for _, keyValue := range keyValues {
		rNum := ihash(keyValue.Key) % task.ReduceNums
		hashKV[rNum] = append(hashKV[rNum], keyValue)
	}
	//将键值对写入中间文件
	oNamePrefix := "mr-map-"
	for i := 0; i < task.ReduceNums; i++ {
		oName := oNamePrefix + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
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
	//fmt.Println(task.InputFiles, "map任务完成")
	return nil
}

func (w *Worker) DoReduce(task *Task) error {

	intermediate, err := w.uploadFile(task)
	//fmt.Println(intermediate)
	//os.Exit(1)
	if err != nil {
		return err
	}
	//排序,进行reduce,并将结果写入文件
	sort.Sort(ByKey(intermediate))
	oName := "mr-tmp-" + strconv.Itoa(task.TaskId)
	ofile, err := os.Create(oName)
	if err != nil {
		return errors.New("reduce worker:" + err.Error())
	}
	//fmt.Println(intermediate)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.ReduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//fmt.Println(intermediate[i].Key, output)
		if _, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output); err != nil {
			return errors.New("reduce worker:" + err.Error())
		}
		i = j
	}
	hashNum := string(task.InputFiles[0][len(task.InputFiles[0])-1])
	ofile.Close()
	return os.Rename(ofile.Name(), "mr-out-"+hashNum)
}

func (w *Worker) uploadFile(t *Task) ([]KeyValue, error) {
	//读取文件并以keyValue形式保存在缓存中
	intermediate := make([]KeyValue, 0)
	for _, file := range t.InputFiles {
		ofile, _ := os.Open(file)
		content, err := ioutil.ReadAll(ofile)
		if err != nil {
			return intermediate, errors.New("reduce worker:" + err.Error())
		}
		ff := func(r rune) bool { return unicode.IsSpace(r) }
		keyValueArr := strings.FieldsFunc(string(content), ff)
		for i := 0; i < len(keyValueArr); i = i + 2 {
			intermediate = append(intermediate, KeyValue{Key: keyValueArr[i], Value: keyValueArr[i+1]})
		}
		ofile.Close()
	}
	return intermediate, nil
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
	//fmt.Println("RPC 远程连接成功")
	err = c.Call(rpcname, args, reply)
	return err
}

//GetTask worker 获取任务
func (w *Worker) GetTask() (*Task, error) {
	args := &TaskRequest{Pid: w.Pid}
	reply := &TaskResponse{}
	err := call("Coordinator.GetTask", args, reply)
	if err == nil {
		//fmt.Printf("worker:success get task:%s\n", reply.T.TaskName)
		return reply.T, nil
	} else {
		return nil, err
	}
}

func (w *Worker) TaskCompleted(t *Task) error {
	args := &CompletedRequest{Pid: w.Pid, T: t}
	reply := &CompletedResponse{}
	if err := call("Coordinator.TaskCompleted", args, reply); err != nil {
		return err
	}
	return nil
}
