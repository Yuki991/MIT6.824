package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

const (
	RPCTimeout = time.Millisecond * 500 // TODO
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func (w *worker) register() {
	args := RegisterWorkerArgs{}

	for {
		reply := RegisterWorkerReply{}
		ok := CallFunc(RPCTimeout, call, "Coordinator.RegisterWorker", &args, &reply)
		if ok {
			w.workerId = reply.WorkerId
			Dprintf("register success, id: %v", w.workerId)
			break
		}
	}
}

func (w *worker) run() {
	// 这里没有写终止条件，一直取task，完成task
	for {
		task := w.requestTask()
		Dprintf("worker %v get task: %v", w.workerId, task)
		w.doTask(task)
	}
}

func (w *worker) requestTask() *Task {
	args := RequestTaskArgs{WorkerId: w.workerId}

	for {
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			return reply.Task
		}
	}
}

func (w *worker) doTask(task *Task) {
	switch task.Phase {
	case MapPhase:
		w.doMapTask(task)
	case ReducePhase:
		w.doReduceTask(task)
	}
}

func (w *worker) doMapTask(task *Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		Dprintf("cannot open %v", task.FileName)
		w.reportTask(task, false)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		Dprintf("cannot read %v", task.FileName)
		w.reportTask(task, false)
		file.Close()
		return
	}
	file.Close()

	kva := w.mapf(task.FileName, string(content))
	reduce := make([][]KeyValue, task.NReduce)
	for _, v := range kva {
		idx := ihash(v.Key) % task.NReduce
		reduce[idx] = append(reduce[idx], v)
	}

	dir, _ := os.Getwd()
	for i, r := range reduce {
		// 生成临时文件
		file, err := ioutil.TempFile(dir, "mr-reduce-tmp-*")
		if err != nil {
			Dprintf("cannot create temp file: %v", err)
			w.reportTask(task, false)
			return
		}
		defer file.Close()

		// 写入临时文件
		enc := json.NewEncoder(file)
		if err := enc.Encode(r); err != nil {
			Dprintf("encode error")
			w.reportTask(task, false)
			return
		}

		// rename
		fileName := reduceName(task.Seq, i)
		if err := os.Rename(file.Name(), fileName); err != nil {
			Dprintf("rename error")
			w.reportTask(task, false)
			return
		}
	}
	w.reportTask(task, true)
}

func (w *worker) doReduceTask(task *Task) {
	mp := make(map[string][]string)
	for i := 0; i < task.NMaps; i++ {
		fileName := reduceName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			Dprintf("cannot open %v", fileName)
			w.reportTask(task, false)
			return
		}

		var vec []KeyValue
		dec := json.NewDecoder(file)
		if err := dec.Decode(&vec); err != nil {
			Dprintf("decode error")
			w.reportTask(task, false)
			return
		}

		for _, kv := range vec {
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}

	// reduce
	result := make([]string, 0)
	for k, v := range mp {
		r := w.reducef(k, v)
		result = append(result, fmt.Sprintf("%v %v\n", k, r))
	}

	// 生成临时文件
	dir, _ := os.Getwd()
	file, err := ioutil.TempFile(dir, "mr-out-tmp-*")
	if err != nil {
		Dprintf("cannot create temp file: %v", err)
		w.reportTask(task, false)
		return
	}
	defer file.Close()

	// 写入临时文件
	if err := ioutil.WriteFile(file.Name(), []byte(strings.Join(result, "")), 0600); err != nil {
		w.reportTask(task, false)
	}

	// rename
	fileName := mergeName(task.Seq)
	if err := os.Rename(file.Name(), fileName); err != nil {
		Dprintf("rename error")
		w.reportTask(task, false)
		return
	}

	w.reportTask(task, true)
}

func (w *worker) reportTask(task *Task, done bool) {
	args := ReportTaskArgs{
		Done:      done,
		Seq:       task.Seq,
		WorkerId:  w.workerId,
		TaskPhase: task.Phase,
	}
	reply := ReportTaskReply{}

	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		Dprintf("report task fail")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
