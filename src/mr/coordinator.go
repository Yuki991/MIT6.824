package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MaxTaskRunningTime = time.Second * 10 // TODO
	ScheduleInterval   = time.Millisecond * 500
)

type TaskStatus int

const (
	TaskStatusReady   TaskStatus = 0
	TaskStatusQueue   TaskStatus = 1
	TaskStatusRunning TaskStatus = 2
	TaskStatusFinish  TaskStatus = 3
	TaskStatusError   TaskStatus = 4
)

type TaskInfo struct {
	Status    TaskStatus
	WorkerId  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	done      bool
	files     []string
	nReduce   int
	taskPhase TaskPhase
	taskInfos []TaskInfo
	workerSeq int // 维护当前worker的编号，每有一个新的worker register就+1
	taskCh    chan Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果有重复请求（因网络延迟等原因），会导致Seq重复+1，不过没有关系，能保证一个ID唯一标识一个worker就ok
	c.workerSeq += 1
	reply.WorkerId = c.workerSeq
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <-c.taskCh
	Dprintf("coordinator allocate task %v to worker %v", task.Seq, args.WorkerId)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.taskInfos[task.Seq] = TaskInfo{
		Status:    TaskStatusRunning,
		WorkerId:  args.WorkerId,
		StartTime: time.Now(),
	}

	reply.Task = &task
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskPhase != c.taskPhase ||
		args.WorkerId != c.taskInfos[args.Seq].WorkerId ||
		c.taskInfos[args.Seq].Status != TaskStatusRunning {
		// 过时rpc
		return nil
	}

	if args.Done {
		c.taskInfos[args.Seq].Status = TaskStatusFinish
	} else {
		c.taskInfos[args.Seq].Status = TaskStatusError
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) createTask(idx int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMaps:    len(c.files),
		Seq:      idx,
		Phase:    c.taskPhase,
	}
	if c.taskPhase == MapPhase {
		task.FileName = c.files[idx]
	}
	return task
}

func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()

	finishCnt := 0
	for i, info := range c.taskInfos {
		switch info.Status {
		case TaskStatusReady, TaskStatusError:
			task := c.createTask(i)
			c.taskInfos[i].Status = TaskStatusQueue
			c.taskCh <- task
		case TaskStatusQueue:
		case TaskStatusRunning:
			// Dprintf("task %v run %v seconds.", i, time.Since(info.StartTime).Seconds())
			if time.Since(info.StartTime) > MaxTaskRunningTime {
				task := c.createTask(i)
				c.taskInfos[i].Status = TaskStatusQueue
				c.taskCh <- task
			}
		case TaskStatusFinish:
			finishCnt++
		}
	}

	if finishCnt == len(c.taskInfos) {
		switch c.taskPhase {
		case MapPhase:
			c.initReduceTask()
		case ReducePhase:
			c.done = true
		}
	}
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskInfos = make([]TaskInfo, len(c.files))
	for i := range c.taskInfos {
		c.taskInfos[i].Status = TaskStatusReady
	}
}

func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.taskInfos = make([]TaskInfo, c.nReduce)
	for i := range c.taskInfos {
		c.taskInfos[i].Status = TaskStatusReady
	}
}

func (c *Coordinator) ticker() {
	cnt := 0
	for !c.done {
		c.schedule()
		time.Sleep(ScheduleInterval)
		cnt++

		if cnt%10 == 0 {
			c.mu.Lock()

			str := ""
			if c.taskPhase == MapPhase {
				str += "Phase: Map, "
			} else {
				str += "Phase: Reduce, "
			}
			str += fmt.Sprintf("[")
			for _, v := range c.taskInfos {
				switch v.Status {
				case TaskStatusReady:
					str += fmt.Sprintf("Ready, ")
				case TaskStatusQueue:
					str += fmt.Sprint("Queue, ")
				case TaskStatusRunning:
					str += fmt.Sprint("Running, ")
				case TaskStatusFinish:
					str += fmt.Sprint("Finish, ")
				case TaskStatusError:
					str += fmt.Sprint("Error, ")
				}
			}
			str += " ]"
			Dprintf("%v", str)

			c.mu.Unlock()
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	Dprintf("files: %v", files)

	// Your code here.
	c.done = false
	c.files = files
	c.nReduce = nReduce
	c.workerSeq = 0
	if len(files) > nReduce {
		c.taskCh = make(chan Task, len(files))
	} else {
		c.taskCh = make(chan Task, nReduce)
	}
	c.initMapTask()
	go c.ticker()

	c.server()
	return &c
}
