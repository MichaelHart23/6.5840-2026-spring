package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState int
// 用于标识一个任务是未分配，还是执行中，还是执行完了
const (
	taskStateUnassigned taskState = iota
	taskStateExecuting
	taskStateDone
)

type phase int

const (
	phaseMap phase = iota
	phaseReduce
	phaseFinished
)

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	// taskID就是这个数组的下标，这个数组用来标记所有任务(包括map和reduce)执行状态
	// 前面的部分是map task，后面的nReduce个是reduce task
	taskStates []taskState
	state phase  // map reduce
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == phaseMap {
		fileNum := len(c.files)
		allMapDone := true
		for i := 0; i < fileNum; i++ {
			if c.taskStates[i] != taskStateDone {
				allMapDone = false
			}
			if (c.taskStates[i] == taskStateUnassigned) {
				reply.FileName = c.files[i]
				reply.TaskID = i
				reply.NReduce = c.nReduce
				reply.TaskType = MapTask
				c.taskStates[i] = taskStateExecuting
				go c.timer(i)
				return nil
			}
		}
		if allMapDone {
			c.state = phaseReduce
		}
	}

	if c.state == phaseReduce {
		n := len(c.files) + c.nReduce
		allReduceDone := true
		for i := len(c.files); i < n; i++ {
			if c.taskStates[i] != taskStateDone {
				allReduceDone = false
			}
			if (c.taskStates[i] == taskStateUnassigned) {
				reply.TaskType = ReduceTask
				reply.TaskID = i
				// key的哈希值
				reply.KeyHash = i - len(c.files)
				c.taskStates[i] = taskStateExecuting
				go c.timer(i)
				return nil
			}
		}
		if allReduceDone {
			c.state = phaseFinished
		}
	}

	if c.state == phaseFinished {
		reply.TaskType = DoneTask
		return nil
	}

	reply.TaskType = WaitTask
	return nil
}

// 每个任务的计时线程，若超过10s没执行完，将把这个任务状态该为未分配，之后会交给另一个worker执行
func (c *Coordinator) timer(taskID int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskStates[taskID] == taskStateExecuting {
		c.taskStates[taskID] = taskStateUnassigned
	}
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	id := args.TaskID
	c.mu.Lock()
	defer c.mu.Unlock()
	// 按理说应该校验task type和当前map或reduce阶段是不是一样，以及id是否合法
	if (c.taskStates[id] == taskStateExecuting) {
		c.taskStates[id] = taskStateDone
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c) // 注册该Coordinator的所有“成员函数”为RPC调用
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	// 在这里已经启动了一个处理RPC的goroutine了
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == phaseFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// nReduce指的是：要把map生成的所有中间结果分成几组，即生成几个中间文件，即要有多少次reduce任务
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files;
	c.nReduce = nReduce
	n := len(files) + nReduce
	for i := 0; i < n; i++ {
		c.taskStates = append(c.taskStates, taskStateUnassigned)
	}
	c.state = phaseMap;

	c.server(sockname)
	return &c
}
