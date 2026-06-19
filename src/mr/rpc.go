package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
    MapTask    TaskType = iota  // 执行 Map
    ReduceTask                  // 执行 Reduce
    WaitTask                    // 还没轮到，稍后再问
    DoneTask                    // 所有任务完成，可以退出了
    NoReply
)

// 请求参数
type GetTaskArgs struct{}

/**
 * TaskID的作用：
 * 当是map任务时：可以看作文件列表的下标，即标识map处理的是哪个文件，也是mr-X-Y中的X（Y是key的哈希值）
 * 当是reduce任务时，就只是任务编号了。
 * 共同作用是让 Coordinator 标识每一个任务，在收到任务完成回复的时候，能知道是哪个任务完成了
 * 其应该是Coordinator中的一个原子变量
 */ 
type GetTaskReply struct {
    TaskType  TaskType // Map / Reduce / Wait / Done
    TaskID    int      // 任务编号，Map 阶段创建中间文件时，命名规则是 mr-X-Y，其中 X = TaskID：
    FileName  string   // 输入文件名，Map 用
    NReduce   int      // Reduce 数量，Map 用
    KeyHash   int      // Reduce 用，要处理些桶里的键值对——要处理哪些哈希值的key，mr-X-Y中的Y
}

type CompleteTaskArgs struct {
    TaskType TaskType
    TaskID   int
}

type CompleteTaskReply struct{}
