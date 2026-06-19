package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}


// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
// mapf和reducef是传过来的map函数和reduce函数

// map产生的中间文件命名为 mr-X-Y, X is the Map task number, Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		time.Sleep(time.Millisecond * 100)
		reply := CallGetTask()
		switch reply.TaskType {
		case MapTask:
			{
				// 读取文件
				content, err := os.ReadFile(reply.FileName)
				if err != nil {
					log.Fatalf("cannot op %v", reply.FileName)
				}
				// 调用map函数
				kva := mapf(reply.FileName, string(content))
				// 在内存中完成分桶
				nReduce := reply.NReduce
				buckets := make([][]KeyValue, reply.NReduce)
				n := len(kva)
				for i := 0; i < n; i++ {
					index := ihash(kva[i].Key) % nReduce
					buckets[index] = append(buckets[index], kva[i])
				}
				// 把分桶后的中间结果写入文件
				for i := 0; i < nReduce; i++ {
					if buckets[i] == nil {
						continue
					}
					tmpFile, err := os.CreateTemp("", "mr-tmp-*")
					if err != nil {
						log.Fatalf("cannot create temp file: %v", err)
					}
					enc := json.NewEncoder(tmpFile)
					for _, kv := range buckets[i] {
						if err := enc.Encode(&kv); err != nil {
							log.Fatalf("json encode error: %v", err)
						}
					}
					tmpFile.Close()

					// 这里会不会有路径的问题？
					oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
					os.Rename(tmpFile.Name(), oname)
				}
				// 通知coordinator任务完成
				CallCompleteTask(MapTask, reply.TaskID)
			}
		case ReduceTask:
			{
				hashKey := reply.KeyHash
				// 获取满足要求的文件列表
				filenames, err := filepath.Glob(fmt.Sprintf("mr-*-%d", hashKey))
				if err != nil {
					log.Fatalf("glob error: %v", err)
				}
				allKv := []KeyValue{}
				// 读取每个文件，把kv加载到内存中
				for _, filename := range filenames {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v, %v", filename, err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						allKv = append(allKv, kv)
					}
					file.Close()
				}
				// 排序所有key
				sort.Slice(allKv, func(i, j int) bool {
					return allKv[i].Key < allKv[j].Key
				})

				outputFilename := fmt.Sprintf("mr-out-%d", hashKey)
				tmpFile, err := os.CreateTemp("", "mr-tmp-*")
				if err != nil {
					log.Fatalf("cannot create temp file: %v", err)
				}

				i := 0
				for i < len(allKv) {
					j := i + 1
					for j < len(allKv) && allKv[j].Key == allKv[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, allKv[k].Value)
					}
					output := reducef(allKv[i].Key, values)

					fmt.Fprintf(tmpFile, "%v %v\n", allKv[i].Key, output)
					i = j
 				}
				tmpFile.Close()
				os.Rename(tmpFile.Name(), outputFilename)
				// 通知coordinator任务完成
				CallCompleteTask(ReduceTask,reply.TaskID)
			}
		case WaitTask:
			{
				time.Sleep(time.Second)
				continue
			}
		case DoneTask:
			{
				return
			}
		case NoReply:
			{
				return
			}
		default:
			{
				return
			}
		}
	}

}


// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply
	}
	reply.TaskType = NoReply
	return reply
}

func CallCompleteTask(taskType TaskType, taskID int) {
	args := CompleteTaskArgs{}
	reply := CompleteTaskReply{}
	args.TaskType = taskType
	args.TaskID = taskID
	call("Coordinator.CompleteTask", &args, &reply)
	// if ok {
	// 	return nil
	// }
	// return error{}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
