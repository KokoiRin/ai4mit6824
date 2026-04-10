package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map 函数返回 KeyValue 切片
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 来选择 reduce 任务编号
// 用于每个 KeyValue 被 Map 输出后
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // coordinator 的 socket 地址

// main/mrworker.go 调用此函数
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := CallGetTask()

		switch reply.Action {
		case DoMap:
			DoMapTask(reply.TaskId, reply.FileName, reply.NReduce, mapf)
		case DoReduce:
			DoReduceTask(reply.TaskId, reply.NMap, reducef)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}

}

func DoMapTask(taskId int, filename string, nReduce int,
	mapf func(string, string) []KeyValue) {

	// 读取文件
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	// 调用 mapf
	kva := mapf(filename, string(content))

	// 创建 nReduce 个临时文件，用 pid 和 taskId 区分避免冲突
	reduceFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	tmpFiles := make([]string, nReduce)
	pid := os.Getpid()
	for i := 0; i < nReduce; i++ {
		tmpFiles[i] = fmt.Sprintf("mr-tmp-%d-%d-%d", pid, taskId, i)
		reduceFiles[i], err = os.Create(tmpFiles[i])
		if err != nil {
			log.Fatalf("cannot create %v", tmpFiles[i])
		}
		encoders[i] = json.NewEncoder(reduceFiles[i])
	}

	// 遍历 kva，写到对应文件
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		encoders[reduceId].Encode(&kv)
	}

	// 关闭文件
	for i := 0; i < nReduce; i++ {
		reduceFiles[i].Close()
	}

	// 原子 rename 到最终文件名
	for i := 0; i < nReduce; i++ {
		os.Rename(tmpFiles[i], fmt.Sprintf("mr-%d-%d", taskId, i))
	}

	// 通知 coordinator 任务完成
	CallReportTaskDone(taskId, Map)
}

func DoReduceTask(reduceId int, nMap int,
	reducef func(string, []string) string) {

	// 读取所有 map 任务产生的属于这个 reduce 的文件
	kva := []KeyValue{}
	for mapId := 0; mapId < nMap; mapId++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 按 key 排序
	sort.Sort(ByKey(kva))

	// 写到临时文件，避免和重试冲突
	pid := os.Getpid()
	tmpFile := fmt.Sprintf("mr-out-tmp-%d-%d", pid, reduceId)
	file, err := os.Create(tmpFile)
	if err != nil {
		log.Fatalf("cannot create %v", tmpFile)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	file.Close()

	// 原子 rename 到最终文件名
	os.Rename(tmpFile, fmt.Sprintf("mr-out-%d", reduceId))

	// 清理中间文件
	for mapId := 0; mapId < nMap; mapId++ {
		os.Remove(fmt.Sprintf("mr-%d-%d", mapId, reduceId))
	}

	// 通知 coordinator 任务完成
	CallReportTaskDone(reduceId, Reduce)
}

// 按 key 排序
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		reply.Action = Exit
	}
	return reply
}

func CallReportTaskDone(taskId int, taskType TaskType) bool {
	args := ReportTaskDoneArgs{
		TaskId: taskId,
		Type:   taskType,
	}
	reply := ReportTaskDoneReply{}

	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	return ok && reply.Ok
}

// 示例函数，展示如何向 coordinator 发起 RPC 调用
//
// RPC 参数和返回类型定义在 rpc.go 中
func CallExample() {

	// 声明参数结构体
	args := ExampleArgs{}

	// 填充参数
	args.X = 99

	// 声明返回结构体
	reply := ExampleReply{}

	// 发送 RPC 请求，等待响应
	// "Coordinator.Example" 告诉接收服务器
	// 我们要调用 Coordinator 结构体的 Example() 方法
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y 应该是 100
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("调用失败!\n")
	}
}

// 向 coordinator 发送 RPC 请求，等待响应
// 通常返回 true
// 出错时返回 false
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
