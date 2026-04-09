package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

	// 在这里实现 worker 逻辑

	// 取消注释以发送示例 RPC 到 coordinator
	// CallExample()

}

func CallGetTask() {
	// 声明参数结构体
	args := GetTaskArgs{}
	// 声明返回结构体
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("获得了数据\n")
	} else {
		fmt.Printf("调用失败!\n")
	}
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
