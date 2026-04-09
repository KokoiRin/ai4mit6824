package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Id        int
	Type      TaskType   // Map 或 Reduce
	Status    TaskStatus // Idle / InProgress / Completed
	FileName  string     // 仅 Map 用
	StartTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	files       []string
	nMap        int
	mapTasks    []Task
	nReduce     int
	reduceTasks []Task
	phase       Phase
}

// 在这里添加 RPC handler，供 worker 调用

// 示例 RPC handler
//
// RPC 参数和返回类型定义在 rpc.go 中
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 启动一个线程监听来自 worker.go 的 RPC 请求
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go 周期性调用 Done() 来检查
// 整个作业是否已完成
func (c *Coordinator) Done() bool {
	return c.phase == DonePhase
}

// 创建 Coordinator
// main/mrcoordinator.go 调用此函数
// nReduce 是 reduce 任务的数量
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 保存输入文件和 reduce 数量
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce

	// 初始化 map 任务
	c.mapTasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = Task{
			Id:       i,
			Type:     Map,
			Status:   Idle,
			FileName: files[i],
		}
	}

	// 初始化 reduce 任务
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Id:     i,
			Type:   Reduce,
			Status: Idle,
		}
	}

	// 初始阶段为 Map
	c.phase = MapPhase

	c.server(sockname)
	return &c
}
