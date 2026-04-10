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
	mu          sync.Mutex
	files       []string
	nMap        int
	mapTasks    []Task
	nReduce     int
	reduceTasks []Task
	phase       Phase
}

// Worker 请求新任务
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		// 分配 Map 任务
		for i := 0; i < c.nMap; i++ {
			if c.mapTasks[i].Status == Idle {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.Action = DoMap
				reply.TaskId = i
				reply.FileName = c.mapTasks[i].FileName
				reply.NReduce = c.nReduce
				return nil
			}
			// 检查超时：如果任务执行超过 10 秒但未完成，重新分配
			if c.mapTasks[i].Status == InProgress {
				if time.Since(c.mapTasks[i].StartTime) > 10*time.Second {
					c.mapTasks[i].Status = InProgress
					c.mapTasks[i].StartTime = time.Now()
					reply.Action = DoMap
					reply.TaskId = i
					reply.FileName = c.files[i]
					reply.NReduce = c.nReduce
					return nil
				}
			}
		}
		// 所有 Map 任务都在执行中，等待
		reply.Action = Wait
		return nil

	case ReducePhase:
		// 分配 Reduce 任务
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i].Status == Idle {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.Action = DoReduce
				reply.TaskId = i
				reply.NReduce = c.nReduce
				return nil
			}
			// 检查超时
			if c.reduceTasks[i].Status == InProgress {
				if time.Since(c.reduceTasks[i].StartTime) > 10*time.Second {
					c.reduceTasks[i].Status = InProgress
					c.reduceTasks[i].StartTime = time.Now()
					reply.Action = DoReduce
					reply.TaskId = i
					reply.NReduce = c.nReduce
					return nil
				}
			}
		}
		reply.Action = Wait
		return nil

	case DonePhase:
		reply.Action = Exit
		return nil
	}

	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case Map:
		// 丢弃：任务不在执行中 或 已进入 reduce 阶段
		if c.mapTasks[args.TaskId].Status != InProgress {
			reply.Ok = false
			return nil
		}
		if c.phase != MapPhase {
			reply.Ok = false
			return nil
		}
		c.mapTasks[args.TaskId].Status = Completed
		// 检查是否所有 Map 任务都完成
		for _, task := range c.mapTasks {
			if task.Status != Completed {
				reply.Ok = true
				return nil
			}
		}
		c.phase = ReducePhase
		reply.Ok = true
	case Reduce:
		// 丢弃：任务不在执行中
		if c.reduceTasks[args.TaskId].Status != InProgress {
			reply.Ok = false
			return nil
		}
		c.reduceTasks[args.TaskId].Status = Completed
		// 检查是否所有 Reduce 任务都完成
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				reply.Ok = true
				return nil
			}
		}
		c.phase = DonePhase
		reply.Ok = true
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()
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
