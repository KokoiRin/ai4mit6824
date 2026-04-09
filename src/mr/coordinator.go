package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TypeStatus int

const (
	Idle TypeStatus = iota
	InProgress
	Completed
)

type Task struct {
	Id       int
	Type     TaskType   // Map 或 Reduce
	Status   TypeStatus // Idle / InProgress / Completed
	FileName string     // 仅 Map 用
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

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server(sockname)
	return &c
}
