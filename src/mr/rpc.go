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

type GetTaskArgs struct{}

type TaskAction int

const (
	DoMap TaskAction = iota
	DoReduce
	Wait
	Exit
)

type GetTaskReply struct {
	Action   TaskAction
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

type ReportTaskDoneArgs struct {
	TaskId int
	Type   TaskType
}

type ReportTaskDoneReply struct {
	Ok bool
}
