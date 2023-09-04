package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Map args and reply
type AssignMapArgs struct {
}

type AssignMapReply struct {
	TaskId    int
	Filename  string
	NoTasks   bool
	NumReduce int
}

// Reduce args and reply
type AssignReduceArgs struct {
}

type AssignReduceReply struct {
	ReduceId    int
	ReduceFiles []string
	NoStart     bool
	NoTasks     bool
}

// Done args and reply
type DoneMapArgs struct {
	Filename string
}

type DoneMapReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
