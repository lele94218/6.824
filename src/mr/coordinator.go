package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
  mapped map[string]bool
  fileList []string
  nReduce int
  currTaskId int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignMap(args *AssignMapArgs, reply *AssignMapReply) error {
  reply.NumReduce = c.nReduce
  for _, filename := range c.fileList {
    if c.mapped[filename] == false {
      reply.Filename = filename
      reply.NoTasks = false
      reply.TaskId = c.currTaskId
      c.mapped[filename] = true
      c.currTaskId++
      return nil
    }
  }
  reply.Filename = ""
  reply.NoTasks = true
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapped:     make(map[string]bool),
		nReduce:    nReduce,
		currTaskId: 0,
	}

	// Your code here.
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
    c.fileList = append(c.fileList, filename)
	}

	c.server()
	return &c
}
