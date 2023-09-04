package mr

import "fmt"
import "log"
import "net"
import "net/http"
import "net/rpc"
import "os"
import "path/filepath"
import "regexp"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	assigned        map[string]bool
	finished        map[string]bool
	startTime       map[string]time.Time
	fileList        []string
	nReduce         int
	reduceStartTime map[int]time.Time
	reduceAssigned  map[int]bool
	currTaskId      int
	currReduceId    int
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignMap(args *AssignMapArgs, reply *AssignMapReply) error {
	reply.NumReduce = c.nReduce
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, filename := range c.fileList {
		if c.assigned[filename] == false {
			reply.Filename = filename
			reply.NoTasks = false
			reply.TaskId = c.currTaskId
			c.assigned[filename] = true
			c.startTime[filename] = time.Now()
			c.currTaskId++
			log.Print("assigned", filename)
			return nil
		}
	}
	reply.Filename = ""
	reply.NoTasks = true
	return nil
}

func GetReduceResultFiles(reduceId int) []string {
	dir := "." // The directory you want to search in
	pattern := fmt.Sprintf(`^mr-\d+-%d$`, reduceId)

	regex, err := regexp.Compile(pattern)
	if err != nil {
		log.Fatal("Error compiling regex:", err)
	}

	matchingFiles := []string{}

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && regex.MatchString(info.Name()) {
			matchingFiles = append(matchingFiles, path)
		}
		return nil
	})

	if err != nil {
		log.Fatal("Error walking directory")
	}

	return matchingFiles
}

func (c *Coordinator) AssignReduce(args *AssignReduceArgs, reply *AssignReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, value := range c.finished {
		if value == false {
			reply.NoStart = true
			return nil
		}
	}
	if c.currReduceId == c.nReduce {
		reply.NoTasks = true
		return nil
	}
	log.Print("start to assign reduce")
	files := GetReduceResultFiles(c.currReduceId)
	reply.NoStart = false
	reply.NoTasks = false
	reply.ReduceFiles = files
	reply.ReduceId = c.currReduceId
	c.currReduceId++
	return nil
}

func (c *Coordinator) DoneMap(args *DoneMapArgs, reply *DoneMapReply) error {
	c.mu.Lock()
	log.Print("finished: ", args.Filename)
	c.finished[args.Filename] = true
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	// Your code here.
	if c.currReduceId == c.nReduce {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

func (c *Coordinator) Schedule() {
	for !c.Done() {
		c.mu.Lock()
		for key, value := range c.startTime {
			if (c.assigned[key] == true && c.finished[key] == false) &&
				time.Now().Sub(value) > (5.0*time.Second) {
				c.assigned[key] = false
				c.finished[key] = false
			}
		}
		c.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		assigned:        make(map[string]bool),
		finished:        make(map[string]bool),
		startTime:       make(map[string]time.Time),
		reduceStartTime: make(map[int]time.Time),
		reduceAssigned:  make(map[int]bool),
		nReduce:         nReduce,
		currTaskId:      0,
		currReduceId:    0,
	}

	// Your code here.
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("cannot open %v", filename)
		}
		file.Close()
		c.fileList = append(c.fileList, filename)
		c.assigned[filename] = false
		c.finished[filename] = false
	}
	go c.Schedule()
	c.server()
	return &c
}
