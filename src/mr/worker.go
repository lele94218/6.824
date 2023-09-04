package mr

import "encoding/json"
import "fmt"
import "hash/fnv"
import "io/ioutil"
import "log"
import "net/rpc"
import "os"
import "sort"
import "time"

type CallStatus int

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	ERROR CallStatus = iota
	NO_TASK
	OK
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		status, filename := CallAssignMap(mapf)
		if status == ERROR {
			log.Fatal("Assign map failed.")
		}
		if status == OK {
			doneStatus := CallDoneMap(filename)
			if doneStatus == ERROR {
				log.Fatal("Call done map failed.")
			}
		}
		if status == NO_TASK {
			reduceStatus := CallAssignReduce(reducef)
			if reduceStatus == ERROR {
				log.Fatal("Assign reduce failed.")
			}
      if reduceStatus == NO_TASK {
        break
      }
		}
		time.Sleep(time.Second)
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

func CallAssignReduce(reducef func(string, []string) string) CallStatus {
	args := AssignReduceArgs{}
	reply := AssignReduceReply{}
	ok := call("Coordinator.AssignReduce", &args, &reply)
	if !ok {
		log.Print("Call failed!")
		return ERROR
	}

  if reply.NoTasks {
		log.Print("No more reduce tasks to assign")
    return NO_TASK
  }
  //
  log.Print(reply)
  //

	kva := []KeyValue{}
	for _, filename := range reply.ReduceFiles {
		ofile, _ := os.Open(filename)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	intermediate := kva

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.ReduceId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return OK
}

func CallDoneMap(filename string) CallStatus {
	args := DoneMapArgs{
		Filename: filename,
	}
	reply := DoneMapReply{}
	ok := call("Coordinator.DoneMap", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return ERROR
	}
	return OK
}

func CallAssignMap(mapf func(string, string) []KeyValue) (CallStatus, string) {
	// declare an argument structure.
	args := AssignMapArgs{}

	// declare a reply structure.
	reply := AssignMapReply{}

	ok := call("Coordinator.AssignMap", &args, &reply)
	if !ok {
		log.Print("call failed!\n")
		return ERROR, ""
	}
	if reply.NoTasks {
		log.Print("No more map tasks to assign")
		return NO_TASK, ""
	}
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	reduceKvaMap := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % reply.NumReduce
		reduceKvaMap[reduceId] = append(reduceKvaMap[reduceId], kv)
	}

	for key, value := range reduceKvaMap {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskId, key)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range value {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encoder error on: %s", oname)
			}
		}
		ofile.Close()
	}
	return OK, filename
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
