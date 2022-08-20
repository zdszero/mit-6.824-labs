package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type SortBy []KeyValue

func (a SortBy) Len() int           { return len(a) }
func (a SortBy) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortBy) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		t := CallGetTask()
		switch t.TaskType {
		case MAP_TASK:
			mapWorker(mapf, t)
		case REDUCE_TASK:
			reduceWorker(reducef, t)
		case WAIT_TASK:
			time.Sleep(time.Second * 1)
		case DONE_TASK:
			break
		default:
			log.Fatalf("Worker Error: unknown task type %d", t.TaskType)
		}
	}
}

func mapWorker(mapf func(string, string) []KeyValue, t Task) {
	content, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		log.Fatalf("read file %v failed\n", t.Filename)
	}
	nReduce := t.NReduce
	// create intermidate files
	ifiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < len(encs); i++ {
		ifiles[i], _ = ioutil.TempFile(".", "mr-*")
		encs[i] = json.NewEncoder(ifiles[i])
	}
	// split reduce task
	kva := mapf(t.Filename, string(content))
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		err := encs[i].Encode(&kv)
		if err != nil {
			log.Fatalf("encode %v key %v value failed", kv.Key, kv.Value)
		}
	}
	// rename tempfile to reduce file
	ofilePrefix := "mr-" + strconv.Itoa(t.FileIndex)
	for oidx, ifile := range ifiles {
		ofilename := ofilePrefix + "-" + strconv.Itoa(oidx)
		ifile.Close()
		err = os.Rename(ifile.Name(), ofilename)
		if err != nil {
			log.Fatalf("rename file %v failed", ifile.Name())
		}
	}
	log.Printf("Worker: map %d finish\n", t.FileIndex)
	CallTaskDone(t)
}

func reduceWorker(reducef func(string, []string) string, t Task) {
	nFiles := t.NFiles
	partIndex := t.PartIndex
	filePrefix := "mr-"
	kva := []KeyValue{}
	for i := 0; i < nFiles; i++ {
		filename := filePrefix + strconv.Itoa(i) + "-" + strconv.Itoa(partIndex)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open intermidate file %v failed", filename)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			err = dec.Decode(&kv)
			if err != nil {
				// fmt.Printf("decode: %v\n", err)
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortBy(kva))
	ofile, err := ioutil.TempFile(".", "mr-*")
	outname := "mr-out-" + strconv.Itoa(partIndex)
	if err != nil {
		log.Println("create tempfile failed")
	}
	// fmt.Printf("len of kva: %d\n", len(kva))
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	err = os.Rename(ofile.Name(), outname)
	if err != nil {
		log.Fatalf("rename file %v failed", ofile.Name())
	}
	log.Printf("Worker: reduce %d finish\n", t.PartIndex)
	CallTaskDone(t)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTask() Task {
	args := ExampleArgs{}
	t := Task{}
	ok := call("Master.GetTask", &args, &t)
	if !ok {
		log.Println("Worker: all tasks has been done, exit")
		os.Exit(0)
	}
	return t
}

func CallTaskDone(t Task) {
	reply := ExampleReply{}
	ok := call("Master.TaskDone", &t, &reply)
	if !ok {
		log.Println("failed to call master taskdone method")
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
