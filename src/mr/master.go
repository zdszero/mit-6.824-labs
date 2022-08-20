package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

var Phase = MAP_TASK

const (
	RUNNING_TIME_LIMIT = time.Second * 10
)

type Master struct {
	// Your definitions here.
	mapWaiting    TaskQueue
	mapRunning    TaskQueue
	reduceWaiting TaskQueue
	reduceRunning TaskQueue

	nReduce int
	nFiles  int
	isDone  bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *ExampleArgs, t *Task) error {
	if m.isDone {
		t.TaskType = DONE_TASK
		return nil
	}
	if !m.mapWaiting.Empty() {
		*t = m.mapWaiting.Pop()
		m.mapRunning.Add(*t)
		go m.checkTimeout(t)
		log.Printf("Master: assign map %v\n", t.FileIndex)
		return nil
	}
	if !m.reduceWaiting.Empty() {
		*t = m.reduceWaiting.Pop()
		m.reduceRunning.Add(*t)
		go m.checkTimeout(t)
		log.Printf("Master: assign reduce %v\n", t.PartIndex)
		return nil
	}
	// all tasks are running
	t.TaskType = WAIT_TASK
	return nil
}

func (m *Master) TaskDone(t *Task, reply *ExampleReply) error {
	switch t.TaskType {
	case MAP_TASK:
		ret := m.mapRunning.Remove(*t)
		if ret == false {
			log.Fatalln("Master Error: fail to remove when task is done")
		}
		if m.mapWaiting.Empty() && m.mapRunning.Empty() {
			log.Println("Master: REDUCE PHASE")
			m.distributeReduce()
		}
	case REDUCE_TASK:
		ret := m.reduceRunning.Remove(*t)
		if ret == false {
			log.Fatalln("Master Error: fail to remove when task is done")
		}
		if m.reduceWaiting.Empty() && m.reduceRunning.Empty() {
			log.Println("Master: done")
			m.isDone = true
		}
	default:
		log.Fatalf("Master Error: unknown task type %d\n", t.TaskType)
	}
	return nil
}

func (m *Master) checkTimeout(t *Task) {
	if t.TaskType == MAP_TASK {
		time.Sleep(RUNNING_TIME_LIMIT)
		inQueue := m.mapRunning.Remove(*t)
		if inQueue == true {
			log.Printf("Master: map %d timeout", t.FileIndex)
			m.mapWaiting.Add(*t)
		}
	} else if t.TaskType == REDUCE_TASK {
		time.Sleep(RUNNING_TIME_LIMIT)
		inQueue := m.reduceRunning.Remove(*t)
		if inQueue == true {
			log.Printf("Master: reduce %d timeout", t.PartIndex)
			m.reduceWaiting.Add(*t)
		}
	}
}

func (m *Master) distributeReduce() {
	reduceTask := Task{
		TaskType:  REDUCE_TASK,
		NReduce:   m.nReduce,
		NFiles:    m.nFiles,
	}
	for i := 0; i < m.nReduce; i++ {
		reduceTask.PartIndex = i
		m.reduceWaiting.Add(reduceTask)
	}
	Phase = REDUCE_TASK
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Printf("map tasks: %d, reduce tasks: %d\n", len(files), nReduce)

	m := Master{
		mapWaiting:    TaskQueue{},
		mapRunning:    TaskQueue{},
		reduceWaiting: TaskQueue{},
		reduceRunning: TaskQueue{},
		nReduce:       nReduce,
		nFiles:        len(files),
		isDone:        false,
	}

	// add all files to map task
	for fileIdx, filename := range files {
		m.mapWaiting.Add(Task{
			TaskType:  MAP_TASK,
			Filename:  filename,
			FileIndex: fileIdx,
			PartIndex: -1,
			NReduce:   nReduce,
			NFiles:    len(files),
		})
	}

	m.server()
	return &m
}
