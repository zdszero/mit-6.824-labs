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
		t.TastType = DONE_TASK
		return nil
	}
	if !m.mapWaiting.Empty() {
		*t = m.mapWaiting.Pop()
		m.mapRunning.Add(*t)
		log.Printf("Master: assign map task on file %v: %v to worker\n", t.FileIndex, t.Filename)
		return nil
	}
	if !m.reduceWaiting.Empty() {
		*t = m.reduceWaiting.Pop()
		m.reduceRunning.Add(*t)
		log.Printf("Master: assign reduce task on part %v to worker\n", t.PartIndex)
		return nil
	}
	// all tasks are running
	t.TastType = WAIT_TASK
	return nil
}

func (m *Master) TaskDone(t *Task, reply *ExampleReply) error {
	switch t.TastType {
	case MAP_TASK:
		log.Printf("Master: map task on file %d: %v has completed\n", t.FileIndex, t.Filename)
		m.mapRunning.Remove(*t)
		if m.mapWaiting.Empty() && m.mapRunning.Empty() {
			log.Println("Master: distribute reduce tasks")
			m.distributeReduce()
		}
	case REDUCE_TASK:
		log.Printf("Master: map task on part %d has completed\n", t.PartIndex)
		m.reduceRunning.Remove(*t)
		if m.reduceWaiting.Empty() && m.reduceRunning.Empty() {
			log.Println("Master: is done")
			m.isDone = true
		}
	default:
		log.Fatalf("Master Error: unknown task type %d\n", t.TastType)
	}
	return nil
}

func (m *Master) distributeReduce() {
	reduceTask := Task{
		TastType:  REDUCE_TASK,
		BeginTime: time.Now(),
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

func (m *Master) collectTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		var array []Task
		if Phase == MAP_TASK {
			array = m.mapRunning.TimeOutTasks()
			for _, t := range array {
				log.Printf("Master: map task %v on file %v times out", t.FileIndex, t.Filename)
				m.mapWaiting.Add(t)
			}
		} else {
			array = m.reduceRunning.TimeOutTasks()
			for _, t := range array {
				log.Printf("Master: reduce task %v on file %v times out", t.FileIndex, t.Filename)
				m.reduceWaiting.Add(t)
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapWaiting:    TaskQueue{},
		mapRunning:    TaskQueue{},
		reduceWaiting: TaskQueue{},
		reduceRunning: TaskQueue{},
		nReduce:       nReduce,
		nFiles:        len(files),
		isDone:        false,
	}

	// Your code here.

	// add all files to map task
	log.Println("Master: add all map task to task queue")
	for fileIdx, filename := range files {
		m.mapWaiting.Add(Task{
			TastType:  MAP_TASK,
			BeginTime: time.Now(),
			Filename:  filename,
			FileIndex: fileIdx,
			PartIndex: -1,
			NReduce:   nReduce,
			NFiles:    len(files),
		})
	}

	go m.collectTimeOut()
	m.server()
	return &m
}
