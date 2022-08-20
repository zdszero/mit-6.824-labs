package mr

import (
	"sync"
)

const (
	MAP_TASK    = 1
	REDUCE_TASK = 2
	WAIT_TASK   = 3
	DONE_TASK   = 4
)

type Task struct {
	TaskType  int
	Filename  string
	FileIndex int
	PartIndex int
	NReduce   int
	NFiles    int
}

type TaskQueue struct {
	q     []Task
	mutex sync.Mutex
}

func (tq *TaskQueue) Add(t Task) {
	tq.mutex.Lock()
	tq.q = append(tq.q, t)
	tq.mutex.Unlock()
}

func (tq *TaskQueue) Pop() Task {
	tq.mutex.Lock()
	t := tq.q[0]
	tq.q = tq.q[1:]
	tq.mutex.Unlock()
	return t
}

func (tq *TaskQueue) Remove(t Task) bool {
	ret := false
	tq.mutex.Lock()
	if t.TaskType == MAP_TASK {
		for idx, task := range tq.q {
			if t.FileIndex == task.FileIndex {
				tq.q = append(tq.q[:idx], tq.q[idx+1:]...)
				ret = true
				break
			}
		}
	} else if t.TaskType == REDUCE_TASK {
		for idx, task := range tq.q {
			if t.PartIndex == task.PartIndex {
				tq.q = append(tq.q[:idx], tq.q[idx+1:]...)
				ret = true
				break
			}
		}
	}
	tq.mutex.Unlock()
	return ret
}

func (tq *TaskQueue) Empty() bool {
	return len(tq.q) == 0
}
