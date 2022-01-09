package mr

import (
	"sync"
	"time"
)

const (
	MAP_TASK    = 1
	REDUCE_TASK = 2
	WAIT_TASK   = 3
	DONE_TASK   = 4
	TIME_LIMIT  = time.Second * 10
)

type Task struct {
	TastType  int
	BeginTime time.Time
	Filename  string
	FileIndex int
	PartIndex int
	NReduce   int
	NFiles    int
}

func (t *Task) IsTimeOut() bool {
	return time.Now().Sub(t.BeginTime) > time.Duration(TIME_LIMIT)
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

func (tq *TaskQueue) Remove(t Task) {
	tq.mutex.Lock()
	for idx, task := range tq.q {
		if t.FileIndex == task.FileIndex {
			tq.q = append(tq.q[:idx], tq.q[idx+1:]...)
			break
		}
	}
	tq.mutex.Unlock()
}

func (tq *TaskQueue) TimeOutTasks() []Task {
	tq.mutex.Lock()
	ret := make([]Task, 0)
	for idx := 0; idx < len(tq.q); {
		t := tq.q[idx]
		if t.IsTimeOut() {
			tq.q = append(tq.q[:idx], tq.q[idx+1:]...)
			ret = append(ret, t)
		} else {
			idx++
		}
	}
	tq.mutex.Unlock()
	return ret
}

func (tq *TaskQueue) Empty() bool {
	return len(tq.q) == 0
}
