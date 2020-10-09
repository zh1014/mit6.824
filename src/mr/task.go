package mr

import "time"

type mapStatus int
type reduceStatus int

const (
	MapTaskInit mapStatus = iota
	TaskMapping
	MapTaskDone
	MapStatusCnt

	iota                        = 0
	ReduceTaskInit reduceStatus = iota
	TaskReducing
	ReduceTaskDone
	ReduceStatusCnt

	taskTimeOut = 10 * time.Second
)

type mapTaskSet []*mapTask

type mapTask struct {
	id       int
	filename string
}

func (s mapTaskSet) one() *mapTask {
	for _, task := range s {
		if task != nil {
			return task
		}
	}
	return nil
}

func (s mapTaskSet) removeTask(task int) *mapTask {
	ret := s[task]
	s[task] = nil
	return ret
}

func (s mapTaskSet) addTask(task *mapTask) {
	if task == nil {
		return
	}
	s[task.id] = task
}

func (s mapTaskSet) contains(task int) bool {
	return s[task] != nil
}

func (s mapTaskSet) isEmpty() bool {
	for _, task := range s {
		if task != nil {
			return false
		}
	}
	return true
}

type reduceTaskSet []bool

func (s reduceTaskSet) one() int {
	for task, exist := range s {
		if exist {
			return task
		}
	}
	return -1
}

func (s reduceTaskSet) removeTask(task int) {
	s[task] = false
}

func (s reduceTaskSet) addTask(task int) {
	s[task] = true
}

func (s reduceTaskSet) contains(task int) bool {
	return s[task]
}

func (s reduceTaskSet) isEmpty() bool {
	for _, exist := range s {
		if exist {
			return false
		}
	}
	return true
}
