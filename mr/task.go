package mr

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	taskTimeOut         = 10 * time.Second
	requestTaskInterval = 1 * time.Second
)

type (
	taskStatus int
)

const (
	Init taskStatus = iota
	Executing
	Done
	StatusCnt int = iota
)

var (
	MapTaskStatusStr = [StatusCnt]string{
		Init:      "Init",
		Executing: "Executing",
		Done:      "Done",
	}
)

func (s taskStatus) String() string {
	return MapTaskStatusStr[s]
}

type task struct {
	id       int
	input 	 []string
	status   taskStatus
}

type TaskMgr struct {
	mu 	 sync.Mutex
	kind taskKind
	list []*task

	headNotStart int
}

func (m *TaskMgr) StartTask() *task {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.list {
		if t.status != Init {
			continue
		}
		if len(t.input) == 0 {
			t.status = Done
			logrus.Infof("StartTask: %v task %v has no content, mark it done directly", m.kind, t.id)
			continue
		}
		t.status = Executing
		go m.timerExecuting(t.id)
		logrus.Infof("StartTask: kind %v, ID %v", m.kind, t.id)
		return t
	}
	return nil
}

func (m *TaskMgr) timerExecuting(id int) {
	time.Sleep(taskTimeOut)
	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.list[id]
	if task.status == Executing {
		task.status = Init
		logrus.Infof("timerExecuting: execute %v task %v timeout", m.kind, id)
	}
}

func (m *TaskMgr) FinishTask(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.list[id].status = Done
	logrus.Infof("TaskDone: kind %v, ID %v", m.kind, id)
}

func (m *TaskMgr) isAllDone() bool {
	for _, t := range m.list {
		if t.status != Done {
			return false
		}
	}
	return true
}

func (m *TaskMgr) NumTask() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.list)
}

func (m *TaskMgr) IsAllDone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isAllDone()
}

type RTaskMgr struct {
	TaskMgr
}

func (m *RTaskMgr) HandleMapOutput(output []string) {
	if m.NumTask() != len(output) {
		logrus.Error("HandleMapOutput: len not equal")
		return
	}
	for i, task := range m.list {
		if output[i] == "" {
			continue
		}
		task.input = append(task.input, output[i])
	}
}
