package mr

import (
	"time"
)

func (m *Master) GetReduceTask(reply *ReduceTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// start reduce task after all of the map tasks done
	if !m.isAllMapTaskDone() {
		return ErrMapStageUnfinished
	}
	task := m.reduceTasks[ReduceTaskInit].one()
	if task < 0 {
		return ErrNoReduceTask
	}
	m.convertReduceTaskStatus(task, ReduceTaskInit, TaskReducing)
	go m.timerReduceTask(task)
	reply.task = task
	return nil
}

func (m *Master) ReduceDone(args *ReduceTaskDoneArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.convertReduceTaskStatus(args.task, TaskReducing, ReduceTaskDone)
	return nil
}

func (m *Master) timerReduceTask(task int) {
	time.Sleep(taskTimeOut)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.convertReduceTaskStatus(task, TaskReducing, ReduceTaskInit)
}

func (m *Master) NReduce(reply *NReduceReply) error {
	reply.n = m.nReduce
	return nil
}
