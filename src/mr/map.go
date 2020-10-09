package mr

import "time"

func (m *Master) GetMapTask(reply *MapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.mapTasks[MapTaskInit].one()
	if task == nil {
		return ErrNoMapTask
	}
	m.convertMapTaskStatus(task.id, MapTaskInit, TaskMapping)
	go m.timerMapTask(task.id)
	reply.taskID = task.id
	reply.filename = task.filename
	return nil
}

func (m *Master) timerMapTask(taskID int) {
	time.Sleep(taskTimeOut)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.convertMapTaskStatus(taskID, TaskMapping, MapTaskInit)
}

func (m *Master) MapDone(args *MapTaskDoneArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.convertMapTaskStatus(args.taskID, TaskMapping, MapTaskDone)
	return nil
}

func (m *Master) isAllMapTaskDone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.mapTasks[MapTaskInit].isEmpty() && m.mapTasks[TaskMapping].isEmpty()
}

func (m *Master) NMap(reply *NMapReply) error {
	reply.n = m.nMap
	return nil
}
