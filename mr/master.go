package mr

import (
	"github.com/sirupsen/logrus"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mapTask 	*TaskMgr
	rdcTask		*RTaskMgr
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
	return m.rdcTask.IsAllDone()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := &Master{}
	m.initMapTasks(files)
	m.initReduceTasks(nReduce)

	m.server()
	return m
}

func (m *Master) initMapTasks(files []string) {
	m.mapTask = &TaskMgr{kind: taskKindMap}
	taskMgr := m.mapTask
	for i, f := range files {
		taskMgr.list = append(taskMgr.list, &task{
			id:       i,
			input: 	  []string{f},
			status:   Init,
		})
	}
	logrus.Info("initMapTasks done.")
}

func (m *Master) initReduceTasks(nReduce int) {
	m.rdcTask = new(RTaskMgr)
	m.rdcTask.kind = taskKindReduce
	m.rdcTask.list = make([]*task, nReduce)
	tasks := m.rdcTask.list
	for i := range tasks {
		tasks[i] = &task{
			id: i,
			status: Init,
		}
	}
	logrus.Info("initReduceTasks done.")
}
