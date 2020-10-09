package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mu          sync.Mutex
	nMap        int
	mapTasks    []mapTaskSet
	nReduce     int
	reduceTasks []reduceTaskSet
}

func (m *Master) convertMapTaskStatus(task int, from, to mapStatus) {
	if from == to {
		return
	}
	rmd := m.mapTasks[from].removeTask(task)
	m.mapTasks[to].addTask(rmd)
}

func (m *Master) convertReduceTaskStatus(task int, from, to reduceStatus) {
	if task == 0 || from == to || !m.reduceTasks[from].contains(task) {
		return
	}
	m.reduceTasks[from].removeTask(task)
	m.reduceTasks[to].addTask(task)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.reduceTasks[ReduceTaskInit].isEmpty() && m.reduceTasks[TaskReducing].isEmpty()
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
	m.nMap = len(files)
	m.nReduce = nReduce

	m.server()
	return m
}

func (m *Master) initMapTasks(files []string) {
	m.mapTasks = make([]mapTaskSet, MapStatusCnt)
	for i := range m.mapTasks {
		m.mapTasks[i] = make(mapTaskSet, len(files))
	}
	taskSet := m.mapTasks[MapTaskInit]
	for i, f := range files {
		taskSet.addTask(&mapTask{id: i, filename: f})
	}
}

func (m *Master) initReduceTasks(nReduce int) {
	m.reduceTasks = make([]reduceTaskSet, ReduceStatusCnt)
	for i := range m.reduceTasks {
		m.reduceTasks[i] = make(reduceTaskSet, nReduce)
	}
	taskSet := m.reduceTasks[ReduceTaskInit]
	for i := 0; i < nReduce; i++ {
		taskSet.addTask(i)
	}
}
