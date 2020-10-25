package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyParam struct {
}

type TaskReply struct {
	Kind taskKind
	ID 	 int

	Input []string
}

type TaskDoneArgs struct {
	Kind taskKind
	ID 	 int

	Output []string
}

type MasterDescReply struct {
	NumMapTask int
	NumRdcTask int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func (m *Master) GetTask(_ *EmptyParam, reply *TaskReply) error {
	mTask := m.mapTask.StartTask()
	if mTask != nil {
		pkgTask(taskKindMap, mTask, reply)
		return nil
	}
	// start reduce task after all of the map tasks done
	if !m.mapTask.isAllDone() {
		return ErrWaitMapStage
	}

	rTask := m.rdcTask.StartTask()
	if rTask != nil {
		pkgTask(taskKindReduce, rTask, reply)
		return nil
	}
	if !m.rdcTask.IsAllDone() {
		return ErrNoExecutableTask
	}
	return ErrMasterExiting
}

func pkgTask(kind taskKind, task *task, reply *TaskReply) {
	reply.Kind = kind
	reply.ID = task.id
	reply.Input = make([]string, len(task.input))
	copy(reply.Input, task.input)
}

func (m *Master) TaskDone(args *TaskDoneArgs, _ *EmptyParam) error {
	if args.Kind == taskKindMap {
		if len(args.Output) != m.rdcTask.NumTask() {
			return ErrInvalidParam
		}
		m.rdcTask.HandleMapOutput(args.Output)
		m.mapTask.FinishTask(args.ID)
	}else if args.Kind == taskKindReduce {
		m.rdcTask.FinishTask(args.ID)
	}else {
		return ErrInvalidParam
	}
	return nil
}

func (m *Master) MasterDesc(_ *EmptyParam, reply *MasterDescReply) error {
	reply.NumMapTask = m.mapTask.NumTask()
	reply.NumRdcTask = m.rdcTask.NumTask()
	return nil
}

