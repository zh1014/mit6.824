package mr

import "errors"

var (
	ErrWaitMapStage     = errors.New("map tasks are executing, no executable task now")
	ErrNoExecutableTask = errors.New("reduce tasks are executing, no executable task now")
	ErrMasterExiting    = errors.New("master exiting")
	ErrInvalidParam     = errors.New("invalid param")
)

type taskKind int

const (
	taskKindMap    taskKind = 1
	taskKindReduce taskKind = 2
)

func (k taskKind) String() string {
	switch k {
	case taskKindMap:
		return "Map"
	case taskKindReduce:
		return "Reduce"
	default:
		return "InvalidTaskKind"
	}
}
