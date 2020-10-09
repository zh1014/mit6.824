package mr

import "errors"

var (
	ErrMapStageUnfinished = errors.New("map stage unfinished")
	ErrNoReduceTask       = errors.New("no reduce task")
	ErrNoMapTask          = errors.New("no map task")
)
