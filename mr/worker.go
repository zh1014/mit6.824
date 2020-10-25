package mr

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"time"
)
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func HandleMapTask(mapf func(string, string) []KeyValue, task *TaskReply, nReduce int) {
	input := task.Input[0]

	// execute task and store key-value pairs to temp file
	kvs := mapf(input, string(getFileContent(input)))
	output := storeKVs(kvs, task.ID, nReduce)

	// done. sync to master
	doneArgs := TaskDoneArgs{Kind:taskKindMap, ID: task.ID, Output: output}
	call("Master.TaskDone", &doneArgs, &EmptyParam{})
}

func getFileContent(filename string) []byte {
	in, err := os.Open(filename)
	checkFail(fmt.Sprintf("cannot open %v", filename), err)
	defer in.Close()
	var content []byte
	content, err = ioutil.ReadAll(in)
	checkFail(fmt.Sprintf("cannot read %v", filename), err)
	return content
}

func storeKV(kv KeyValue, tmpFiles []*os.File, edrs []*json.Encoder) {
	outputID := ihash(kv.Key) % len(edrs)
	enc := edrs[outputID]
	var err error
	if enc == nil {
		var tmp *os.File
		tmp, err = ioutil.TempFile("", "temp")
		checkFail("storeKV: open output file", err)
		tmpFiles[outputID] = tmp
		enc = json.NewEncoder(tmp)
		edrs[outputID] = enc
	}
	err = enc.Encode(&kv)
	checkFail("encode to output file", err)
}

func storeKVs(kvs []KeyValue, mTaskID, nReduce int) []string {
	tmpFiles := make([]*os.File, nReduce)
	edrs := make([]*json.Encoder, nReduce)
	for _, kv := range kvs {
		storeKV(kv, tmpFiles, edrs)
	}

	var (
		err error
		tmpFilenames []string
	)
	for i, f := range tmpFiles {
		if f == nil {
			tmpFilenames = append(tmpFilenames, "")
			continue
		}
		newPath := tmpFilename(mTaskID, i)
		err = os.Rename(f.Name(), newPath)
		checkFail(fmt.Sprintf("rename to output file %v failed", newPath), err)
		f.Close()
		tmpFilenames = append(tmpFilenames, newPath)
		logrus.Infof("storeKVs: tmp file %v generated", newPath)
	}
	return tmpFilenames
}

func HandleReduceTask(reducef func(string, []string) string, task *TaskReply) {
	interKvs := readKVInFiles(task.Input)

	uniqFile, err := ioutil.TempFile("", "mr-out")
	checkFail("HandleReduceTask create uniqFile file", err)
	defer uniqFile.Close()
	i := 0
	for i < len(interKvs) {
		j := i + 1
		for j < len(interKvs) && interKvs[j].Key == interKvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, interKvs[k].Value)
		}
		output := reducef(interKvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(uniqFile, "%v %v\n", interKvs[i].Key, output)
		checkFail("HandleReduceTask", err)
		i = j
	}
	outFile := outFilename(task.ID)
	err = os.Rename(uniqFile.Name(), outFile)
	checkFail("HandleReduceTask rename", err)
	removeFiles(task.Input)
	doneArgs := TaskDoneArgs{Kind: taskKindReduce, ID: task.ID, Output: []string{outFile}}
	call("Master.TaskDone", &doneArgs, &EmptyParam{})
}

func checkFail(prefix string, err error) {
	if err != nil {
		logrus.Fatalf("%v: %v", prefix, err)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logrus.Fatal("dialing:", err)
	}
	defer c.Close()

	//logrus.Debugf("calling %v, args %+v", rpcname, args)
	err = c.Call(rpcname, args, reply)
	if err != nil {
		logrus.Errorf("rpc call %v: %v", rpcname, err)
	}
	return err
}

func WorkerRun(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	masterDesc := MasterDescReply{}
	err := call("Master.MasterDesc", &EmptyParam{}, &masterDesc)
	checkFail("can not get number of map task", err)
	logrus.Infof("get MasterDesc: %v", masterDesc)

	for {
		time.Sleep(requestTaskInterval)

		reply := &TaskReply{}
		err = call("Master.GetTask", &EmptyParam{}, reply)
		if err != nil {
			//logrus.Errorf("failed to get task: %v", err)
			if err.Error() == ErrMasterExiting.Error() {
				// worker exit
				return
			}else {
				// retry
				continue
			}
		}
		logrus.Infof("HandleReduceTask: get %v task %v", reply.Kind, reply.ID)

		if len(reply.Input) == 0 {
			logrus.Errorf("task has no content: %+v", reply)
			continue
		}

		if reply.Kind == taskKindMap {
			HandleMapTask(mapf, reply, masterDesc.NumRdcTask)
		}else if reply.Kind == taskKindReduce {
			HandleReduceTask(reducef, reply)
		}else {
			logrus.Errorf("WorkerRun: unknown reply.Kind %v", reply.Kind)
		}
	}
}
