package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func MapWorker(workerID int, mapf func(string, string) []KeyValue) {
	r := NReduceReply{}
	if ok := call("Master.NReduce", nil, &r); ok {
		log.Fatalf("Master.NReduce failed")
	}
	nReduce := r.n
	for {
		time.Sleep(10 * time.Second)

		reply := MapTaskReply{}
		if ok := call("Master.GetMapTask", nil, &reply); !ok {
			continue
		}
		in, err := os.Open(reply.filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.filename)
		}
		content, err := ioutil.ReadAll(in)
		if err != nil {
			log.Fatalf("cannot read %v", reply.filename)
		}
		in.Close()
		tmpFiles := make([]*os.File, nReduce)
		kva := mapf(reply.filename, string(content))
		for _, kv := range kva {
			writeKV2TmpFile(workerID, reply.taskID, kv, tmpFiles)
		}
		for i, f := range tmpFiles {
			newPath := tmpFilename(reply.taskID, i)
			err = os.Rename(f.Name(), newPath)
			if err != nil {
				log.Fatalf("rename to output file: %v", err)
			}
			f.Close()
		}
		doneArgs := MapTaskDoneArgs{taskID: reply.taskID}
		call("Master.MapDone", &doneArgs, nil)
	}
}

func writeKV2TmpFile(workerID, taskID int, kv KeyValue, tmpFiles []*os.File) {
	reduceID := ihash(kv.Key) % len(tmpFiles)
	out := tmpFiles[reduceID]
	var err error
	if out == nil {
		tmpFilename := strings.Join([]string{"tmp", strconv.Itoa(workerID), strconv.Itoa(taskID), strconv.Itoa(reduceID)}, "-")
		out, err = os.Create(tmpFilename)
		checkFail("writeKV2TmpFile: open output file", err)
		tmpFiles[reduceID] = out
	}
	enc := json.NewEncoder(out) // fixme(zhanghao): pass []*json.Encoder to avoid creating duplicated encoder
	err = enc.Encode(&kv)
	if err != nil {
		log.Fatalf("encode to output file: %v", err)
	}
}

// ------------------ reduce ------------------

func ReduceWorker(workerID int, reducef func(string, []string) string) {
	r := NReduceReply{}
	if ok := call("Master.NMap", nil, &r); ok {
		log.Fatalf("Master.NMap failed")
	}
	nMap := r.n
	for {
		time.Sleep(10 * time.Second)

		reply := ReduceTaskReply{}
		if ok := call("Master.GetReduceTask", nil, &reply); !ok {
			continue
		}
		files := tmpFileOfReduce(nMap, reply.task)
		intermediate := kvInFiles(files)

		filename := outFilename(reply.task)
		tmp := strconv.Itoa(workerID) + "-" + filename
		tmpFile, err := os.Create(tmp)
		checkFail("ReduceWorker create tmp file", err)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			_, err = fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
			checkFail("ReduceWorker", err)
			i = j
		}
		err = os.Rename(tmp, filename)
		checkFail("ReduceWorker rename", err)
		tmpFile.Close()
		closeFiles(files)
		doneArgs := ReduceTaskDoneArgs{task: reply.task}
		call("Master.ReduceDone", &doneArgs, nil)
	}
}

func checkFail(prefix string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", prefix, err)
	}
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}

func kvInFiles(files []*os.File) []KeyValue {
	ret := make([]KeyValue, 0, 500)
	for _, f := range files {
		var kvs []KeyValue
		dec := json.NewDecoder(f)
		err := dec.Decode(&kvs)
		if err != nil {
			log.Fatalf("kvInFiles: %v", err)
		}
		ret = append(ret, kvs...)
	}
	return ret
}

func tmpFileOfReduce(nMap, r int) []*os.File {
	var files []*os.File
	for i := 0; i < nMap; i++ {
		filename := tmpFilename(i, r)
		f, err := os.Open(filename)
		if err != nil {
			if err == os.ErrNotExist {
				continue
			}
			log.Fatalf("tmpFileOfReduce: %v", err)
		}
		files = append(files, f)
	}
	return files
}

func tmpFilename(mapTask, reduceTask int) string {
	return strings.Join([]string{"mr", strconv.Itoa(mapTask), strconv.Itoa(reduceTask)}, "-")
}

func outFilename(reduceTask int) string {
	return "mr-out-" + strconv.Itoa(reduceTask)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
