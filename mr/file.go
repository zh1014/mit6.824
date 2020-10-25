package mr

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func removeFiles(files []string) {
	var err error
	for _, f := range files {
		err = os.Remove(f)
		checkFail("removeFiles", err)
	}
}

func readKVInFiles(input []string) []KeyValue {
	if len(input) == 0 {
		return nil
	}
	var (
		err error
		f   *os.File
		ret = make([]KeyValue, 0, 50*len(input))
	)
	for _, filename := range input {
		f, err = os.Open(filename)
		if err != nil {
			if err == os.ErrNotExist {
				continue
			}
			log.Fatalf("readKVInFiles: %v", err)
		}
		kvs := readKVInFile(f)
		ret = append(ret, kvs...)
		err = f.Close()
		checkFail("readKVInFiles close", err)
	}
	sort.Sort(ByKey(ret))
	return ret
}

func readKVInFile(f *os.File) []KeyValue {
	ret := make([]KeyValue, 0, 50)
	dec := json.NewDecoder(f)
	for {
		var kv KeyValue
		err := dec.Decode(&kv)
		if err != nil {
			break
		}
		ret = append(ret, kv)
	}
	return ret
}

func tmpFilename(mapTask, reduceTask int) string {
	return strings.Join([]string{"/Users/zhanghao1/code/6.824/mr-tmp/mr", strconv.Itoa(mapTask), strconv.Itoa(reduceTask)}, "-")
}

func outFilename(reduceTask int) string {
	return "/Users/zhanghao1/code/6.824/mr-tmp/mr-out-" + strconv.Itoa(reduceTask)
}
