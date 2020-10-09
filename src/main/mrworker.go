package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"../mr"
	"strconv"
)
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so workerType(map|reduce) workerID\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	wID, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("invalid worker id: %v", err)
	}
	typ := os.Args[2]
	if typ == "map" {
		mr.MapWorker(wID, mapf)
	} else if typ == "reduce" {
		mr.ReduceWorker(wID, reducef)
	} else {
		log.Fatalf("unknown worker type %v", typ)
	}
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
