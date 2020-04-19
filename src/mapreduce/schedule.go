package mapreduce

import (
	"fmt"
	"sync"
)

type ResultStruct struct {
	Srv string
	TaskNumberIndex int
	Reply bool
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	//the mapFiles argument holds the names of the files that are the inputs to the map phase, one per map task.
		//nReduce is the number of reduce tasks
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	//reference
	//https://gobyexample.com/waitgroups
	var wg sync.WaitGroup
	for i:=0; i<ntasks; i++{
		wg.Add(1)
		go worker(jobName,mapFiles,phase,registerChan,n_other,i, &wg)
	}
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

func worker(jobName string, mapFiles []string, phase jobPhase, registerChan chan string, n_other int ,id int, wg *sync.WaitGroup) {
	dotaskargs := DoTaskArgs{jobName, mapFiles[id], phase, id, n_other}
	result := false
	var workerAddress string
	for result!=true {
		workerAddress = <- registerChan
		result  = call(workerAddress, "Worker.DoTask", dotaskargs, nil)
	}
	wg.Done()
	registerChan <- workerAddress
}