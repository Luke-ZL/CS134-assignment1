package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func doIthjob(jobIndex int, mr *MapReduce, doneChan chan int, jobT JobType) {
	var nOtherPhase int
	switch jobT {
	case Map:
		nOtherPhase = mr.nReduce
	case Reduce:
		nOtherPhase = mr.nMap
	default:
		return
	}
	for {
		worker := <-mr.registerChannel
		var reply DoJobReply
		callok := call(worker, "Worker.DoJob", &DoJobArgs{mr.file, jobT, jobIndex, nOtherPhase}, &reply)
		if callok && reply.OK {
			doneChan <- jobIndex
			mr.registerChannel <- worker
			return
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	doneChan := make(chan int)
	// map
	for i := 0; i < mr.nMap; i++ {
		go doIthjob(i, mr, doneChan, Map)
	}
	for i := 0; i < mr.nMap; i++ {
		//log.Println(<-doneChan)
		<-doneChan
	}
	//reduce
	for i := 0; i < mr.nReduce; i++ {
		go doIthjob(i, mr, doneChan, Reduce)
	}
	for i := 0; i < mr.nReduce; i++ {
		//log.Println(<-doneChan)
		<-doneChan
	}

	return mr.KillWorkers()
}
