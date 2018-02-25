package mapreduce

import "container/list"
import "fmt"


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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mapFinishChannel := make(chan int, nMap)
	for i := 0; i < mr.nMap; i++ {
		go func (jobNumber int){
			for{
				availWorker := <- mr.registerChannel
				doJobArgs := &DoJobArgs{}
				doJobReply := &DoJobReply{}
				doJobArgs.File = mr.file
				doJobArgs.Operation = Map
				doJobArgs.JobNumber = jobNumber
				doJobArgs.NumOtherPhase = mr.nReduce
				ok := call(availWorker, "Worker.DoJob", doJobArgs, doJobReply)
				if ok == true {
					mr.registerChannel <- availWorker
					mapFinishChannel <- jobNumber
					break
				}
			}
		}(i)
	}
	//make sure all map are finished
	for i := 0; i < mr.nMap; i++ {
		<- mapFinishChannel
	}

	reduceFinishChannel := make(chan int, nReduce)
	for i := 0; i < mr.nReduce; i++ {
		go func (jobNumber int) {
			for {
				availWorker := <- mr.registerChannel
				doJobArgs := &DoJobArgs{}
				doJobReply := &DoJobReply{}
				doJobArgs.File = mr.file
				doJobArgs.Operation = Reduce
				doJobArgs.JobNumber = jobNumber
				doJobArgs.NumOtherPhase = mr.nMap
				ok := call(availWorker, "Worker.DoJob", doJobArgs, doJobReply)
				if ok == true {
					mr.registerChannel <- availWorker
					reduceFinishChannel <- jobNumber
					break
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<- reduceFinishChannel
	}

	fmt.Println("master.go finished")

	return mr.KillWorkers()
}
