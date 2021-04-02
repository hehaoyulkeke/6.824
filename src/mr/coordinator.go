package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	mapTasks chan int
	reduceTasks chan int
	mapSuc map[int]bool
	reduceSuc map[int]bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *struct{}, reply *TaskReply) error {
	c.mu.Lock()
	mapSucCnt := len(c.mapSuc)
	c.mu.Unlock()
	if mapSucCnt == len(c.files) {
		// deliver reduce task
		n := <- c.reduceTasks
		reply.N = n
		reply.Phase = Reduce
		go c.mayRetry(n, Reduce)
		return nil
	} else {
		// deliver map task
		n := <- c.mapTasks
		reply.N = n
		reply.Filename = c.files[n]
		reply.NReduce = c.nReduce
		reply.Phase = Map
		go c.mayRetry(n, Map)
		return nil
	}
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Phase {
	case Map:
		c.mapSuc[args.N] = true
	case Reduce:
		c.reduceSuc[args.N] = true
	}
	return nil
}

func (c *Coordinator) mayRetry(n int, phase string) {
	time.Sleep(10*time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()
	switch phase {
	case Map:
		if _, ok := c.mapSuc[n]; !ok {
			//map task timeout fail
			c.mapTasks <- n
		}
	case Reduce:
		if _, ok := c.reduceSuc[n]; !ok {
			//reduce task timeout fail
			c.reduceTasks <- n
		}
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := len(c.reduceSuc) == c.nReduce
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	mCh := make(chan int, len(files))
	for i := range files {
		mCh <- i
	}
	rCh := make(chan int, nReduce)
	for i:=0; i<nReduce; i++ {
		rCh <- i
	}
	c.mapTasks = mCh
	c.reduceTasks = rCh
	c.mapSuc = make(map[int]bool)
	c.reduceSuc = make(map[int]bool)
	c.files = files
	c.nReduce = nReduce
	// Your code here.
	c.server()
	return &c
}
