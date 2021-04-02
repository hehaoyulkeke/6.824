package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply := callTask()
		var err error
		switch reply.Phase {
		case Map:
			err = doMap(reply.Filename, reply.N, reply.NReduce, mapf)
		case Reduce:
			err = doReduce(reply.N, reducef)
		}
		if err != nil {
			replyDoneTask(reply.N, reply.Phase)
		}
	}
}

func doMap(filename string, nMap int, nReduce int, mapf func(string, string) []KeyValue) error {
	intermediate := make(map[int][]KeyValue)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	_ = file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		partion := ihash(kv.Key) % nReduce
		intermediate[partion] = append(intermediate[partion], kv)
	}
	//write to file
	for p, ls := range intermediate {
		file, err := os.Create(intermediateFilename(nMap, p))
		if err != nil {
			log.Fatalf("cannot create %s", file.Name())
			return err
		}
		enc := json.NewEncoder(file)
		for _, kv := range ls {
			_ = enc.Encode(&kv)
		}
		file.Close()
	}
	return nil
}

func doReduce(nReduce int, reducef func(string, []string) string) error {
	buf := make(map[string][]string)
	files, _ := ioutil.ReadDir("mr-tmp")
	for _, file := range files {
		fh, err := os.Open(file.Name())
		if err != nil {
			return err
		}
		dec := json.NewDecoder(fh)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			buf[kv.Key] = append(buf[kv.Key], kv.Value)
		}
	}

	//output
	outFile, err := os.Create(outFilename(nReduce))
	if err != nil {
		return err
	}
	for k, ls := range buf {
		v := reducef(k, ls)
		fmt.Fprintf(outFile, "%v %v\n", k, v)
	}
	outFile.Close()
	return nil
}

func intermediateFilename(nMap, nReduce int) string {
	return fmt.Sprintf("mr-%d-%d", nMap, nReduce)
}

func outFilename(nReduce int) string {
	return fmt.Sprintf("mr-out-%d", nReduce)
}


func callTask() TaskReply {
	args := struct{}{}
	reply := TaskReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func replyDoneTask(n int, phase string) {
	args := DoneTaskArgs{n, phase}
	reply := struct{}{}
	call("Coordinator.DoneTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
