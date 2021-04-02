package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


const (
	Map = "Map"
	Reduce = "Reduce"
)

// Add your RPC definitions here.
type DoneTaskArgs struct {
	N int
	Phase string

}

type TaskReply struct {
	Phase string
	N int
	NReduce int
	Filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
