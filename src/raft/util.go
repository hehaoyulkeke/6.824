package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%s:%d;Term:%d;Log:%v;logLen:%v;Commit:%v;Apply:%v; next: %v]",
		rf.role, rf.me, rf.currentTerm, rf.log, len(rf.log)-1, rf.commitIndex, rf.lastApplied, rf.nextIndex)
}

const (
	Leader = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"
)

const None = -1

const (
	heartbeatInterval = 50 * time.Millisecond
	pingInterValBase = 150
	pingInterValOffset = 150
)

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}