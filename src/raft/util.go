package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
		rf.role, rf.me, rf.currentTerm, rf.votedFor, len(rf.log)-1, rf.commitIndex, rf.lastApplied)
}

const (
	Leader = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"
)

const None = -1

const heartbeatInterval = 50 * time.Millisecond