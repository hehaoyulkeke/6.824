package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "6.824/labrpc"
import "6.824/labgob"
import "time"
import "math/rand"
import "bytes"
//import "fmt"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER = "Follower"
	CANDIDATE = "Candidate"
	LEADER = "Leader"
	HBINTERVAL = 100 * time.Millisecond
)

//选举超时: 300-500ms
func getRandomElectionTime() time.Duration {
	return (time.Duration(rand.Intn(3))+3)*100*time.Millisecond
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32                 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state   string
	applyCh chan ApplyMsg
	//获得票数
	votes   int

	electionTimer *time.Timer
	pingTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state==LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err!=nil {
		DPrintf("raft %d, encode currentTerm err: %v",rf.currentTerm,err)
	}
	err = e.Encode(rf.votedFor)
	if err!=nil {
		DPrintf("raft %d, encode votedFor err: %v",rf.currentTerm,err)
	}

	err = e.Encode(rf.log)
	if err!=nil {
		DPrintf("raft %d, encode log err: %v",rf.currentTerm,err)
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor    int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log)!=nil {
		DPrintf("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=LEADER {
		return 0,0,false
	}
	rf.log = append(rf.log,LogEntry{Term:rf.currentTerm, Command:command})
	rf.persist()
	index = len(rf.log)-1
	term = rf.log[index].Term
	isLeader = true
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	serversNum := len(rf.peers)
	rf.nextIndex = make([]int,serversNum)
	rf.matchIndex = make([]int,serversNum)
	rf.state = FOLLOWER

	rf.log = append(rf.log, LogEntry{Term:0,Command:nil})
	rf.electionTimer = time.NewTimer(getRandomElectionTime())
	rf.pingTimer = time.NewTimer(HBINTERVAL)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionLoop()
	go rf.applyLoop()
	return rf
}

func (rf *Raft) becomeCandidate() {
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.votes = 1
	DPrintf("raft: %d, term: %d become candidate\n", rf.me,rf.currentTerm)
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
	rf.electionTimer.Reset(getRandomElectionTime())
	DPrintf("raft: %d, term: %d become follower\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	for i:=0; i<len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.pingTimer.Reset(HBINTERVAL)
	go rf.pingLoop()
	DPrintf("raft: %d, term: %d become leader\n", rf.me,rf.currentTerm)
}


func (rf *Raft) electionLoop() {
	for {
		<-rf.electionTimer.C
		rf.electionTimer.Reset(getRandomElectionTime())
		rf.mu.Lock()
		if rf.state==LEADER {
			rf.mu.Unlock()
			continue
		}

		rf.becomeCandidate()
		rf.persist()
		for i:=0;i<len(rf.peers);i++ {
			if i==rf.me {
				continue
			}
			args := &RequestVoteArgs {
				Term:           rf.currentTerm,
				CandidateId:    rf.me,
				LastLogIndex:   len(rf.log)-1,
				LastLogTerm:    rf.log[len(rf.log)-1].Term,
			}

			// fmt.Printf("here1 %d\n",args.Term)
			reply := &RequestVoteReply {
				Term:           0,
				VoteGranted:    false,
			}
			go rf.sendRequestVote(i,args,reply)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) pingLoop() {
	for {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		//append entries to each peer
		for peerId := range rf.peers {
			if peerId==rf.me {
				rf.nextIndex[peerId] = len(rf.log)
				rf.matchIndex[peerId] = len(rf.log)-1
				continue
			}
			prevLogIndex := rf.nextIndex[peerId]-1
			appendEntriesArgs := &AppendEntriesArgs {
				Term:               rf.currentTerm,
				LeaderId:           rf.me,
				PrevLogIndex:       prevLogIndex,
				PrevLogTerm:        rf.log[prevLogIndex].Term,
				Entries:            rf.log[rf.nextIndex[peerId]:],
				LeaderCommit:       rf.commitIndex,
			}
			appendEntriesReply := &AppendEntriesReply {
				Term:		0,
				Success:	false,
			}
			go rf.sendLogEntries(peerId,appendEntriesArgs,appendEntriesReply)
		}
		rf.mu.Unlock()
		<-rf.pingTimer.C
		rf.pingTimer.Reset(HBINTERVAL)
		DPrintf("raft %d start next ping round\n",rf.me)
	}
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(time.Duration(rand.Intn(5)+3)*10*time.Millisecond)
		//rf.mu.Lock()
		DPrintf("raft %d start apply, commitIndex %d, lastApplied %d",rf.me,rf.commitIndex,rf.lastApplied)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.lastApplied,rf.log[rf.lastApplied])
		}
		//rf.mu.Unlock()
	}
}

func (rf *Raft) apply(applyIndex int, entry LogEntry) {
	rf.applyCh <- ApplyMsg{
		CommandValid:   true,
		Command:        entry.Command,
		CommandIndex:   applyIndex,
	}
	DPrintf("raft %d, apply cammand: %d", rf.me, applyIndex)
}