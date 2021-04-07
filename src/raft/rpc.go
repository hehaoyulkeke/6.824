package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == None || rf.votedFor == args.CandidateId) && rf.isLogUp2Date(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.becomeFollower(args.Term)
	DPrintf("%v recv request vote", rf)
}

func (rf *Raft) isLogUp2Date(term int, index int) bool {
	lastLogEntry := rf.log[len(rf.log)-1]
	if lastLogEntry.Term != term {
		return term > lastLogEntry.Term
	} else {
		return index >= lastLogEntry.Index
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// delete all conflict entries
	var i int
	for i=args.PrevLogIndex+1; i<len(rf.log); i++ {
		if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			break
		}
	}
	rf.log = rf.log[min(i+1, len(rf.log)-1):]
	// append new entries
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Success = true
	DPrintf("%v recv append", rf)
	rf.becomeFollower(args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}