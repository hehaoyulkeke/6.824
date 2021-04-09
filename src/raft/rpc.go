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
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	DPrintf("%v recv request vote", rf)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
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
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		var i int
		for i=len(rf.log)-1; i>=1 && rf.log[i].Term != args.PrevLogTerm; i-- {

		}
		reply.ConflictIndex = i+1
		reply.Success = false
	} else {
		// delete all conflict entries
		// append new entries
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		reply.Success = true
	}
	DPrintf("%v recv append", rf)
	rf.becomeFollower(args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}