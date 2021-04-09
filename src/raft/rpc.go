package raft

import "sort"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateId     int
	LastLogIndex    int
	LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	VoteGranted bool
}

//日志是否比当前的新
func upToDate(currTerm int, currIndex int, dstTerm int, dstIndex int) bool {
	if currTerm!=dstTerm {
		return dstTerm>currTerm
	}
	return dstIndex>=currIndex
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm>args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//fmt.Println("here??")
	//无论什么状态，立即变成FOLLOWER
	if rf.currentTerm<args.Term {
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	//fmt.Printf("raft:%d, votedFor:%d\n",rf.me, rf.votedFor)
	//fmt.Printf("currTerm:%d, currIndex:%d, argsTerm:%d, argsIndex:%d\n",rf.currentTerm,len(rf.log)-1,args.LastLogTerm,args.LastLogIndex)
	if (rf.votedFor==-1 || rf.votedFor==args.CandidateId) && upToDate(rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex){
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	return
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogTerm     int
	PrevLogIndex    int
	Entries         []LogEntry
	LeaderCommit    int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
}

func minInt(a, b int) int {
	if a<b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term<rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//收到来自LEADER的信息，有两种情况：
	//1.当前是CANDIDATE，变成FOLLOWER
	//2.当前是FOLLOWER，重置计时
	rf.becomeFollower(args.Term)
	rf.persist()
	//与PrevLogIndex,PrevLogTerm不匹配，删除PrevLogIndex之后的log
	if args.PrevLogIndex>=len(rf.log) || rf.log[args.PrevLogIndex].Term!=args.PrevLogTerm {
		if args.PrevLogIndex < len(rf.log) {
			rf.log = rf.log[0:args.PrevLogIndex]
			rf.persist()
		}
		return
	}
	rf.log = append(rf.log[0:args.PrevLogIndex+1],args.Entries...)
	rf.persist()
	if args.LeaderCommit>rf.commitIndex {
		//prevIndex := rf.commitIndex
		rf.commitIndex = minInt(args.LeaderCommit,len(rf.log)-1)
	}
	DPrintf("raft %d, log len:%d, commitIndex:%d,lastAppliedIndex:%d",rf.me,len(rf.log),rf.commitIndex,rf.lastApplied)
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", *args, reply)
	//fmt.Printf("server %d, voteReply: %t\n", server,reply.VoteGranted)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term>rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
	}
	//状态已经发生变化，直接返回
	if rf.state != CANDIDATE || rf.currentTerm!=args.Term {
		return ok
	}
	if ok==false {
		return ok
	}
	if reply.VoteGranted==true {
		rf.votes++
		if rf.votes>len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
	return ok
}

func (rf *Raft) sendLogEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries",*args,reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
	}
	//状态发生变化，直接返回
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return ok
	}

	id := server
	if reply.Success {
		rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries) // do not depend on len(rf.log)
		rf.nextIndex[id] = rf.matchIndex[id] + 1
		DPrintf("raft %d, matchIndex %+v",rf.me,rf.matchIndex)

		majorityIndex := getMajoritySameIndex(rf.matchIndex)
		if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
			rf.commitIndex = majorityIndex
			DPrintf("raft %d advance commit index to %d\n", rf.me, rf.commitIndex)
		}
	} else {
		prevIndex := args.PrevLogIndex
		for prevIndex > 0 && rf.log[prevIndex].Term == args.PrevLogTerm {
			prevIndex--
		}
		rf.nextIndex[id] = prevIndex + 1
	}
	return ok
}

func getMajoritySameIndex(matchIndex []int) int {
	tmp := make([]int, len(matchIndex))
	copy(tmp, matchIndex)

	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))

	idx := len(tmp) / 2
	return tmp[idx]
}