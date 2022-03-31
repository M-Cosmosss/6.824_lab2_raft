package raft

import (
	"fmt"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
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
	if args.Term > rf.currentTerm {
		rf.updateTermWithoutLock(args.Term)
		rf.changeRoleWithoutLock(follower)
		rf.logWithoutLock(fmt.Sprintf("RequestVote: find bigger term from Node-%d", args.CandidateID))
	}
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateID) || args.Term < rf.currentTerm {
		rf.logWithoutLock(fmt.Sprintf("RPC RequestVote fail:Candidate-%d.VotedFor: %d", args.CandidateID, rf.votedFor))
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm < rf.logs[rf.currentLogIndex].Term || (args.LastLogTerm == rf.logs[rf.currentLogIndex].Term && args.LastLogIndex < rf.currentLogIndex) {
		reply.VoteGranted = false
		rf.logWithoutLock(fmt.Sprintf("RPC RequestVote fail 5.4:Candidate-%d.", args.CandidateID))
		return
	}
	rf.logWithoutLock(fmt.Sprintf("RPC RequestVote success:Candidate-%d.\n"+
		"args.LastLogTerm: %d. rf.logs[rf.currentLogIndex].Term: %d. rf.currentLogIndex: %d", args.CandidateID, args.LastLogTerm, rf.logs[rf.currentLogIndex].Term,
		rf.currentLogIndex))
	reply.VoteGranted = true
	rf.changeVotedForWithoutLock(args.CandidateID)

	rf.resetHeartBeatTimer()
	return
}

func (rf *Raft) startVote() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.log(fmt.Sprintf("start vote"))

	peerNums := len(rf.peers)
	for {
		timer := time.NewTimer(rpcTimeOut * time.Millisecond)
		acquiredVotes, neededVotes := 1, peerNums/2+1
		ch := make(chan bool, peerNums-1)
		for i := 0; i < peerNums; i++ {
			if i == rf.me {
				continue
			}
			go rf.doVote(ch, i)
		}
		for i := 0; i < peerNums-1; i++ {
			select {
			case result := <-ch:
				rf.mu.Lock()
				if result {
					rf.logWithoutLock(fmt.Sprintf("acquiredVotes++"))
					acquiredVotes++
				}
				if rf.state != candidate {
					rf.logWithoutLock("stop vote for become follower")
					rf.mu.Unlock()
					return
				}
				if acquiredVotes >= neededVotes {
					rf.logWithoutLock(fmt.Sprintf("become leader"))
					rf.changeRoleWithoutLock(leader)
					for i2 := range rf.nextIndex {
						//rf.nextIndex[i2] = rf.commitIndex + 1
						rf.nextIndex[i2] = rf.currentLogIndex + 1
					}
					for i2 := range rf.matchIndex {
						rf.matchIndex[i2] = 0
					}
					t := rf.currentTerm
					rf.mu.Unlock()
					rf.broadcastHeartBeat(t)
					go rf.startAppendEntries()
					return
				}
				rf.mu.Unlock()
			case <-timer.C:
				rf.logWithoutLock("startVote TimeOut")
				goto TIMEOUT
			}
		}
	TIMEOUT:
		time.Sleep(rf.randElectionTime())
		rf.mu.Lock()
		if rf.state != candidate {
			rf.logWithoutLock("stop vote for become follower.")
			rf.mu.Unlock()
			return
		}
		rf.updateTermWithoutLock()
		rf.changeVotedForWithoutLock(rf.me)
		rf.mu.Unlock()
		rf.log("vote again")
		select {
		case <-rf.kill:
			return
		default:
		}
	}
}

func (rf *Raft) doVote(ch chan bool, index int) {
	rf.mu.RLock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.currentLogIndex,
		LastLogTerm:  rf.logs[rf.currentLogIndex].Term,
	}
	rf.mu.RUnlock()
	reply := &RequestVoteReply{}
	rf.log(fmt.Sprintf("doVote to Node-%d", index))
	if ok := rf.sendRequestVote(index, args, reply); !ok {
		rf.log(fmt.Sprintf("RequestVote Error: server %d.", index))
		ch <- false
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTermWithoutLock(reply.Term)
		rf.changeRoleWithoutLock(follower)
		rf.logWithoutLock(fmt.Sprintf("doVote: find bigger term from Node-%d", index))
	}
	rf.logWithoutLock(fmt.Sprintf("RequestVote: server %d %t.", index, reply.VoteGranted))
	ch <- reply.VoteGranted
	rf.mu.Unlock()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
