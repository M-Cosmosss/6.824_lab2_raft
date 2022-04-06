package raft

import (
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


// RequestVote 请求选票 RPC 函数，满足指定的条件后才能向对方投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 如果请求的任期大于自身，就更新自身任期并重置 VotedFor
	if args.Term > rf.currentTerm {
		rf.updateTermWithoutLock(args.Term)
		rf.changeRoleWithoutLock(follower)
		rf.logWithoutLock("RequestVote: find bigger term from Node-%d", args.CandidateID)
	}
	// 如果自身已经将选票投给了对方以外的人，或者对方的任期低于自己，就返回失败
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateID) || args.Term < rf.currentTerm {
		rf.logWithoutLock("RPC RequestVote fail:Candidate-%d.VotedFor: %d", args.CandidateID, rf.votedFor)
		reply.VoteGranted = false
		return
	}
	// 如果对方的日志落后于自己，就返回失败
	if args.LastLogTerm < rf.logs[rf.currentLogIndex].Term || (args.LastLogTerm == rf.logs[rf.currentLogIndex].Term && args.LastLogIndex < rf.currentLogIndex) {
		reply.VoteGranted = false
		rf.logWithoutLock("RPC RequestVote fail 5.4:Candidate-%d.", args.CandidateID)
		return
	}
	rf.logWithoutLock("RPC RequestVote success:Candidate-%d.\n"+
		"args.LastLogTerm: %d. rf.logs[rf.currentLogIndex].Term: %d. rf.currentLogIndex: %d", args.CandidateID, args.LastLogTerm, rf.logs[rf.currentLogIndex].Term,
		rf.currentLogIndex)
	// 符合条件，投票给对方
	reply.VoteGranted = true
	rf.changeVotedForWithoutLock(args.CandidateID)

	//　需要重置心跳超时计时器，防止刚投票出去自身就计时器触发变为了任期更大的 candidate
	rf.resetHeartBeatTimer()
	return
}

// startVote 开始选举，向所有 Raft 节点请求投票
func (rf *Raft) startVote() {
	rf.log("start vote")

	peerNums := len(rf.peers)
	for {
		timer := time.NewTimer(rpcTimeOut)
		// 已获得的选票与选举成功需要的票数
		acquiredVotes, neededVotes := 1, peerNums/2+1
		ch := make(chan bool, peerNums-1)
		for i := 0; i < peerNums; i++ {
			if i == rf.me {
				continue
			}
			// 起协程向节点 i 发起 RPC 请求投票，用 channel 返回结果
			go rf.doVote(ch, i)
		}
		for i := 0; i < peerNums-1; i++ {
			select {
			case result := <-ch:
				rf.mu.Lock()
				if result {
					rf.logWithoutLock("acquiredVotes++")
					acquiredVotes++
				}
				// 检测到自身不再是 candidate 时退出
				if rf.state != candidate {
					rf.logWithoutLock("stop vote for become follower")
					rf.mu.Unlock()
					return
				}
				if acquiredVotes >= neededVotes {
					rf.logWithoutLock("become leader")
					rf.changeRoleWithoutLock(leader)
					// 初始化每个 follower 对应的 nextIndex 与 matchIndex
					for i2 := range rf.nextIndex {
						rf.nextIndex[i2] = rf.currentLogIndex + 1
					}
					for i2 := range rf.matchIndex {
						rf.matchIndex[i2] = 0
					}
					// 防止出现发起心跳时任期已经改变的情况，使用现在的任期进行心跳广播
					t := rf.currentTerm
					rf.mu.Unlock()
					rf.broadcastHeartBeat(t)
					// 启动日志分发
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
		// 使用随机的选举超时时间休眠，防止多个 candidate 一直竞争选票
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

// doVote 异步地发起请求选票 RPC，通过 channel 返回结果
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
	rf.log("doVote to Node-%d", index)
	if ok := rf.sendRequestVote(index, args, reply); !ok {
		rf.log("RequestVote Error: server %d.", index)
		ch <- false
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTermWithoutLock(reply.Term)
		rf.changeRoleWithoutLock(follower)
		rf.logWithoutLock("doVote: find bigger term from Node-%d", index)
	}
	rf.logWithoutLock("RequestVote: server %d %t.", index, reply.VoteGranted)
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
