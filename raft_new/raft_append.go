package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.PrevLogIndex = -1
	reply.PrevLogTerm = -1
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.state == candidate {
		if args.Term >= rf.currentTerm {
			rf.mu.Lock()
			rf.changeRoleWithoutLock(follower)
			rf.updateTermWithoutLock(args.Term)
			rf.mu.Unlock()
			rf.log("AppendEntries: candidate to follower")
		}
	}
	if args.Term > rf.currentTerm {
		rf.log("received bigger Term")
		rf.mu.Lock()
		rf.updateTermWithoutLock(args.Term)
		rf.changeRoleWithoutLock(follower)
		rf.mu.Unlock()
	}
	rf.resetHeartBeatTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.PrevLogIndex < rf.currentLogIndex && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logWithoutLock(fmt.Sprintf(fmt.Sprintf("AppendEntries: delete logs: args.PrevLogIndex-%d, rf.currentLogIndex-%d", args.PrevLogIndex, rf.currentLogIndex)))
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.currentLogIndex = args.PrevLogIndex
	}
	if args.PrevLogIndex > rf.currentLogIndex {
		rf.logWithoutLock(fmt.Sprintf("AppendEntries: [Error] args.PrevLogIndex > rf.currentLogIndex.%d %d.", args.PrevLogIndex, rf.currentLogIndex))
		reply.PrevLogIndex = rf.currentLogIndex
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logWithoutLock(fmt.Sprintf("AppendEntries: [Error] rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm.%d %d.", rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm))
		reply.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		return
	}
	if len(args.Entries) == 0 {
		rf.logWithoutLock(fmt.Sprintf("AppendEntries: Received from leader-%d,reset heartbeat timer", args.LeaderID))
	} else {
		if args.PrevLogIndex < rf.currentLogIndex {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			rf.currentLogIndex = args.PrevLogIndex + len(args.Entries)
			rf.logWithoutLock(fmt.Sprintf("AppendEntries1: Received from leader-%d,append log.currentLog-%d", args.LeaderID, rf.currentLogIndex))
		} else if args.PrevLogIndex == rf.currentLogIndex {
			rf.logs = append(rf.logs, args.Entries...)
			rf.currentLogIndex += len(args.Entries)
			rf.logWithoutLock(fmt.Sprintf("AppendEntries2: Received from leader-%d,append log.currentLog-%d", args.LeaderID, rf.currentLogIndex))
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.currentLogIndex, args.LeaderCommit)
		rf.logWithoutLock(fmt.Sprintf("AppendEntries: commit update from leader-%d,now %d\n"+
			"len(args.Entries): %d. args.PrevLogIndex: %d. rf.currentLogIndex: %d. rf.logs[args.PrevLogIndex]-%d. args.PrevLogTerm-%d", args.LeaderID, rf.commitIndex, len(args.Entries), args.PrevLogIndex,
			rf.currentLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm))
	}
	reply.Success = true
	return
}

func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.doAppendEntries(i)
	}
	return
}

func (rf *Raft) doAppendEntries(server int) {
	m := make(map[int]int)
	step := 1
	for {
		rf.mu.RLock()
		if rf.state != leader {
			rf.mu.RUnlock()
			rf.log("exit doAppendEntries")
			return
		}
		if rf.currentLogIndex < rf.nextIndex[server] && rf.commitIndex <= rf.matchIndex[server] {
			//rf.log(fmt.Sprintf("doAppendEntries sleep: %d-rf.commitIndex-%d rf.matchIndex-%d",server,rf.commitIndex , rf.matchIndex[server]))
			rf.mu.RUnlock()
			time.Sleep(doAppendEntriesTime * time.Millisecond)
			continue
		}
		rf.logWithoutLock(fmt.Sprintf("doAppendEntries: %d-rf.commitIndex-%d rf.matchIndex-%d", server, rf.commitIndex, rf.matchIndex[server]))
		end := rf.currentLogIndex
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
			Entries:      rf.logs[rf.nextIndex[server] : end+1],
			LeaderCommit: rf.commitIndex,
		}
		theCommitIndex := rf.commitIndex
		rf.mu.RUnlock()
		reply := &AppendEntriesReply{}

		ch := make(chan bool, 1)
		timer := time.NewTimer(heartBeat * time.Millisecond)
		go rf.sendAppendEntriesWithChannel(server, args, reply, ch)
		select {
		case <-timer.C:
			rf.log(fmt.Sprintf("doAppendEntries to Node-%d rpc Timeout", server))
			continue
		case ok := <-ch:
			if !ok {
				rf.log(fmt.Sprintf("doAppendEntries to Node-%d RPC FAILED", server))
				continue
			}
		}

		if reply.Success {
			rf.mu.RLock()
			rf.logWithoutLock(fmt.Sprintf("doAppendEntries to Node-%d Success %d", server, end))
			rf.nextIndex[server] = end + 1
			rf.matchIndex[server] = min(theCommitIndex, end)
			if m[end] != 1 {
				rf.commitCh <- end
				m[end] = 1
			}
			step = 1
			rf.logWithoutLock(fmt.Sprintf("doAppendEntries: server-%d nextIndex-%d matchIndex-%d ", server, rf.nextIndex[server], rf.matchIndex[server]))
			rf.mu.RUnlock()
			continue
		} else {
			rf.mu.Lock()
			rf.logWithoutLock(fmt.Sprintf("doAppendEntries to Node-%d Fail %d", server, end))
			if reply.Term > rf.currentTerm {
				rf.logWithoutLock(fmt.Sprintf("doAppendEntries to Node-%d find bigger term-%d. currentTerm-%d", server, reply.Term, rf.currentTerm))
				rf.updateTermWithoutLock(reply.Term)
				rf.changeRoleWithoutLock(follower)
				rf.mu.Unlock()
				return
			}
			if reply.PrevLogIndex >= 0 {
				rf.logWithoutLock(fmt.Sprintf("doAppendEntries to Node-%d Fail %d.Set nextIndex from reply", server, end))
				rf.nextIndex[server] = reply.PrevLogIndex + 1
				goto NEXT
			}
			if reply.PrevLogTerm != -1 {
				rf.nextIndex[server] -= step
				if step < 1024 {
					step *= 2
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				rf.logWithoutLock(fmt.Sprintf("doAppendEntries to Node-%d Fail %d.Step-%d. rf.nextIndex[server]-%d", server, end, step, rf.nextIndex[server]))
				goto NEXT
			}
			rf.logWithoutLock("[ERROR] doAppendEntries")
			//panic("[ERROR] doAppendEntries")
			//if rf.nextIndex[server] > 1 {
			//	rf.log(fmt.Sprintf("doAppendEntries to Node-%d Fail %d.Set nextIndex--.", server, end))
			//	rf.nextIndex[server]--
			//}
		}
	NEXT:
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.log("sendAppendEntries to Node-" + strconv.Itoa(server))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriesWithChannel(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
	//rf.log("sendAppendEntries to Node-" + strconv.Itoa(server))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	ch <- ok
}
