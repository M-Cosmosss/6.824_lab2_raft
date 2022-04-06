package raft

import (
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
	// 遇到任期低于自己的请求直接返回错误
	if args.Term < rf.currentTerm {
		return
	}
	// 在候选人状态收到大于等于当前任期的请求时，更新任期并转为跟随者
	if rf.state == candidate {
		if args.Term >= rf.currentTerm {
			// 更新任期与转换状态必须在加锁状态一起完成，不然会有竞争问题
			rf.mu.Lock()
			rf.changeRoleWithoutLock(follower)
			rf.updateTermWithoutLock(args.Term)
			rf.mu.Unlock()
			rf.log("AppendEntries: candidate to follower")
		}
	}
	// 收到任期大于自身的请求时，直接更新任期并转换为跟随者
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
	// 检测到 leader 日志与本地不一致时，删除后续所有日志
	if args.PrevLogIndex < rf.currentLogIndex && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logWithoutLock("AppendEntries: delete logs: args.PrevLogIndex-%d, rf.currentLogIndex-%d", args.PrevLogIndex, rf.currentLogIndex)
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.currentLogIndex = args.PrevLogIndex
	}
	// leader 发送的日志序号大于自身最新的日志序号时，返回失败并附带自身最新的日志序号
	if args.PrevLogIndex > rf.currentLogIndex {
		rf.logWithoutLock("AppendEntries: [Error] args.PrevLogIndex > rf.currentLogIndex.%d %d.", args.PrevLogIndex, rf.currentLogIndex)
		reply.PrevLogIndex = rf.currentLogIndex
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 检测到 leader 末尾日志与本地不一致时，返回失败
		rf.logWithoutLock("AppendEntries: [Error] rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm.%d %d.", rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		return
	}
	if len(args.Entries) == 0 {
		// 不附带日志。判断为心跳
		rf.logWithoutLock("AppendEntries: Received from leader-%d,reset heartbeat timer", args.LeaderID)
	} else {
		if args.PrevLogIndex < rf.currentLogIndex {
			// leader 发送的日志序号小于自身最新的日志序号时，将本地日志从该序号开始的替换
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			rf.currentLogIndex = args.PrevLogIndex + len(args.Entries)
			rf.logWithoutLock("AppendEntries1: Received from leader-%d,append log.currentLog-%d", args.LeaderID, rf.currentLogIndex)
		} else if args.PrevLogIndex == rf.currentLogIndex {
			// leader 发送的日志序号等于自身最新的日志序号时
			rf.logs = append(rf.logs, args.Entries...)
			rf.currentLogIndex += len(args.Entries)
			rf.logWithoutLock("AppendEntries2: Received from leader-%d,append log.currentLog-%d", args.LeaderID, rf.currentLogIndex)
		}
	}
	// 根据 leader 的 commitIndex 更新自身的
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.currentLogIndex, args.LeaderCommit)
		rf.logWithoutLock("AppendEntries: commit update from leader-%d,now %d\n"+
			"len(args.Entries): %d. args.PrevLogIndex: %d. rf.currentLogIndex: %d. rf.logs[args.PrevLogIndex]-%d. args.PrevLogTerm-%d", args.LeaderID, rf.commitIndex, len(args.Entries), args.PrevLogIndex,
			rf.currentLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
	}
	reply.Success = true
	return
}

// startAppendEntries 为每个 follower 分别起一个单独的协程执行 doAppendEntries 进行日志复制分发
func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.doAppendEntries(i)
	}
	return
}

// doAppendEntries 对编号为入参 server 的 follower 进行持续的日志复制分发工作
// 循环检测 currentLogIndex 与 commitIndex 来判断是否需要向 follower 更新
// 为了加速 follower 日志差异过大的日志同步速度，日志序号的回退速度是指数级
// 检测到不再是 leader 时，退出
func (rf *Raft) doAppendEntries(server int) {
	// map 用于记录已经向 commitCh 发送过的日志序号
	m := make(map[int]int)
	// 返回失败时，重试前回退日志序号的步进
	step := 1
	for {
		rf.mu.RLock()
		if rf.state != leader {
			rf.mu.RUnlock()
			rf.log("exit doAppendEntries")
			return
		}
		// 判断是否需要进行日志复制操作，不需要就解锁并休眠
		if rf.currentLogIndex < rf.nextIndex[server] && rf.commitIndex <= rf.matchIndex[server] {
			rf.mu.RUnlock()
			time.Sleep(doAppendEntriesTime)
			continue
		}
		rf.logWithoutLock("doAppendEntries: %d-rf.commitIndex-%d rf.matchIndex-%d", server, rf.commitIndex, rf.matchIndex[server])
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
		timer := time.NewTimer(heartBeatCheck)
		go rf.sendAppendEntriesWithChannel(server, args, reply, ch)
		select {
		case <-timer.C:
			rf.log("doAppendEntries to Node-%d rpc Timeout", server)
			continue
		case ok := <-ch:
			if !ok {
				// 如果 RPC 发生错误就重试
				rf.log("doAppendEntries to Node-%d RPC FAILED", server)
				continue
			}
		}

		if reply.Success {
			rf.mu.RLock()
			rf.logWithoutLock("doAppendEntries to Node-%d Success %d", server, end)
			rf.nextIndex[server] = end + 1
			rf.matchIndex[server] = min(theCommitIndex, end)
			// 如果该日志序号还未向 commitCh 发送过就发送
			if m[end] != 1 {
				rf.commitCh <- end
				m[end] = 1
			}
			// 请求成功时重置步进
			step = 1
			rf.logWithoutLock("doAppendEntries: server-%d nextIndex-%d matchIndex-%d ", server, rf.nextIndex[server], rf.matchIndex[server])
			rf.mu.RUnlock()
			continue
		} else {
			rf.mu.Lock()
			rf.logWithoutLock("doAppendEntries to Node-%d Fail %d", server, end)
			// 如果对方的任期大于自己，那更新任期并转为 follower 然后退出
			if reply.Term > rf.currentTerm {
				rf.logWithoutLock("doAppendEntries to Node-%d find bigger term-%d. currentTerm-%d", server, reply.Term, rf.currentTerm)
				rf.updateTermWithoutLock(reply.Term)
				rf.changeRoleWithoutLock(follower)
				rf.mu.Unlock()
				return
			}
			// 如果请求失败并且返回体带着 follower 的最新日志序号，就直接将 nextIndex 回退到此
			if reply.PrevLogIndex >= 0 {
				rf.logWithoutLock("doAppendEntries to Node-%d Fail %d.Set nextIndex from reply", server, end)
				rf.nextIndex[server] = reply.PrevLogIndex + 1
				goto NEXT
			}
			// 如果请求失败且不符合上面的条件，就开始以指数速度回退日志直到成功
			if reply.PrevLogTerm != -1 {
				rf.nextIndex[server] -= step
				if step < 1024 {
					step *= 2
				}
				// 日志序号最低不能低于1
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				rf.logWithoutLock("doAppendEntries to Node-%d Fail %d.Step-%d. rf.nextIndex[server]-%d", server, end, step, rf.nextIndex[server])
				goto NEXT
			}
		}
	NEXT:
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriesWithChannel(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	ch <- ok
}
