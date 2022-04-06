package raft

import (
	"fmt"
	"time"
)

// checkHeartBeat 处于 follower 状态时，检测 leader 是否长时间无发送心跳
// 认为 leader 离线时发起选举
func (rf *Raft) checkHeartBeat() {
	rf.log("checkHeartBeat start")
	for {
		rf.mu.RLock()
		for rf.state != follower {
			rf.mu.RUnlock()
			time.Sleep(heartBeatCheck)
			rf.mu.RLock()
		}
		t := time.NewTimer(heartBeatRPCTimeout)
		select {
		case <-rf.heartBeatTimer.C:
			if rf.state == leader {
				rf.logWithoutLock("leader heart error")
				panic("leader heart error")
			}
			rf.changeRoleWithoutLock(candidate)
			rf.updateTermWithoutLock()
			rf.changeVotedForWithoutLock(rf.me)
			rf.logWithoutLock("HeartBeat timeout")
			rf.mu.RUnlock()
			rf.startVote()
		case <-t.C:
			rf.mu.RUnlock()
		case <-rf.kill:
			rf.mu.RUnlock()
			return
		}
	}
}

// heartBeat 处于 leader 状态时，定期向所有 follower 发送心跳
func (rf *Raft) heartBeat() {
	for {
		select {
		case <-rf.kill:
			return
		default:
		}
		time.Sleep(heartBeatCheck)
		rf.mu.RLock()
		if rf.state == leader {
			rf.mu.RUnlock()
			rf.broadcastHeartBeat(rf.currentTerm)
			continue
		}
		rf.mu.RUnlock()
	}
}

// broadcastHeartBeat 向所有 follower 发送心跳
func (rf *Raft) broadcastHeartBeat(t int) {
	rf.log("broadcastHeartBeat Start")

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int, t int) {
			rf.mu.RLock()
			ch := make(chan bool, 1)
			timer := time.NewTimer(heartBeatCheck)
			if t != rf.currentTerm {
				rf.logWithoutLock("broadcast cancel")
				rf.mu.RUnlock()
				return
			}
			args := &AppendEntriesArgs{
				Term:         t,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Entries:      nil,
				LeaderCommit: -1,
			}
			reply := &AppendEntriesReply{}
			rf.mu.RUnlock()
			go rf.sendAppendEntriesWithChannel(i, args, reply, ch)
			select {
			case <-timer.C:
				rf.log(fmt.Sprintf("HeartBeat to Node-%d rpc Timeout", i))
				return
			case ok := <-ch:
				if !ok {
					rf.log(fmt.Sprintf("HeartBeat to Node-%d rpc Fail", i))
					return
				}
			}
			if !reply.Success && reply.Term <= t {
				rf.log(fmt.Sprintf("HeartBeat to Node-%d Fail,nextIndex--", i))
				rf.mu.Lock()
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}
				rf.mu.Unlock()
			}
			return
		}(i, t)
	}
	rf.log("broadcastHeartBeat Done")
}

// resetHeartBeatTimer 重置心跳超时定时器
func (rf *Raft) resetHeartBeatTimer() {
	if ok := rf.heartBeatTimer.Reset(rf.randHeartBeatTime()); !ok {
		// 如果定时器已经触发，并且定时器管道中存在未被读取的信号，就将其移除
		if len(rf.heartBeatTimer.C) > 0 {
			<-rf.heartBeatTimer.C
		}
	}
	rf.logWithoutLock("resetHeartBeatTimer")
}
