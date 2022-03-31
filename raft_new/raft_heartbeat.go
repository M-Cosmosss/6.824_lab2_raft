package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) checkHeartBeat() {
	rf.log(fmt.Sprintf("checkHeartBeat start"))
	for {
		rf.mu.RLock()
		for rf.state != follower {
			rf.mu.RUnlock()
			time.Sleep(heartBeat * time.Millisecond)
			rf.mu.RLock()
		}
		//rf.mu.RUnlock()
		t := time.NewTimer(time.Millisecond * 10)
		//rf.mu.RLock()
		select {
		case <-rf.heartBeatTimer.C:
			if rf.state == leader {
				rf.logWithoutLock("leader heart error")
				panic("")
			}
			rf.changeRoleWithoutLock(candidate)
			rf.updateTermWithoutLock()
			rf.changeVotedForWithoutLock(rf.me)
			rf.logWithoutLock(fmt.Sprintf("HeartBeat timeout"))
			rf.mu.RUnlock()
			rf.startVote()
		case <-t.C:
			//rf.logWithoutLock("HeartBeat Timer running")
			rf.mu.RUnlock()
		case <-rf.kill:
			rf.mu.RUnlock()
			return
			//default:
			//	rf.log(fmt.Sprintf("HeartBeat running"))
			//	time.Sleep(heartBeat * time.Millisecond)
		}
		//return
		//rf.heartBeatTimer = time.NewTimer(randHeartBeatTime())
	}
}

func (rf *Raft) heartBeat() {
	for {
		select {
		case <-rf.kill:
			return
		default:
		}
		time.Sleep(heartBeat * time.Millisecond)
		rf.mu.RLock()
		if rf.state == leader {
			rf.mu.RUnlock()
			rf.broadcastHeartBeat(rf.currentTerm)
			continue
		}
		rf.mu.RUnlock()
	}
}
func (rf *Raft) broadcastHeartBeat(t int) {
	rf.log("broadcastHeartBeat Start")

	//ch := make(chan int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int, t int) {
			rf.mu.RLock()
			ch := make(chan bool, 1)
			timer := time.NewTimer(heartBeat * time.Millisecond)
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

func (rf *Raft) resetHeartBeatTimer() {
	if ok := rf.heartBeatTimer.Reset(rf.randHeartBeatTime()); !ok {
		if len(rf.heartBeatTimer.C) > 0 {
			<-rf.heartBeatTimer.C
		}
	}
	rf.logWithoutLock("resetHeartBeatTimer")
}
