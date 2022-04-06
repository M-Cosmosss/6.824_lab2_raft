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

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	leader = iota
	follower
	candidate
)
const (
	timeOutBase             = 400
	timeOutMaxRange         = 300
	electionTimeOutBase     = 100
	electionTimeOutMaxRange = 300
	rpcTimeOut              = 200 * time.Millisecond
	heartBeatCheck          = 100 * time.Millisecond
	heartBeatRPCTimeout     = 10 * time.Millisecond
	doAppendEntriesTime     = 10 * time.Millisecond
)

func init() {
	fd, err := os.Create("./log/" + time.Now().Format("0102T15_04_05.0000") + ".log")
	if err != nil {
		panic(err)
	}
	log.SetOutput(fd)
	log.SetFlags(log.Lmicroseconds)
}

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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int
	// 当前任期
	currentTerm int
	// 当前任期选票投给了谁，-1代表未投
	votedFor int
	// 当前最新 log 序号
	currentLogIndex int
	// 已提交 log 序号
	commitIndex int
	// 上一个应用的 log 序号
	lastApplied int
	// follower 状态下的心跳超时定时器
	heartBeatTimer *time.Timer
	// 日志
	logs []*LogEntry
	// 通过关闭 channel 来通知该 Raft 实例的所有协程退出
	kill chan struct{}
	// leader 状态下，用于接收统计 follower 日志复制成功信息
	commitCh chan int
	// 应用日志后通过该 channel 通知测试函数
	applyCh chan ApplyMsg
	// leader 状态下，用于统计各 follower 需要同步日志的进度
	nextIndex []int
	// leader 状态下，用于统计各 follower 已经同步日志的进度
	matchIndex []int
}

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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 日志
type LogEntry struct {
	Term    int
	Command interface{}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	if rf.state == leader {
		rf.currentLogIndex++
		rf.logs = append(rf.logs, &LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.logWithoutLock("Start: Index %d,command %d", rf.currentLogIndex, command)
		return rf.currentLogIndex, rf.currentTerm, true
	}
	return -1, -1, false
}

// applyWorker 运行于单独的后台协程中，定期检测 commitIndex 是否大于 lastApplied，若大于则向 applyCh 发送应用日志，并更新前两者
func (rf *Raft) applyWorker() {
	for {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			time.Sleep(doAppendEntriesTime)
			rf.mu.Unlock()
			continue
		}
		if rf.lastApplied+1 > rf.currentLogIndex {
			rf.logWithoutLock("[ERROR] applyWorker: race")
			rf.mu.Unlock()
			time.Sleep(doAppendEntriesTime)
			continue
		}
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[rf.lastApplied + 1].Command,
			CommandIndex:  rf.lastApplied + 1,
			SnapshotValid: false,
		}
		rf.lastApplied++
		rf.logWithoutLock("applyWorker: apply index-%d", rf.lastApplied)
		rf.applyCh <- msg
		rf.mu.Unlock()
	}
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
	close(rf.kill)
	rf.log("killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) log(i ...interface{}) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	str := fmt.Sprintf(i[0].(string), i[1:]...)
	var s string
	switch rf.state {
	case leader:
		s = "leader   "
	case follower:
		s = "follower "
	case candidate:
		s = "candidate"
	}
	log.Printf("Node-%d-%d %s: "+str+"\n", rf.me, rf.currentTerm, s)
}

func (rf *Raft) logWithoutLock(i ...interface{}) {
	str := fmt.Sprintf(i[0].(string), i[1:]...)
	var s string
	switch rf.state {
	case leader:
		s = "leader   "
	case follower:
		s = "follower "
	case candidate:
		s = "candidate"
	}
	log.Printf("Node-%d-%d %s: "+str+"\n", rf.me, rf.currentTerm, s)
}

// updateTerm 更新任期，默认+=1，若入参存在就更新为入参值
// 将投票去向重置为无
func (rf *Raft) updateTerm(i ...int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(i) == 0 {
		rf.currentTerm++
	} else {
		rf.currentTerm = i[0]
	}
	rf.votedFor = -1
}

func (rf *Raft) updateTermWithoutLock(i ...int) {
	if len(i) == 0 {
		rf.currentTerm++
	} else {
		rf.currentTerm = i[0]
	}
	rf.votedFor = -1
}

// changeVotedFor 更新投票去向
func (rf *Raft) changeVotedFor(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = i
}

func (rf *Raft) changeVotedForWithoutLock(i int) {
	rf.votedFor = i
}

// randHeartBeatTime 用于生成随机的心跳超时定时器时间
func (rf *Raft) randHeartBeatTime() time.Duration {
	n := timeOutBase + rand.Intn(timeOutMaxRange)
	rf.logWithoutLock("rand time %d", n)
	return time.Duration(n) * time.Millisecond
}

// randElectionTime 用于生成随机的选举超时定时器时间
func (rf *Raft) randElectionTime() time.Duration {
	n := electionTimeOutBase + rand.Intn(electionTimeOutMaxRange)
	rf.logWithoutLock("rand election time %d", n)
	return time.Duration(n) * time.Millisecond
}

// changeRole 更改 Raft 实例的职责状态
// 当从非 follower 状态变为 follower 状态时，需要重置心跳超时计时器
func (rf *Raft) changeRole(r int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prev := rf.state
	rf.state = r
	if r == follower && prev != follower {
		rf.resetHeartBeatTimer()
	}
}

func (rf *Raft) changeRoleWithoutLock(r int) {
	prev := rf.state
	rf.state = r
	if r == follower && prev != follower {
		rf.resetHeartBeatTimer()
	}
}


func (rf *Raft) updateCommitIndex(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = i
}

// commitWatcher 在 leader 状态下通过 commitCh 统计各 follower 的日志复制成功情况
// 当有过半的 follower 已经复制成功日志时，将该日志视为已提交
// 退出 leader 状态时，重置用于统计的 map
func (rf *Raft) commitWatcher() {
	m := make(map[int]int)
	half := len(rf.peers) / 2
	for {
		select {
		case index := <-rf.commitCh:
			if rf.state != leader {
				m = make(map[int]int)
				rf.log("commitWatcher: reset")
				continue
			}
			if m[index] != -1 {
				m[index]++
				rf.log("commitWatcher: %d++ now %d", index, m[index])
			} else {
				rf.log("commitWatcher: committed %d++", index)
			}
			if m[index] >= half {
				if rf.commitIndex > index {
					m[index] = -1
					rf.log("commitWatcher: commit older %d", index)
					continue
				}
				rf.updateCommitIndex(index)
				m[index] = -1
				rf.log("commitWatcher: commit %d", index)
			}
		case <-rf.kill:
			return
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = follower
	rf.votedFor = -1
	rf.heartBeatTimer = time.NewTimer(rf.randHeartBeatTime())
	rf.kill = make(chan struct{})
	rf.applyCh = applyCh
	rf.commitCh = make(chan int, len(rf.peers))
	rf.currentLogIndex = 0
	rf.commitIndex = 0
	rf.logs = []*LogEntry{&LogEntry{}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.checkHeartBeat()
	go rf.heartBeat()
	go rf.applyWorker()
	go rf.commitWatcher()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}
