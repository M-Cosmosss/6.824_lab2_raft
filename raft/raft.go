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
	"strconv"

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
	rpcTimeOut              = 200
	electionTimeOutBase     = 100
	electionTimeOutMaxRange = 300
	heartBeat               = 100
	doAppendEntriesTime     = 10
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
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           int
	currentTerm     int
	votedFor        int
	currentLogIndex int
	commitIndex     int
	lastApplied     int
	heartBeatTimer  *time.Timer
	logs            []*LogEntry
	kill            chan struct{}
	commitCh        chan int
	applyCh         chan ApplyMsg
	nextIndex       []int
	matchIndex      []int
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.state == candidate {
		if args.Term >= rf.currentTerm {
			rf.state = follower
			rf.updateTerm(args.Term)
			rf.log("AppendEntries: candidate to follower")
		}
	}
	if args.Term > rf.currentTerm {
		rf.log("received bigger Term")
		rf.updateTerm(args.Term)
		rf.state = follower
	}
	if ok := rf.heartBeatTimer.Reset(rf.randHeartBeatTime()); !ok {
		rf.log("Node-" + strconv.Itoa(rf.me) + ":heartBeatTimer reset error")
		//if !rf.heartBeatTimer.Stop() && len(rf.heartBeatTimer.C) > 0 {
		//	<-rf.heartBeatTimer.C
		//}
		//rf.heartBeatTimer.Reset(rf.randHeartBeatTime())
	}
	if args.PrevLogIndex < rf.currentLogIndex {
		rf.log(fmt.Sprintf(fmt.Sprintf("AppendEntries: delete logs: args.PrevLogIndex-%d, rf.currentLogIndex-%d", args.PrevLogIndex, rf.currentLogIndex)))
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.currentLogIndex = args.PrevLogIndex
	}
	if args.PrevLogIndex > rf.currentLogIndex {
		rf.log(fmt.Sprintf("AppendEntries: [Error] args.PrevLogIndex > rf.currentLogIndex.%d %d.", args.PrevLogIndex, rf.currentLogIndex))
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log(fmt.Sprintf("AppendEntries: [Error] rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm.%d %d.", rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm))
		return
	}
	if len(args.Entries) == 0 {
		rf.log(fmt.Sprintf("AppendEntries: Received from leader-%d,reset heartbeat timer", args.LeaderID))
	} else {
		rf.logs = append(rf.logs, args.Entries...)
		rf.currentLogIndex += len(args.Entries)
		rf.log(fmt.Sprintf("AppendEntries: Received from leader-%d,append log.currentLog-%d", args.LeaderID, rf.currentLogIndex))
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.currentLogIndex, args.LeaderCommit)
		rf.log(fmt.Sprintf("AppendEntries: commit update from leader-%d,now %d", args.LeaderID, rf.commitIndex))
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
	for {
		if rf.state != leader {
			rf.log("exit doAppendEntries")
			return
		}
		if rf.currentLogIndex < rf.nextIndex[server] {
			time.Sleep(doAppendEntriesTime * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		end := rf.currentLogIndex
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
			Entries:      rf.logs[rf.nextIndex[server] : end+1],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		if ok := rf.sendAppendEntries(server, args, reply); !ok {
			continue
		}
		if reply.Success {
			rf.log(fmt.Sprintf("doAppendEntries to Node-%d Success %d", server, end))
			rf.nextIndex[server] = end + 1
			rf.matchIndex[server] = end
			if m[end] != 1 {
				rf.commitCh <- end
				m[end] = 1
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
				rf.state = follower
				return
			}
			rf.log(fmt.Sprintf("doAppendEntries to Node-%d Fail %d", server, end))
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.log("sendAppendEntries to Node-" + strconv.Itoa(server))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
		rf.updateTerm(args.Term)
		rf.state = follower
		rf.log(fmt.Sprintf("RequestVote: find bigger term from Node-%d", args.CandidateID))
	}
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateID) || args.Term < rf.currentTerm {
		rf.log(fmt.Sprintf("RPC RequestVote fail:Candidate-%d.VotedFor: %d", args.CandidateID, rf.votedFor))
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm < rf.logs[rf.currentLogIndex].Term || (args.LastLogTerm == rf.logs[rf.currentLogIndex].Term && args.LastLogIndex < rf.currentLogIndex) {
		reply.VoteGranted = false
		rf.log(fmt.Sprintf("RPC RequestVote fail 5.4:Candidate-%d.", args.CandidateID))
		return
	}
	rf.log(fmt.Sprintf("RPC RequestVote success:Candidate-%d.", args.CandidateID))
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID

	rf.heartBeatTimer.Reset(rf.randHeartBeatTime())
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
		rf.log(fmt.Sprintf("Start: Index %d,command %d", rf.currentLogIndex, command))
		return rf.currentLogIndex, rf.currentTerm, true
	} else {
		return -1, -1, false
	}
}

func (rf *Raft) applyWorker() {
	for {
		if rf.commitIndex <= rf.lastApplied {
			time.Sleep(doAppendEntriesTime * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		if rf.lastApplied+1 > rf.currentLogIndex {
			rf.log("[ERROR] applyWorker: race")
			rf.mu.Unlock()
			time.Sleep(doAppendEntriesTime * time.Millisecond)
			continue
			//rf.log(fmt.Sprintf("rf.commitIndex: %d. rf.lastApplied: %d.",rf.commitIndex,rf.lastApplied))
			//panic("")
		}
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[rf.lastApplied+1].Command,
			CommandIndex:  rf.lastApplied + 1,
			SnapshotValid: false,
		}
		rf.lastApplied++
		rf.log(fmt.Sprintf("applyWorker: apply index-%d", rf.lastApplied))
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

func (rf *Raft) startVote() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.log(fmt.Sprintf("start vote"))
	rf.state = candidate
	peerNums := len(rf.peers)
	for {
		timer := time.NewTimer(rpcTimeOut * time.Millisecond)
		acquiredVotes, neededVotes := 1, peerNums/2+1
		rf.updateTerm()
		rf.votedFor = rf.me
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
				if result {
					rf.log(fmt.Sprintf("acquiredVotes++"))
					acquiredVotes++
				}
				if rf.state != candidate {
					rf.log("stop vote for become follower")
					return
				}
				if acquiredVotes >= neededVotes {
					rf.log(fmt.Sprintf("become leader"))
					rf.state = leader
					for i2 := range rf.nextIndex {
						rf.nextIndex[i2] = rf.currentLogIndex + 1
					}
					for i2 := range rf.matchIndex {
						rf.matchIndex[i2] = 0
					}
					rf.broadcastHeartBeat()
					go rf.startAppendEntries()
					return
				}
			case <-timer.C:
				rf.log("startVote TimeOut")
				goto TIMEOUT
			}
		}
	TIMEOUT:
		time.Sleep(rf.randElectionTime())
		rf.mu.Lock()
		if rf.state != candidate {
			rf.log("stop vote for become follower.")
			rf.mu.Unlock()
			return
		}
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
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.currentLogIndex,
		LastLogTerm:  rf.logs[rf.currentLogIndex].Term,
	}
	reply := &RequestVoteReply{}
	rf.log(fmt.Sprintf("doVote to Node-%d", index))
	if ok := rf.sendRequestVote(index, args, reply); !ok {
		rf.log(fmt.Sprintf("RequestVote Error: server %d.", index))
		ch <- false
		return
	}
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.state = follower
		rf.log(fmt.Sprintf("doVote: find bigger term from Node-%d", index))
	}
	rf.log(fmt.Sprintf("RequestVote: server %d %t.", index, reply.VoteGranted))
	ch <- reply.VoteGranted
	return
}

func (rf *Raft) checkHeartBeat() {
	rf.log(fmt.Sprintf("checkHeartBeat start"))
	for {
		for rf.state != follower {
			time.Sleep(heartBeat * time.Millisecond)
		}
		//t := time.NewTimer(time.Second)
		select {
		case <-rf.heartBeatTimer.C:
			if rf.state == leader {
				rf.log("leader heart error")
				panic("")
			}
			rf.log(fmt.Sprintf("HeartBeat timeout"))
			rf.startVote()
		//case <-t.C:
		//	rf.log("HeartBeat Timer Error")
		//	panic("HeartBeat Timer Error")
		case <-rf.kill:
			return
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
		if rf.state == leader {
			rf.broadcastHeartBeat()
		}
		time.Sleep(heartBeat * time.Millisecond)
	}
}
func (rf *Raft) broadcastHeartBeat() {
	rf.log("broadcastHeartBeat Start")

	//ch := make(chan int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, args, reply); !ok {
				rf.log(fmt.Sprintf("HeartBeat to Node-%d rpc Fail", i))
				return
			}
			if !reply.Success {
				rf.log(fmt.Sprintf("HeartBeat to Node-%d Fail,nextIndex--", i))
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}
			}
			return
		}(i)
	}
	rf.log("broadcastHeartBeat Done")
}

func (rf *Raft) log(str string) {
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

func (rf *Raft) updateTerm(i ...int) {
	if len(i) == 0 {
		rf.currentTerm++
	} else {
		rf.currentTerm = i[0]
	}
	rf.votedFor = -1
}

func (rf *Raft) randHeartBeatTime() time.Duration {
	n := timeOutBase + rand.Intn(timeOutMaxRange)
	rf.log(fmt.Sprintf("rand time %d", n))
	return time.Duration(n) * time.Millisecond
}

func (rf *Raft) randElectionTime() time.Duration {
	n := electionTimeOutBase + rand.Intn(electionTimeOutMaxRange)
	rf.log(fmt.Sprintf("rand election time %d", n))
	return time.Duration(n) * time.Millisecond
}

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
				rf.log(fmt.Sprintf("commitWatcher: %d++ now %d", index, m[index]))
			} else {
				rf.log(fmt.Sprintf("commitWatcher: committed %d++", index))
			}
			if m[index] >= half {
				if rf.commitIndex > index {
					m[index] = -1
					rf.log(fmt.Sprintf("commitWatcher: commit older %d", index))
					continue
				}
				rf.commitIndex = index
				m[index] = -1
				rf.log(fmt.Sprintf("commitWatcher: commit %d", index))
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
