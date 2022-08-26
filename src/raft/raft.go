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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labrpc"
)

const (
	Follower           = 0
	Candidate          = 1
	Leader             = 2
	MinElectionTimeout = 500
	MaxElectionTimeout = 1000
)

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// import "bytes"
// import "../labgob"

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
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state           int
	rand            *rand.Rand
	voteCount       int           // candidate
	timer           *time.Timer   // follower, candidate
	higherTermCh    chan struct{} // candidate, leader
	majorityVotesCh chan struct{} // candidate
	leaderFoundCh   chan struct{} // candidate
	applyCh         chan ApplyMsg // leader

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

func (rf *Raft) resetTimer() {
	duration := time.Duration(MinElectionTimeout + rf.rand.Intn(MaxElectionTimeout-MinElectionTimeout+1))
	rf.timer.Reset(duration * time.Millisecond)
}

func (rf *Raft) stopTimer() {
	rf.timer.Stop()
}

func (rf *Raft) role() string {
	var ret string
	switch rf.state {
	case Follower:
		ret = "follower"
	case Candidate:
		ret = "candidate"
	case Leader:
		ret = "leader"
	}
	return ret
}

func (rf *Raft) dprintf(format string, a ...interface{}) {
	var args []interface{}
	args = append(args, rf.me)
	args = append(args, rf.role())
	args = append(args, rf.persister.currentTerm)
	args = append(args, a...)
	DPrintf("[%d](%-9s){%3d} "+format, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.currentTerm, rf.state == Leader
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

// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandiateId   int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

type AppendEntriesArgs struct {
	IsHeartbeat  bool
	Term         int
	LeaderId     int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        LogEntry
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.persister.currentTerm
	reply.VotedGranted = false
	if args.Term < rf.persister.currentTerm {
		rf.dprintf("reject %d: lower term", args.CandiateId)
		return
	} else if args.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = args.Term
		rf.persister.votedFor = -1
		if rf.state != Follower {
			rf.dprintf("recv term %d, convert to follower", args.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
	}
	if rf.persister.votedFor != -1 {
		return
	}
	// log completeness check
	lastLogIndex := len(rf.persister.logs) - 1
	lastLogTerm := rf.persister.logs[len(rf.persister.logs)-1].Term
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		rf.dprintf("reject %d: inconsistent log", args.CandiateId)
		return
	}
	rf.persister.votedFor = args.CandiateId
	if rf.state == Follower {
		rf.stopTimer()
		rf.resetTimer()
	}
	rf.dprintf("grant %d", args.CandiateId)
	reply.VotedGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.stopTimer()
	rf.resetTimer()
	reply.Success = false
	reply.Term = rf.persister.currentTerm
	if args.Term < rf.persister.currentTerm {
		return
	} else if args.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = args.Term
		rf.persister.votedFor = -1
		rf.dprintf("term(AE) = %d", args.Term)
		if rf.state != Follower {
			rf.dprintf("recv term %d, convert to follower", args.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
	}
	if rf.state == Candidate {
		rf.state = Follower
		rf.leaderFoundCh <- struct{}{}
		rf.dprintf("recv heartbeat, convert to follower")
	}
	if args.IsHeartbeat {
		if args.LeaderCommit > rf.commitIndex {
			rf.setCommitIndex(min(args.LeaderCommit, len(rf.persister.logs)-1))
		}
		return
	}
	if args.PrevLogIndex >= 0 {
		lastLogIndex := len(rf.persister.logs) - 1
		if lastLogIndex < args.PrevLogIndex || rf.persister.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
		if lastLogIndex >= args.PrevLogIndex+1 && rf.persister.logs[args.PrevLogIndex+1].Term == args.Entry.Term {
			reply.Success = true
			return
		}
	} else {
		// initial heartbeat
		reply.Success = true
		return
	}
	rf.persister.logs = rf.persister.logs[:args.PrevLogIndex+1]
	rf.persister.logs = append(rf.persister.logs, args.Entry)
	rf.dprintf("APPEND %3d{%3d}: %v", args.Entry.LogIndex, args.Entry.Term, args.Entry.Command)
	rf.matchIndex[args.LeaderId] = args.Entry.LogIndex
	rf.matchIndex[rf.me] = args.Entry.LogIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.persister.logs)
	}
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return false
	}
	if reply.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = reply.Term
		rf.persister.votedFor = -1
		if rf.state == Candidate {
			rf.dprintf("recv term %d, convert to follower", reply.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
		return ok
	}
	if reply.VotedGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.dprintf("elected as leader")
			rf.state = Leader
			rf.majorityVotesCh <- struct{}{}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// rf.dprintf("->[%d](%v) %v: prevTerm %d, prevIndex %d", server, args.Entry.Command, reply.Success, args.PrevLogTerm, args.PrevLogIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return false
	}
	if reply.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = reply.Term
		rf.persister.votedFor = -1
		if rf.state == Leader {
			rf.dprintf("recv term %d, convert to follower", reply.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
		return false
	}
	return ok
}

func (rf *Raft) sendLogEntry(server int, args AppendEntriesArgs, reply AppendEntriesReply) bool {
	nextIndex := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		nextIndex[i] = args.PrevLogIndex + 1
	}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return false
	}
	for !reply.Success {
		nextIndex[server]--
		args.PrevLogIndex = nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
		args.Entry = rf.persister.logs[nextIndex[server]]
		ok = rf.sendAppendEntries(server, &args, &reply)
		// leader become follower
		if !ok {
			return false
		}
	}
	rf.matchIndex[server] = nextIndex[server]
	for nextIndex[server]++; nextIndex[server] < len(rf.persister.logs); nextIndex[server]++ {
		args.PrevLogIndex = nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
		args.Entry = rf.persister.logs[nextIndex[server]]
		ok = rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return false
		}
		if !reply.Success {
			log.Fatalln("Error: AppendEntry RPC fails")
		}
		rf.matchIndex[server] = nextIndex[server]
	}
	rf.dprintf("matchIndex[%d] = %d", server, rf.matchIndex[server])
	if rf.majorityMatched(rf.matchIndex[server]) {
		rf.setCommitIndex(rf.matchIndex[server])
	}
	return ok
}

func (rf *Raft) sendHeartbeat(server int) {
	commitIndex := -1
	if rf.matchIndex[server] >= rf.commitIndex {
		commitIndex = rf.commitIndex
	}
	args := &AppendEntriesArgs{
		IsHeartbeat:  true,
		Term:         rf.persister.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}
	go rf.sendAppendEntries(server, args, reply)
}

func (rf *Raft) majorityMatched(n int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// already committed
	if n < rf.commitIndex {
		return false
	}
	cnt := 0
	for i := 0; i < len(rf.matchIndex); i++ {
		// DPrintf("matchIndex[%d][%d] = %d", rf.me, i, rf.matchIndex[i])
		if rf.matchIndex[i] >= n {
			cnt++
		}
	}
	if cnt > len(rf.peers)/2 {
		return true
	}
	return false
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	if commitIndex <= rf.commitIndex {
		return
	}
	rf.commitIndex = commitIndex
	// rf.dprintf("commitIndex = %d", rf.commitIndex)
}

func (rf *Raft) applyCommands() {
	for {
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.persister.logs[i].Command,
					CommandIndex: i,
				}
				rf.dprintf(" APPLY %3d{%3d}: %v", i, rf.persister.logs[i].Term, msg.Command)
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
		time.Sleep(time.Millisecond * 100)
	}
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
	index := -1
	term := -1
	isLeader := (rf.state == Leader)

	// Your code here (2B).
	if rf.state != Leader {
		return index, term, false
	}
	rf.dprintf(" START %3d{%3d}: %v", len(rf.persister.logs), rf.persister.currentTerm, command)
	entry := LogEntry{
		Term:     rf.persister.currentTerm,
		LogIndex: len(rf.persister.logs),
		Command:  command,
	}
	// for i := 0; i < len(rf.peers); i++ {
	// 	rf.nextIndex[i] = len(rf.persister.logs)
	// }
	rf.persister.logs = append(rf.persister.logs, entry)
	rf.matchIndex[rf.me]++
	// rf.dprintf("matchIndex[%d] = %d", rf.me, rf.matchIndex[rf.me])
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		prevLogIndex := len(rf.persister.logs) - 2
		prevLogTerm := rf.getLogTerm(prevLogIndex)
		args := AppendEntriesArgs{
			IsHeartbeat:  false,
			Term:         rf.persister.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entry:        entry,
		}
		reply := AppendEntriesReply{}
		go rf.sendLogEntry(server, args, reply)
	}
	index = len(rf.persister.logs) - 1
	term = rf.persister.currentTerm
	return index, term, isLeader
}

func (rf *Raft) getLogTerm(logIndex int) int {
	if logIndex < 0 || logIndex >= len(rf.persister.logs) {
		log.Panicf("log index %d is out of range", logIndex)
	}
	if logIndex == 0 {
		return 0
	} else {
		return rf.persister.logs[logIndex].Term
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) mainRoutine() {
	rf.dprintf("")
	go rf.applyCommands()
	for {
		switch rf.state {
		case Follower:
			rf.followerRoutine()
		case Candidate:
			rf.candidateRoutine()
		case Leader:
			rf.leaderRoutine()
		default:
			log.Fatalln("Error: Unknown role type")
		}
	}
}

func (rf *Raft) followerRoutine() {
	rf.mu.Lock()
	rf.resetTimer()
	rf.mu.Unlock()
	<-rf.timer.C
	rf.mu.Lock()
	rf.dprintf("timeoout, convert to candidate")
	rf.state = Candidate
	rf.mu.Unlock()
}

func (rf *Raft) candidateRoutine() {
electAgain:
	rf.mu.Lock()
	rf.persister.currentTerm++
	rf.persister.votedFor = rf.me
	rf.dprintf("term(TO) = %d", rf.persister.currentTerm)
	rf.voteCount = 1
	rf.stopTimer()
	rf.resetTimer()
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.persister.currentTerm,
			CandiateId:   rf.me,
			LastLogIndex: len(rf.persister.logs) - 1,
			LastLogTerm:  rf.persister.logs[len(rf.persister.logs)-1].Term,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(server, args, reply)
	}
	select {
	case <-rf.timer.C:
		rf.dprintf("timeout, elect again")
		goto electAgain
	case <-rf.majorityVotesCh:
	case <-rf.leaderFoundCh:
	case <-rf.higherTermCh:
	}
	rf.stopTimer()
}

func (rf *Raft) leaderRoutine() {
	go rf.notifyFollowers()
	for {
		select {
		case <-rf.higherTermCh:
			break
		}
	}
}

func (rf *Raft) notifyFollowers() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			go rf.sendHeartbeat(server)
		}
		time.Sleep(100 * time.Millisecond)
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
	rf.persister.currentTerm = 0
	rf.persister.votedFor = -1
	s := rand.NewSource(time.Now().UnixNano())
	rf.rand = rand.New(s)
	rf.state = Follower
	rf.voteCount = 0
	rf.timer = time.NewTimer(5 * time.Second)
	rf.stopTimer()
	rf.higherTermCh = make(chan struct{}, 1)
	rf.majorityVotesCh = make(chan struct{}, 1)
	rf.leaderFoundCh = make(chan struct{}, 1)
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.persister.logs = append(rf.persister.logs, LogEntry{
		Term:     0,
		LogIndex: 0,
		Command:  struct{}{},
	})
	for i := 0; i < len(peers); i++ {
		// TODO
		rf.nextIndex[i] = len(rf.persister.logs)
		rf.matchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainRoutine()

	return rf
}
