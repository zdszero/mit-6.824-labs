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
	MinElectionTimeout = 700
	MaxElectionTimeout = 1500
)

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
	msgCh           chan ApplyMsg // leader
}

func (rf *Raft) resetTimer() {
	duration := time.Duration(MinElectionTimeout + rf.rand.Intn(MaxElectionTimeout-MinElectionTimeout+1))
	rf.timer.Reset(duration * time.Millisecond)
}

func (rf *Raft) stopTimer() {
	rf.timer.Stop()
}

func (rf *Raft) roleText() string {
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
	PrevLogIndex int
	PrevLogTerm  int
	// TODO
	// Entries      []LogEntry
	LeaderCommit int
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
	if args.Term < rf.persister.currentTerm {
		reply.VotedGranted = false
		return
	} else if args.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = args.Term
		rf.persister.votedFor = -1
		if rf.state != Follower {
			role := rf.roleText()
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
			DPrintf("[%d](%s) recv term %d, convert to follower", rf.me, role, reply.Term)
		}
	}
	if rf.persister.votedFor != -1 {
		reply.VotedGranted = false
		return
	}
	// log completeness check
	rf.persister.votedFor = args.CandiateId
	DPrintf("[%d] vote for %d in term %d", rf.me, rf.persister.votedFor, rf.persister.currentTerm)
	reply.VotedGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.stopTimer()
	rf.resetTimer()
	reply.Term = rf.persister.currentTerm
	if args.Term < rf.persister.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = args.Term
		rf.persister.votedFor = -1
		DPrintf("[%d] term(AE) = %d", rf.me, args.Term)
		if rf.state != Follower {
			role := rf.roleText()
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
			DPrintf("[%d](%s) recv term %d, convert to follower", rf.me, role, reply.Term)
		}
	}
	if args.IsHeartbeat {
		if rf.state == Candidate {
			rf.state = Follower
			rf.leaderFoundCh <- struct{}{}
			DPrintf("[%d](candidate) recv heartbeat, convert to follower", rf.me)
		}
	}
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = reply.Term
		rf.persister.votedFor = -1
		if rf.state == Candidate {
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
			DPrintf("[%d](candidate) recv term %d, convert to follower", rf.me, reply.Term)
		}
		return ok
	}
	if reply.VotedGranted == true {
		rf.voteCount++
		if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
			rf.state = Leader
			rf.majorityVotesCh <- struct{}{}
			DPrintf("[%d](condidate) elected as leader", rf.me)
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.persister.currentTerm {
		rf.persister.currentTerm = reply.Term
		rf.persister.votedFor = -1
		if rf.state == Leader {
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
			DPrintf("[%d](leader) convert to follower", rf.me)
		}
	}
	// reply.Success
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	DPrintf("[%d](follower)", rf.me)
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
	rf.state = Candidate
	DPrintf("[%d](follower) timeout, convert to candidate", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) candidateRoutine() {
electAgain:
	rf.mu.Lock()
	rf.persister.currentTerm++
	rf.persister.votedFor = rf.me
	DPrintf("[%d] term(TO) = %d", rf.me, rf.persister.currentTerm)
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
			LastLogIndex: -1, // @TODO
			LastLogTerm:  -1,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(server, args, reply)
	}
	select {
	case <-rf.timer.C:
		DPrintf("[%d](candidate) timeout, elect again", rf.me)
		goto electAgain
	case <-rf.majorityVotesCh:
	case <-rf.leaderFoundCh:
	case <-rf.higherTermCh:
	}
	rf.stopTimer()
}

func (rf *Raft) leaderRoutine() {
	go rf.sendHeartbeats()
	for {
		select {
		case msg := <-rf.msgCh:
			rf.handleMessage(msg)
		case <-rf.higherTermCh:
			break
		}
	}
}

func (rf *Raft) handleMessage(msg ApplyMsg) {
	// @TODO
}

func (rf *Raft) sendHeartbeats() {
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
			// TODO
			args := &AppendEntriesArgs{
				IsHeartbeat: true,
				Term:        rf.persister.currentTerm,
				LeaderId:    rf.me,
			}
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(server, args, reply)
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
	rf.msgCh = make(chan ApplyMsg)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainRoutine()

	return rf
}
