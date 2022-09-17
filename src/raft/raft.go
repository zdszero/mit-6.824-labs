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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
)

const (
	// state
	Follower  = 0
	Candidate = 1
	Leader    = 2
	// election settings
	MinElectionTimeout = 500
	MaxElectionTimeout = 1000
	// RPC return status
	RpcSucceed          = 0
	RpcFailed           = 1
	RpcHighTerm         = 2
	RpcAssumptionFailed = 3
)

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// import "bytes"
// import "mit-6.824/labgob"

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
	// log entry
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// snapshot
	SnapshotValid bool
	SnapshotData  []byte
	SnapshotIndex int
	SnapshotTerm  int
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
	killCh          chan struct{}

	// persistent
	CurrentTerm       int
	VotedFor          int
	Logs              []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int

	// volatitle
	leaderId    int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	cond        *sync.Cond
}

func (rf *Raft) GetId() int { return rf.me }

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) resetTimer() {
	duration := time.Duration(MinElectionTimeout + rf.rand.Intn(MaxElectionTimeout-MinElectionTimeout+1))
	rf.timer.Reset(duration * time.Millisecond)
}

func (rf *Raft) stopTimer() {
	rf.timer.Stop()
}

func (rf *Raft) stateText() string {
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
	args = append(args, rf.stateText())
	args = append(args, rf.CurrentTerm)
	args = append(args, a...)
	DPrintf("[%d](%-9s){%3d} "+format, args...)
}

func (rf *Raft) getLastLogIndex() int {
	return rf.LastIncludedIndex + len(rf.Logs)
}

func (rf *Raft) getLogTerm(logIndex int) int {
	if logIndex == rf.LastIncludedIndex {
		return rf.LastIncludedTerm
	}
	firstLogIndex := rf.LastIncludedIndex + 1
	lastLogIndex := rf.LastIncludedIndex + len(rf.Logs)
	if logIndex < firstLogIndex || logIndex > lastLogIndex {
		log.Panicf("log index %d is out of range [%d, %d]", logIndex, rf.LastIncludedIndex, lastLogIndex)
	}
	return rf.Logs[logIndex-firstLogIndex].Term
}

func (rf *Raft) inLog(logIndex int) bool {
	firstLogIndex := rf.LastIncludedIndex + 1
	lastLogIndex := rf.LastIncludedIndex + len(rf.Logs)
	return logIndex >= firstLogIndex && logIndex <= lastLogIndex
}

func (rf *Raft) getLogCommand(logIndex int) interface{} {
	firstLogIndex := rf.LastIncludedIndex + 1
	lastLogIndex := rf.LastIncludedIndex + len(rf.Logs)
	if logIndex < firstLogIndex || logIndex > lastLogIndex {
		log.Panicf("log index %d is out of range [%d, %d]", logIndex, rf.LastIncludedIndex, lastLogIndex)
	}
	return rf.Logs[logIndex-firstLogIndex].Command
}

// discard all log entries > index
func (rf *Raft) discardLogAfter(index int) {
	if index < rf.LastIncludedIndex {
		log.Panicf("discard log before index %d, last included index = %d", index, rf.LastIncludedIndex)
	}
	rf.Logs = rf.Logs[:index-rf.LastIncludedIndex]
	rf.dprintf("discard all records after %d", index-rf.LastIncludedIndex-1)
}

// get all log entries > index
func (rf *Raft) getLogAfter(index int) []LogEntry {
	if index < rf.LastIncludedIndex {
		log.Panicf("get log after index %d, last included index = %d", index, rf.LastIncludedIndex)
	}
	return rf.Logs[index-rf.LastIncludedIndex:]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.CurrentTerm); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.VotedFor); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.Logs); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.LastIncludedIndex); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.LastIncludedTerm); err != nil {
		log.Fatal("encode error:", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.CurrentTerm); err != nil {
		log.Fatal("encode error:", err)
	}

	if err := e.Encode(rf.VotedFor); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.Logs); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.LastIncludedIndex); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(rf.LastIncludedTerm); err != nil {
		log.Fatal("encode error:", err)
	}
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		log.Fatal("decode error:", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatal("decode error:", err)
	}
	if err := d.Decode(&logs); err != nil {
		log.Fatal("decode error:", err)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		log.Fatal("decode error:", err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		log.Fatal("decode error:", err)
	}
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Logs = logs
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
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
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Success bool
	Term    int

	// fast backup
	XTerm  int
	XIndex int
	XLen   int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.VotedGranted = false
	if args.Term < rf.CurrentTerm {
		rf.dprintf("reject %d: lower term", args.CandiateId)
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state != Follower {
			rf.dprintf("recv term %d, convert to follower", args.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
	}
	if rf.VotedFor != -1 {
		return
	}
	// log completeness check
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		rf.dprintf("reject %d: inconsistent log", args.CandiateId)
		return
	}
	rf.VotedFor = args.CandiateId
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
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
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
		rf.leaderId = args.LeaderId
		if args.LeaderCommit > rf.commitIndex {
			rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
			rf.dprintf("commitIndex(heartbeat) = %d", rf.commitIndex)
		}
		return
	}
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = true
		return
	}
	if args.PrevLogIndex >= 0 {
		lastLogIndex := rf.getLastLogIndex()
		// if log is shorter than leader's
		if lastLogIndex < args.PrevLogIndex {
			reply.XTerm = -1
			reply.XLen = lastLogIndex + 1
			return
		}
		// lastLogIndex >= PrevLogIndex
		if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			xterm := rf.getLogTerm(args.PrevLogIndex)
			reply.XTerm = xterm
			xindex := args.PrevLogIndex
			for rf.getLogTerm(xindex) == xterm && xindex > rf.LastIncludedIndex {
				xindex--
			}
			// xindex = the first index with xterm OR LastIncludedIndex+1
			xindex++
			reply.XIndex = xindex
			return
		}

		// a1: new entries
		// a2: old entries after prevLogIndex
		isPrefix := func(a1 []LogEntry, a2 []LogEntry, diff_index *int) bool {
			min_len := min(len(a1), len(a2))
			for i := 0; i < min_len; i++ {
				if a1[i].LogIndex == a2[i].LogIndex && a1[i].Term == a2[i].Term {
					continue
				} else {
					*diff_index = i
					return false
				}
			}
			if len(a1) <= len(a2) {
				return true
			}
			*diff_index = len(a2)
			return false
		}
		diff_index := -1
		if isPrefix(args.Entries, rf.getLogAfter(args.PrevLogIndex), &diff_index) {
			reply.Success = true
			return
		}
		// nextIndex = args.PrevLogIndex+1
		absDiffIndex := args.PrevLogIndex + 1 + diff_index
		if absDiffIndex <= lastLogIndex {
			rf.discardLogAfter(absDiffIndex - 1)
		}
		rf.Logs = append(rf.Logs, args.Entries[diff_index:]...)
		for _, e := range args.Entries {
			rf.dprintf("APPEND %3d{%3d}: %v", e.LogIndex, rf.CurrentTerm, e.Command)
		}
		rf.persist()
		reply.Success = true
	} else {
		// initial heartbeat
		reply.Success = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) int {
	// state might change when goroutine is really executed
	rf.mu.Lock()
	if rf.state != Candidate || rf.CurrentTerm != args.Term || rf.killed() {
		rf.mu.Unlock()
		return RpcAssumptionFailed
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return RpcFailed
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// recheck all assumptions
	// - still a candidate
	// - term hasn't changed in RequestVote RPC call
	// - not killed
	if rf.state != Candidate || rf.CurrentTerm != args.Term || rf.killed() {
		return RpcAssumptionFailed
	}
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state == Candidate {
			rf.dprintf("recv term %d, convert to follower", reply.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
		return RpcHighTerm
	}
	if reply.VotedGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.dprintf("elected as leader")
			rf.state = Leader
			rf.majorityVotesCh <- struct{}{}
		}
	}
	return RpcSucceed
}

// return false when:
// - RPC failed
// - assumption failed
// - higher term
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	rf.mu.Lock()
	if rf.state != Leader || rf.CurrentTerm != args.Term || rf.killed() {
		rf.mu.Unlock()
		return RpcAssumptionFailed
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return RpcFailed
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// recheck assumptions
	if rf.state != Leader || rf.CurrentTerm != args.Term || rf.killed() {
		return RpcAssumptionFailed
	}
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state == Leader {
			rf.dprintf("recv term %d, convert to follower", reply.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
		return RpcHighTerm
	}
	return RpcSucceed
}

func (rf *Raft) replicateLog(server int) bool {
	rf.nextIndex[server] = rf.getLastLogIndex()
	rf.mu.Lock()
	if !rf.inLog(rf.nextIndex[server]) {
		rf.mu.Unlock()
		return rf.sendSnapshot(server) == RpcSucceed
	}
	prevLogIndex := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		IsHeartbeat:  false,
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogTerm(prevLogIndex),
		Entries:      rf.getLogAfter(prevLogIndex),
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	ret := rf.sendAppendEntries(server, args, reply)
	if ret != RpcSucceed {
		return false
	}
	for !reply.Success {
		if reply.XTerm == -1 && reply.XLen > 0 {
			rf.nextIndex[server] = reply.XLen
		} else {
			rf.mu.Lock()
			if !(rf.inLog(reply.XIndex) || reply.XIndex == rf.LastIncludedIndex) {
				rf.mu.Unlock()
				return rf.sendSnapshot(server) == RpcSucceed
			}
			rf.mu.Unlock()
			if rf.getLogTerm(reply.XIndex) == reply.XTerm {
				rf.nextIndex[server] = reply.XIndex + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
		// nextIndex > LastIncludedIndex
		rf.mu.Lock()
		if !rf.inLog(rf.nextIndex[server]) {
			rf.mu.Unlock()
			return rf.sendSnapshot(server) == RpcSucceed
		}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
		args.Entries = rf.getLogAfter(args.PrevLogIndex)
		rf.mu.Unlock()
		ret := rf.sendAppendEntries(server, args, reply)
		if ret != RpcSucceed {
			return false
		}
	}
	rf.matchIndex[server] = args.PrevLogIndex
	rf.matchIndex[server] += len(args.Entries)
	match := rf.matchIndex[server]
	// rf.dprintf("matchIndex[%d] = %d", server, rf.matchIndex[server])
	if rf.majorityMatched(match) && rf.getLogTerm(match) == rf.CurrentTerm {
		rf.setCommitIndex(match)
		rf.dprintf("commitIndex(majority) = %d", rf.commitIndex)
	}
	return true
}

func (rf *Raft) replicateLogLoop(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		rf.cond.L.Lock()
		for rf.matchIndex[server] >= rf.getLastLogIndex() {
			rf.cond.Wait()
		}
		rf.cond.L.Unlock()
		rf.replicateLog(server)
	}
}

func (rf *Raft) majorityMatched(n int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// already committed
	if n <= rf.commitIndex {
		return false
	}
	cnt := 0
	for _, m := range rf.matchIndex {
		if m >= n {
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
}

func (rf *Raft) setAppliedIndex(appliedIndex int) {
	if appliedIndex < rf.lastApplied {
		return
	}
	rf.lastApplied = appliedIndex
}

func (rf *Raft) applyCommandsLoop() {
	for !rf.killed() {
	recheck:
		if rf.lastApplied < rf.commitIndex {
			rf.dprintf(" APPLY %d-%d", rf.lastApplied, rf.commitIndex)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if i <= rf.LastIncludedIndex {
					goto recheck
				}
				cmd := rf.getLogCommand(i)
				if cmd == nil {
					log.Fatalf("index %d cmd is nil", i)
				}
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       cmd,
					CommandIndex:  i,
					SnapshotValid: false,
				}
				rf.applyCh <- msg
				rf.setAppliedIndex(i)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	close(rf.applyCh)
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = -1
	term = -1
	isLeader = (rf.state == Leader)

	// Your code here (2B).
	if !isLeader || rf.killed() {
		return
	}
	index = rf.getLastLogIndex() + 1
	term = rf.CurrentTerm
	rf.dprintf(" START %3d{%3d}: %v", index, term, command)
	entry := LogEntry{
		Term:     term,
		LogIndex: index,
		Command:  command,
	}
	rf.Logs = append(rf.Logs, entry)
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.cond.L.Lock()
	rf.cond.Broadcast()
	rf.cond.L.Unlock()
	return
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
	rf.killCh <- struct{}{}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) mainRoutine() {
	rf.dprintf("")
	go rf.applyCommandsLoop()
	for !rf.killed() {
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
	select {
	case <-rf.killCh:
		return
	case <-rf.timer.C:
	}
	rf.mu.Lock()
	rf.dprintf("timeoout, convert to candidate")
	rf.state = Candidate
	rf.mu.Unlock()
}

func (rf *Raft) candidateRoutine() {
electAgain:
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.dprintf("term(TO) = %d", rf.CurrentTerm)
	rf.voteCount = 1
	rf.stopTimer()
	rf.resetTimer()
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLogTerm(lastLogIndex)
		args := &RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandiateId:   rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(server, args, reply)
	}
	select {
	case <-rf.killCh:
		return
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
	go rf.notifyFollowersLoop()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = rf.getLastLogIndex()
			continue
		}
		rf.matchIndex[i] = 0
		go rf.replicateLogLoop(i)
	}
	select {
	case <-rf.killCh:
		return
	case <-rf.higherTermCh:
	}
	rf.stopTimer()
}

func (rf *Raft) notifyFollowersLoop() {
	for !rf.killed() {
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
			args := &AppendEntriesArgs{
				IsHeartbeat:  true,
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: min(rf.matchIndex[server], rf.commitIndex),
			}
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(server, args, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastIncludedTerm {
		return
	}
	tmpTerm := rf.getLogTerm(index)
	tempLogs := []LogEntry{}
	tempLogs = append(tempLogs, rf.Logs[index-rf.LastIncludedIndex:]...)
	rf.Logs = tempLogs
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = tmpTerm
	rf.persister.SaveSnapshot(snapshot)
	rf.dprintf("server(%d) take snapshot on index = %d", rf.me, rf.LastIncludedIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state != Follower {
			rf.dprintf("recv term %d, convert to follower", args.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
	}
	rf.leaderId = args.LeaderId
	if args.LastIncludedIndex < rf.LastIncludedIndex || args.LastIncludedTerm < rf.LastIncludedTerm {
		rf.dprintf("Error: install snapshot with smaller last index or term")
	}

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		rf.dprintf("receive older snapshot on index %d", args.LastIncludedIndex)
		return
	}
	// args.LastIncludedIndex > rf.LastIncludedIndex

	if args.LastIncludedIndex >= rf.getLastLogIndex() {
		rf.Logs = []LogEntry{}
	} else {
		tempLogs := []LogEntry{}
		tempLogs = append(tempLogs, rf.Logs[args.LastIncludedIndex-rf.LastIncludedIndex:]...)
		rf.Logs = tempLogs
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveSnapshot(args.Data)
	rf.dprintf("server(%d) install snapshot on index = %d", rf.me, rf.LastIncludedIndex)
	rf.setCommitIndex(args.LastIncludedIndex)
	rf.setAppliedIndex(args.LastIncludedIndex)
	rf.persist()

	restoreMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotData:  args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCh <- restoreMsg
	rf.dprintf("install snapshot from leader(%d) on index %d", args.LeaderId, args.LastIncludedIndex)
}

func (rf *Raft) sendSnapshot(server int) int {
	rf.dprintf("sending snapshot to %d", server)
	lastIncludedIndex := rf.LastIncludedIndex
	lastIncludedTerm := rf.LastIncludedTerm
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Lock()
	if rf.state != Leader || rf.CurrentTerm != args.Term || lastIncludedIndex != rf.LastIncludedIndex ||
		lastIncludedTerm != rf.LastIncludedTerm || rf.killed() {
		rf.mu.Unlock()
		return RpcAssumptionFailed
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return RpcFailed
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// recheck assumptions
	if rf.state != Leader || rf.CurrentTerm != args.Term || lastIncludedIndex != rf.LastIncludedIndex ||
		lastIncludedTerm != rf.LastIncludedTerm || rf.killed() {
		return RpcAssumptionFailed
	}
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state == Leader {
			rf.dprintf("recv term %d, convert to follower", reply.Term)
			rf.state = Follower
			rf.higherTermCh <- struct{}{}
		}
		return RpcHighTerm
	}
	rf.matchIndex[server] = rf.LastIncludedIndex
	rf.nextIndex[server] = rf.LastIncludedIndex + 1
	return RpcSucceed
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

	// volatitle data
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	s := rand.NewSource(time.Now().UnixNano())
	rf.rand = rand.New(s)
	rf.state = Follower
	rf.voteCount = 1
	rf.timer = time.NewTimer(5 * time.Second)
	rf.stopTimer()
	rf.higherTermCh = make(chan struct{}, 1)
	rf.majorityVotesCh = make(chan struct{}, 1)
	rf.leaderFoundCh = make(chan struct{}, 1)
	rf.killCh = make(chan struct{}, 1)
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.cond = sync.NewCond(&sync.Mutex{})

	// persistent data
	if persister.ReadRaftState() != nil {
		rf.readPersist(persister.ReadRaftState())
		rf.setAppliedIndex(rf.LastIncludedIndex)
	} else {
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		paddingEntry := LogEntry{
			Term:     0,
			LogIndex: 0,
			Command:  nil,
		}
		rf.Logs = []LogEntry{paddingEntry}
		rf.LastIncludedIndex = -1
		rf.LastIncludedTerm = 0
	}
	go rf.mainRoutine()

	return rf
}
