package raft

//
// git clone https://github.com/l1nkkk/6.824.git mit-6.824-srcs
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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft states
type State int

const (
	Unknown State = iota
	Follower
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Invalid"
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh   chan ApplyMsg
	state     State
	timeout   time.Duration
	voteCount int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	lastApplied int
	commitIndex int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Channels
	winVoteCh   chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
}

func (rf *Raft) ToString() string {
	// Threading unsafe
	return fmt.Sprintf(
		`[%d]Raft[state: %s, currentTerm: %d,
		 votedFor: %d, log: %v, lastApplied: %d,
		 commitIndex: %d, nextIndex: %v, matchIndex: %v]`,
		rf.me, rf.state.String(), rf.currentTerm,
		rf.votedFor, rf.log, rf.lastApplied, rf.commitIndex,
		rf.nextIndex, rf.matchIndex)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	DPrintf("[%d] GetState: term: %d, isleader: %t\n", rf.me, term, isleader)

	return term, isleader
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

func (rf *Raft) resetChannels() {
	rf.winVoteCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

func (rf *Raft) getPersistState() []byte {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		EPrintf("[ERROR] [%d] persist failed\n", rf.me)
		panic("persist failed")
	}
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		EPrintf("[IMPORTANT][%d] applyLogs: %v\n", rf.me, rf.log[i])
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
	EPrintf("[IMPORTANT][%d] full log: %v lastApplied: %d commitIndex: %d\n",
		rf.me, rf.log, rf.lastApplied, rf.commitIndex)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	EPrintf("[%d] readPersist\n", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		EPrintf("[%d] readPersist no data found\n", rf.me)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		EPrintf("[ERROR] [%d] readPersist failed\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
  rf.mu.Lock()
  defer rf.mu.Unlock()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := rf.getLastIndex()
	if index <= lastIndex {
		return
	}
	rf.log = rf.log[lastIndex+1:]
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}

// RPC definitions start here

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	EPrintf("[%d] RequestVote from server %d\n", rf.me, args.CandidateId)
	EPrintf("[%d] currentTerm: %d, args.Term: %d\n", rf.me, rf.currentTerm, args.Term)
	if args.Term < rf.currentTerm {
		DPrintf("[%d] vote for server %d failed, args.Term < rf.currentTerm\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[%d] became follower, args.Term > rf.currentTerm\n", rf.me, args.CandidateId)
		rf.toFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		((args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex()) ||
			args.LastLogTerm > rf.getLastTerm()) {
		DPrintf("[%d] vote for server %d succeed since leader is up-to-date, args.LastLogIndex: %d, rf.getLastIndex(): %d\n",
			rf.me, args.CandidateId, args.LastLogIndex, rf.getLastIndex())
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		select {
		case rf.grantVoteCh <- true:
		default:
			DPrintf("[%d] grantVoteCh is full\n", rf.me)
		}
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%d] calling sendRequestVote to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("[%d] sendRequestVote failed to server %d\n", rf.me, server)
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] Request vote from server %d failed, reply.Term > rf.currentTerm\n", rf.me, server)
		rf.toFollower(reply.Term)
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			DPrintf("[%d] win vote with voteCount: %d, total peers: %d\n",
				rf.me, rf.voteCount, len(rf.peers))
			select {
			case rf.winVoteCh <- true:
			default:
				DPrintf("[%d] winVoteCh is full\n", rf.me)
			}
		}
	} else {
		DPrintf("[%d] vote from server %d is rejected\n", rf.me, server)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	select {
	case rf.heartbeatCh <- true:
	default:
		DPrintf("[%d] heartbeatCh is full\n", rf.me)
	}

	// Check follower log is shorter than leader
	if args.PrevLogIndex > lastIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = lastIndex + 1
		return
	}
	// If an existing entry conflicts with a new one
	// (same index but different terms), delete the existing entry
	// and all that follow it
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		EPrintf("[%d] conflict detected, args.PrevLogIndex: %d, args.PrevLogTerm: %d, rf.log[args.PrevLogIndex].Term: %d\n",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		conflictIndex := args.PrevLogIndex
		for i := args.PrevLogIndex; i >= 0 &&
			rf.log[i].Term == rf.log[args.PrevLogIndex].Term; i-- {
			conflictIndex = i
		}
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Append entries
	i, j := args.PrevLogIndex+1, 0
	for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.log = rf.log[:i]
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)

	// If leaderCommit > commitIndex,
	// set commitIndex =min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.applyLogs()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("[%d] sendAppendEntries failed to server %d\n", rf.me, server)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return ok
	}

	// Update nextIndex and matchIndex
	if reply.Success {
		EPrintf("[%d] AppendEntries to server %d success\n", rf.me, server)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries)
		if rf.nextIndex[server] > rf.matchIndex[server] {
			rf.matchIndex[server] = rf.nextIndex[server]
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm == 0 {
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		EPrintf("[%d] AppendEntries to server %d failed, conflict index: %d log %v\n",
			rf.me, server, reply.ConflictIndex, rf.log)
	} else {
		// try to find the conflictTerm in log
		newNextIndex := rf.getLastIndex()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}
		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	EPrintf("[%d] finding the n to commit\n", rf.me)
	for n := rf.getLastIndex(); n > rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me && rf.matchIndex[server] >= n {
					count++
				}
			}
		}
		EPrintf("[%d] n: %d, count: %d\n", rf.me, n, count)
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			EPrintf("[%d] commiting index: %d\n", rf.me, rf.commitIndex)
			go rf.applyLogs()
			break
		}
	}

	return ok
}

// RPC definitions end here

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, isLeader
	}

	isLeader = true
	term = rf.currentTerm
	EPrintf("[%d] Start send command: %v\n", rf.me, command)
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	index = rf.getLastIndex()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("[%d] broadcastHeartbeat\n", rf.me)
	if rf.state != Leader {
		DPrintf("[ERROR] [%d] broadcastHeartbeat failed, not leader\n", rf.me)
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			entries := rf.log[rf.nextIndex[i]:]
			prevTerm := rf.log[rf.nextIndex[i]-1].Term
			leaderCommit := rf.commitIndex
			go func(server int) {
				DPrintf("[%d] broadcastHeartbeat to server %d with entries: %v\n", rf.me, server, entries)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: leaderCommit,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  prevTerm,
					Entries:      make([]LogEntry, len(entries)),
				}
				copy(args.Entries, entries)
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(server, args, reply)
			}(i)
		}
	}
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.resetChannels()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	EPrintf("[%d] become leader with term %d\n", rf.me, rf.currentTerm)
	// Sleep for a while to avoid no agreement on election.
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	rf.broadcastHeartbeat()
}

func (rf *Raft) toFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	EPrintf("[%d] become follower with term %d\n", rf.me, term)
	if state != Follower {
		select {
		case rf.stepDownCh <- true:
		default:
			DPrintf("[%d] stepDownCh is full\n", rf.me)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(120 * time.Millisecond):
				// Short timeout for leader to send heartbeat in case of follwer re-election.
				rf.mu.Lock()
				rf.broadcastHeartbeat()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.heartbeatCh:
				DPrintf("[%d] receive heartbeat\n", rf.me)
			case <-rf.grantVoteCh:
			case <-time.After(randomTimeout()):
				// Leader not alive, start election
				DPrintf("[%d] re-election from follower\n", rf.me)
				rf.startElection(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winVoteCh:
				rf.toLeader()
			case <-time.After(randomTimeout()):
				// Re-election
				DPrintf("[%d] re-election from candidate\n", rf.me)
				rf.startElection(Candidate)
			}
		default:
			DPrintf("[%d] invalid state: %s\n", rf.me, state.String())
		}
	}
}

func (rf *Raft) startElection(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.state = Candidate
	rf.resetChannels()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	if rf.state != Candidate {
		return
	}

	DPrintf("[%d] start election with term %d\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastIndex(),
				LastLogTerm:  rf.getLastTerm(),
			}
			reply := &RequestVoteReply{}
			go func(server int) {
				rf.sendRequestVote(server, args, reply)
			}(i)
		}
	}
}

func randomTimeout() time.Duration {
	return time.Duration(360+rand.Intn(240)) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.timeout = randomTimeout()
	rf.state = Follower
	rf.votedFor = -1
	rf.winVoteCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
