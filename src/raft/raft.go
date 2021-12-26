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
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type CommandAndTerm struct {
	Command interface{}
	Term    int
}

type State int8

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type SnapshotData struct {
	snapshotIndex int
	snapshotTerm  int
}
type InstallSnapshotResult struct {
	snapshotIndex int
	snapshotTerm  int
	succeeded     bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	dead             int32               // set by Kill()
	applyCh          chan ApplyMsg
	state            State
	receiveVoteCount int
	commitedIndex    int
	lastApplied      int
	currentTerm      int
	votedFor         int
	log              []CommandAndTerm
	nextIndex        []int
	matchIndex       []int
	appenderCv       *sync.Cond
	applierCv        *sync.Cond
	bgLoopShutdownCh chan int
	electionTs       time.Time
	snapshot         SnapshotData
	leaderId         int

	reprMu sync.Mutex
	repr   string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot.snapshotIndex)
	e.Encode(rf.snapshot.snapshotTerm)
	data := w.Bytes()
	if snapshot != nil {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
	rf.reprMu.Lock()
	rf.repr = rf.String()
	rf.reprMu.Unlock()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries = make([]CommandAndTerm, 1)
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		log.Fatal("failed to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
		rf.snapshot.snapshotIndex = snapshotIndex
		rf.snapshot.snapshotTerm = snapshotTerm
		rf.lastApplied = rf.snapshot.snapshotIndex
		rf.commitedIndex = rf.snapshot.snapshotIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	result := InstallSnapshotResult{lastIncludedIndex, lastIncludedTerm, true}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nextLogIndex() > lastIncludedIndex+1 || rf.snapshot.snapshotIndex >= lastIncludedIndex {
		// make sure log length does not shrink after istalling snapshot
		rf.DLog("Rejecting snapshot: nextLogIndex=%d, lastIncludedIndex=%d", rf.nextLogIndex(), lastIncludedIndex)
		result.succeeded = false
		return false
	}
	rf.DLog("Applying snapshot %d %d", lastIncludedIndex, lastIncludedTerm)
	rf.snapshot = SnapshotData{lastIncludedIndex, lastIncludedTerm}
	rf.log = make([]CommandAndTerm, 1) // dummy entry at 0
	rf.commitedIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persist(snapshot)
	return true
}

// called with rf.mu locked
func (rf *Raft) getEntryTerm(i int) int {
	if i == rf.snapshot.snapshotIndex {
		return rf.snapshot.snapshotTerm
	}
	return rf.getEntry(i).Term
}

// called with rf.mu locked
func (rf *Raft) getEntry(i int) CommandAndTerm {
	if i == 0 {
		// assume there is a dummy entry 0 with term=0
		return CommandAndTerm{}
	}
	i -= rf.snapshot.snapshotIndex
	if i <= 0 {
		panic(fmt.Sprintf("getEntry index out of band: %d", i))
	}
	return rf.log[i]
}

// called with rf.mu locked
func (rf *Raft) setEntry(i int, cat CommandAndTerm) {
	i -= rf.snapshot.snapshotIndex
	if i <= 0 || i > len(rf.log) {
		log.Fatal("Setting entry with index out of band: ", i)
	}
	if i == len(rf.log) {
		rf.log = append(rf.log, cat)
	} else {
		rf.log[i] = cat
	}
}

// called with rf.mu locked
func (rf *Raft) trimLogEntriesAfter(i int) {
	i -= rf.snapshot.snapshotIndex
	if i <= 0 {
		log.Fatal("Triming out of band", i)
	}
	rf.log = rf.log[:i]
}

// called with rf.mu locked
func (rf *Raft) nextLogIndex() int {
	return rf.snapshot.snapshotIndex + len(rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitedIndex {
		log.Fatal("Snapshotting uncommited index ", index)
	}
	delta := index - rf.snapshot.snapshotIndex
	rf.snapshot = SnapshotData{index, rf.getEntryTerm(index)}
	rf.DLog("Snapshotting: discarding %d log entries", delta)
	oldLog := rf.log
	rf.log = make([]CommandAndTerm, len(rf.log)-delta)
	copy(rf.log, oldLog[delta:])
	rf.persist(snapshot)
}

type InstallSnapshotReq struct {
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}
type InstallSnapshotResp struct {
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotReq, resp *InstallSnapshotResp) {
	rf.mu.Lock()
	rf.UpdateElectionTs()
	rf.mu.Unlock()

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = req.Snapshot
	msg.SnapshotIndex = req.SnapshotIndex
	msg.SnapshotTerm = req.SnapshotTerm
	rf.applyCh <- msg
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	PeerIndex    int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) DLog(format string, args ...interface{}) {
	if Debug == 0 {
		return
	}
	s := fmt.Sprintf(format, args...)
	if s[len(s)-1] != '\n' {
		s += "\n"
	}
	prefix := fmt.Sprintf("%v", time.Now().Format("15:04:05.00000"))
	rf.reprMu.Lock()
	DPrintf("%s %s %s", prefix, rf.repr, s)
	rf.reprMu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.MaybeBecomeFollower(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	lastIndex := rf.nextLogIndex() - 1
	lastTerm := rf.getEntryTerm(lastIndex)
	peerMoreUpToDate := false
	canVoteForPeer := rf.votedFor == args.PeerIndex || rf.votedFor == -1
	if args.LastLogTerm > lastTerm {
		peerMoreUpToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
		peerMoreUpToDate = true
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else if canVoteForPeer && peerMoreUpToDate {
		rf.votedFor = args.PeerIndex
		rf.persist(nil)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	if reply.VoteGranted {
		// refresh election timeout
		rf.UpdateElectionTs()
		rf.DLog("%v voted for %v at term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	} else {
		rf.DLog("%v did not vote for %v at term %v. currentTerm: %v, votedFor: %v\n",
			rf.me, args.PeerIndex, args.Term, rf.currentTerm, rf.votedFor)
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
	index := 0
	term := 0
	rf.mu.Lock()
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.nextLogIndex()
		term = rf.currentTerm
		rf.setEntry(index, CommandAndTerm{command, term})
		rf.persist(nil)
		rf.appenderCv.Broadcast()
	}
	rf.mu.Unlock()
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
	rf.DLog("stopping")
	atomic.StoreInt32(&rf.dead, 1)
	go rf.cleanup()
}

func (rf *Raft) cleanup() {
	for i := 0; i < len(rf.peers)+3; i++ {
		// n-1 appender
		// 1 applier
		// 1 election scheduling loop
		// 1 heart beat
		// 1 snapshotter
	Loop:
		for {
			select {
			case <-rf.bgLoopShutdownCh:
				break Loop
			case <-time.After(time.Millisecond * 100):
				rf.DLog("broadcasting cv")
				rf.mu.Lock()
				rf.appenderCv.Broadcast()
				rf.applierCv.Broadcast()
				rf.mu.Unlock()
			}
		}
		rf.DLog("%d bg loops stopped", i+1)
	}
	// all loops stopped, drain channels and wait for inflight rpcs to finish
	rf.DLog("fully shutdown")
	atomic.StoreInt32(&rf.dead, 2)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 2
}

func (rf *Raft) isStopping() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesRequest struct {
	LeaderId         int
	Term             int
	Entries          []CommandAndTerm
	PreviousLogTerm  int
	PreviousLogIndex int
	LeaderCommit     int
}

type AppendEntriesResponse struct {
	Successs                  bool
	Term                      int
	ConflictingTerm           int
	ConflictingTermStartIndex int
	LogLen                    int
	SnapshotIndex             int
	SnapshotTerm              int
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		rf.DLog("Ignoring old query %v, term=%v", req, rf.currentTerm)
		resp.Successs = false
		return
	}
	rf.MaybeBecomeFollower(req.Term)
	if rf.state != FOLLOWER {
		panic(fmt.Sprintf("Ignore AppendEntries when state=%v", rf.state))
	}
	if rf.leaderId == -1 {
		rf.leaderId = req.LeaderId
		rf.UpdateElectionTs()
	} else if rf.leaderId != req.LeaderId {
		panic(fmt.Sprintf("two leaders %d %d in term %d", rf.leaderId, req.LeaderId, rf.currentTerm))
	} else {
		// receive from current leader
		rf.UpdateElectionTs()
	}

	if len(req.Entries) > 0 {
		rf.DLog("Append Entries %v", req)
	}
	shouldAppend := req.PreviousLogIndex < rf.snapshot.snapshotIndex ||
		(req.PreviousLogIndex < rf.nextLogIndex() &&
			rf.getEntryTerm(req.PreviousLogIndex) == req.PreviousLogTerm)
	if shouldAppend {
		resp.Successs = true
		if len(req.Entries) > 0 {
			firstIndex := req.PreviousLogIndex + 1
			for i, cat := range req.Entries {
				index := i + firstIndex
				if index <= rf.snapshot.snapshotIndex {
					continue
				}
				if index < rf.nextLogIndex() {
					if cat.Term != rf.getEntryTerm(index) {
						if rf.commitedIndex >= index {
							log.Fatal("trimming commited log", index)
						}
						rf.trimLogEntriesAfter(index)
						rf.setEntry(index, cat)
					}
				} else if index == rf.nextLogIndex() {
					rf.setEntry(index, cat)
				} else {
					log.Fatal("index > log length", index, rf.nextLogIndex())
				}
			}
			rf.persist(nil)
			rf.DLog("%d entries appended", len(req.Entries))
			rf.appenderCv.Broadcast()
		}
		if req.LeaderCommit > rf.commitedIndex {
			newValue := Min(req.PreviousLogIndex+len(req.Entries), req.LeaderCommit)
			if newValue > rf.commitedIndex {
				rf.commitedIndex = newValue
				rf.DLog("committed index=%v", rf.commitedIndex)
				rf.applierCv.Signal()
			}
		}
		rf.DLog("finish Append Entries %v", req)
	} else {
		// - prev index too new (>= next log id)
		// - prev index too old (< snapshot index)
		// - conflict term at previous index
		if req.PreviousLogIndex >= rf.nextLogIndex() {
			resp.ConflictingTermStartIndex = rf.nextLogIndex()
			resp.ConflictingTerm = 0 // impossible term
		} else {
			resp.ConflictingTerm = rf.getEntryTerm(req.PreviousLogIndex)
			startingIndex := req.PreviousLogIndex
			for startingIndex > rf.snapshot.snapshotIndex &&
				startingIndex > 1 &&
				rf.getEntryTerm(startingIndex-1) == resp.ConflictingTerm {
				startingIndex--
			}
			resp.ConflictingTermStartIndex = startingIndex
		}
		resp.SnapshotIndex = rf.snapshot.snapshotIndex
		resp.SnapshotTerm = rf.snapshot.snapshotTerm
		resp.Successs = false
		rf.DLog("ShouldApped=false %v", req)
	}
}

func (rf *Raft) SendAppendEntriesRequests(peerIndex int, req *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{}
	newNextIndex := req.PreviousLogIndex + len(req.Entries) + 1
	if rf.peers[peerIndex].Call("Raft.AppendEntries", req, resp) {
		rf.mu.Lock()
		// ignore response if no longer leader
		if rf.state == LEADER {
			rf.MaybeBecomeFollower(resp.Term)
			if resp.Successs {
				if rf.nextIndex[peerIndex] < newNextIndex {
					rf.nextIndex[peerIndex] = newNextIndex
					rf.matchIndex[peerIndex] = newNextIndex - 1
					rf.appenderCv.Broadcast()
					if rf.UpdateCommitIndex() {
						rf.DLog("committedIndex=%v", rf.commitedIndex)
						rf.applierCv.Signal()
					}
					rf.DLog("AppendEntries succeeded for %v. Inc nextIndex to %v", peerIndex, rf.nextIndex[peerIndex])
				}
			} else if resp.Term <= req.Term {
				// failed due to conflict
				if resp.SnapshotIndex >= rf.snapshot.snapshotIndex &&
					rf.getEntryTerm(resp.SnapshotIndex) != resp.SnapshotTerm {
					log.Fatal("peer's last snapshot term mismatch")
				}
				i := rf.nextIndex[peerIndex] - 1
				for i > resp.ConflictingTermStartIndex &&
					i > rf.snapshot.snapshotIndex &&
					rf.getEntryTerm(i-1) != resp.ConflictingTerm {
					i--
				}
				if i <= rf.snapshot.snapshotIndex {
					i = rf.snapshot.snapshotIndex
				}
				if rf.nextIndex[peerIndex] != i {
					rf.nextIndex[peerIndex] = i
					rf.appenderCv.Broadcast()
					rf.DLog("Reducing nextIndex for %d to %d", peerIndex, i)
				}
			}
		}
		rf.mu.Unlock()
	}
}

// call with rf.mu hold
func (rf *Raft) SendHeartBeat() {
	if rf.state != LEADER {
		return
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			req := &AppendEntriesRequest{}
			req.LeaderId = rf.me
			req.Term = rf.currentTerm
			// otherwise appender will send snapshot later
			if rf.nextIndex[i] > rf.snapshot.snapshotIndex {
				req.PreviousLogIndex = rf.nextIndex[i] - 1
				req.PreviousLogTerm = rf.getEntryTerm(req.PreviousLogIndex)
			} else {
				req.PreviousLogIndex = rf.snapshot.snapshotIndex
				req.PreviousLogTerm = rf.snapshot.snapshotTerm
			}
			req.LeaderCommit = rf.commitedIndex
			go rf.SendAppendEntriesRequests(i, req)
		}
	}
}

// called with rf.mu locked
func (rf *Raft) MaybeBecomeFollower(term int) {
	if (rf.state == CANDIDATE && term >= rf.currentTerm) || term > rf.currentTerm {
		if term > rf.currentTerm {
			rf.currentTerm = term
			rf.leaderId = -1
		}
		rf.votedFor = -1
		rf.persist(nil)
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		}
		rf.DLog("becomes a follower, term=%v", rf.currentTerm)
	}
}

func (rf *Raft) SendRequestVote(peerIndex int, req *RequestVoteArgs) {
	resp := &RequestVoteReply{}
	if rf.peers[peerIndex].Call("Raft.RequestVote", req, resp) {
		if resp.VoteGranted {
			term := resp.Term
			promote := false
			rf.mu.Lock()
			if rf.state == CANDIDATE {
				if term == rf.currentTerm {
					rf.receiveVoteCount++
					if rf.receiveVoteCount*2 > len(rf.peers) {
						promote = true
					}
				} else {
					rf.DLog("Ignore voting from old term %v", term)
				}
				if promote {
					rf.state = LEADER
					rf.receiveVoteCount = 0
					for i, _ := range rf.peers {
						rf.matchIndex[i] = 0
						rf.nextIndex[i] = rf.nextLogIndex()
					}
					rf.leaderId = rf.me
					rf.SendHeartBeat()
					rf.appenderCv.Broadcast()
					rf.DLog("%v becomes leader\n", rf.me)
				}
			} else {
				// receive vote when not a candidate
				rf.DLog("on longer a candidate. %v ignored vote", rf.me)
			}
			rf.mu.Unlock()
		}
	}

}

// called with rf.mu locked
func (rf *Raft) StartNewElection() {
	rf.UpdateElectionTs()
	rf.currentTerm++
	rf.leaderId = -1
	rf.receiveVoteCount = 1
	rf.votedFor = rf.me
	rf.persist(nil)
	rf.DLog("%v started new election for term %v\n", rf.me, rf.currentTerm)
	req := &RequestVoteArgs{}
	req.Term = rf.currentTerm
	req.PeerIndex = rf.me
	req.LastLogIndex = rf.nextLogIndex() - 1
	req.LastLogTerm = rf.getEntryTerm(req.LastLogIndex)
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.SendRequestVote(i, req)
		}
	}
}

// called with rf.mu locked
func (rf *Raft) UpdateCommitIndex() bool {
	if rf.state != LEADER {
		log.Fatal("only leader should call UpdateCommitIndex. Got ", rf.state)
	}
	a := make([]int, len(rf.peers))
	copy(a, rf.matchIndex)
	a[rf.me] = rf.nextLogIndex() - 1
	sort.Ints(a)
	quorumReplicatedIndex := a[(len(a)-1)/2]
	if quorumReplicatedIndex >= rf.snapshot.snapshotIndex &&
		rf.getEntryTerm(quorumReplicatedIndex) == rf.currentTerm &&
		quorumReplicatedIndex > rf.commitedIndex {
		rf.commitedIndex = quorumReplicatedIndex
		return true
	}
	return false
}

func (rf *Raft) RunSnapshotter() {
	for !rf.isStopping() {
		rf.mu.Lock()
		if rf.state == LEADER {
			for i, peer := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.nextIndex[i] <= rf.snapshot.snapshotIndex {
					req := InstallSnapshotReq{rf.persister.ReadSnapshot(), rf.snapshot.snapshotIndex, rf.snapshot.snapshotTerm}
					resp := InstallSnapshotResp{}
					peer := peer
					i := i
					go func() {
						rf.mu.Lock()
						rf.DLog("%d->%d sending snapshot", rf.me, i)
						rf.mu.Unlock()
						peer.Call("Raft.InstallSnapshot", &req, &resp)
					}()
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
	rf.DLog("shutting down snapshotter")
	rf.bgLoopShutdownCh <- 0
}

func (rf *Raft) RunAppender(i int) {
Loop:
	for !rf.isStopping() {
		var shouldAppend bool
		rf.mu.Lock()
		for {
			if rf.isStopping() {
				rf.mu.Unlock()
				break Loop
			}
			shouldAppend = rf.state == LEADER && rf.nextIndex[i] < rf.nextLogIndex() && rf.nextIndex[i] > rf.snapshot.snapshotIndex

			if shouldAppend {
				break
			} else {
				rf.appenderCv.Wait()
			}
		}
		req := AppendEntriesRequest{}
		index := rf.nextIndex[i]
		newNextIndex := rf.nextLogIndex()
		req.Term = rf.currentTerm
		req.LeaderId = rf.me
		for i := index; i < rf.nextLogIndex(); i++ {
			req.Entries = append(req.Entries, rf.getEntry(i))
		}
		req.PreviousLogIndex = index - 1
		req.PreviousLogTerm = rf.getEntryTerm(req.PreviousLogIndex)
		req.LeaderCommit = rf.commitedIndex
		rf.mu.Unlock()
		rf.DLog("sending log entries %v~%v to %v", index, newNextIndex-1, i)
		done := make(chan int, 1)
		go func() {
			rf.SendAppendEntriesRequests(i, &req)
			done <- 0
		}()
		select {
		case <-done:
			rf.DLog("finished sending log entries %v~%v to %v", index, newNextIndex-1, i)
		case <-time.After(time.Millisecond * 200):
			rf.DLog("AppendEntries RPC tmieout. Retrying.")
		}
	}
	rf.DLog("shutting down appender loop %d", i)
	rf.bgLoopShutdownCh <- 0
}

func (rf *Raft) RunAplier() {
Loop:
	for {
		rf.mu.Lock()
		for {
			if rf.isStopping() {
				rf.mu.Unlock()
				break Loop
			}
			if rf.lastApplied < rf.snapshot.snapshotIndex {
				panic(fmt.Sprintf("last appied id %d < snapshot id %d", rf.lastApplied, rf.snapshot.snapshotIndex))
			}
			if rf.lastApplied < rf.commitedIndex {
				break
			} else {
				rf.applierCv.Wait()
			}
		}
		msgs := []ApplyMsg{}
		for rf.lastApplied < rf.commitedIndex {
			rf.lastApplied++
			msg := ApplyMsg{}
			msg.CommandValid = true
			cat := rf.getEntry(rf.lastApplied)
			msg.Command = cat.Command
			msg.CommandTerm = cat.Term
			msg.CommandIndex = rf.lastApplied
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
	rf.DLog("shutting down applier loop")
	rf.bgLoopShutdownCh <- 0
}

// called with rf.mu locked
func (rf *Raft) UpdateElectionTs() {
	rf.electionTs = time.Now().Add(time.Millisecond * time.Duration(200+rand.Intn(200)))
}

func (rf *Raft) RunElectionSchedulingLoop() {
	rf.UpdateElectionTs()
	for !rf.isStopping() {
		rf.mu.Lock()
		if rf.isStopping() {
			rf.mu.Unlock()
			break
		}
		if time.Now().After(rf.electionTs) {
			if rf.state == FOLLOWER {
				rf.state = CANDIDATE
				rf.DLog("%v becomes candidate\n", rf.me)
			}
			if rf.state == CANDIDATE {
				rf.StartNewElection()
			}
			rf.UpdateElectionTs()
		}
		sleepDuration := rf.electionTs.Sub(time.Now())
		rf.mu.Unlock()
		time.Sleep(sleepDuration)
	}
	rf.DLog("shutting down election scheduling loop")
	rf.bgLoopShutdownCh <- 0
}

func (rf *Raft) RunHeartBeatLoop() {
	for !rf.isStopping() {
		select {
		case <-time.After(time.Millisecond * 100):
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.SendHeartBeat()
			}
			rf.mu.Unlock()
		}
	}
	rf.DLog("shutting down heart beat loop")
	rf.bgLoopShutdownCh <- 0
}

// called with rf.mu locked
func (rf *Raft) String() string {
	var state string
	var log string
	var index int
	var term int
	index = rf.me
	term = rf.currentTerm
	if rf.state == LEADER {
		state = "*"
	} else if rf.state == FOLLOWER {
		state = "."
	} else {
		state = "?"
	}
	if rf.snapshot.snapshotIndex > 0 {
		log += fmt.Sprintf("snap(%d:%d) ", rf.snapshot.snapshotIndex, rf.snapshot.snapshotTerm)
	}
	if rf.snapshot.snapshotIndex == rf.lastApplied {
		log += ">"
	}
	if rf.snapshot.snapshotIndex == rf.commitedIndex {
		log += "|"
	}
	if len(rf.log) < 10 {
		for i := 1; i < len(rf.log); i++ {
			log += fmt.Sprintf("(%d,%d)", i+rf.snapshot.snapshotIndex, rf.log[i].Term)
			if i+rf.snapshot.snapshotIndex == rf.lastApplied {
				log += ">"
			}
			if i+rf.snapshot.snapshotIndex == rf.commitedIndex {
				log += "|"
			}
			log += " "
		}
	} else {
		for i, j := 1, 1; i < len(rf.log); i = j {
			for j < len(rf.log) && rf.log[j].Term == rf.log[i].Term {
				j++
				if j-1+rf.snapshot.snapshotIndex == rf.lastApplied ||
					j-1+rf.snapshot.snapshotIndex == rf.commitedIndex {
					break
				}
			}
			log += fmt.Sprintf("(%d-%d:%d) ", i+rf.snapshot.snapshotIndex, j-1+rf.snapshot.snapshotIndex, rf.log[i].Term)
			if j-1+rf.snapshot.snapshotIndex == rf.lastApplied {
				log += ">"
			}
			if j-1+rf.snapshot.snapshotIndex == rf.commitedIndex {
				log += "|"
			}
		}
	}
	return fmt.Sprintf("%p %s%d(%d)[%s]", rf, state, index, term, log)
}

var once sync.Once

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
	once.Do(func() {
		CreateNewLogFile()
	})
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.receiveVoteCount = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]CommandAndTerm, 1) // dummy entry at 0
	rf.bgLoopShutdownCh = make(chan int)
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.nextLogIndex()
	}
	rf.appenderCv = sync.NewCond(&rf.mu)
	rf.applierCv = sync.NewCond(&rf.mu)
	go rf.RunElectionSchedulingLoop()
	for i, _ := range peers {
		if i != me {
			go rf.RunAppender(i)
		}
	}
	go rf.RunAplier()
	go rf.RunHeartBeatLoop()
	go rf.RunSnapshotter()
	rf.DLog("%v started\n", rf.me)
	return rf
}
