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
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"time"

	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type RaftStatus int32

const (
	follower RaftStatus = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu         deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	term       int                 // 当前任期
	status     RaftStatus
	lastAppend int64 //上一次追加日志的时间
	hasVote    bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Entry []byte

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	isLeader := rf.status == leader
	// Your code here (2A).
	return term, isLeader
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //任期
	CandidateId  int //候选人id
	LastLogIndex int //上一个日志下表
	LastLogTerm  int //上一个日志所在的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //若投票被拒绝，候选人通过这个更新自己的任期
	VoteGranted bool //是否被投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	//fmt.Println(args.CandidateId, "向", rf.me, "征票")
	if rf.hasVote && rf.voteCheck() {
		reply.VoteGranted = true
		rf.hasVote = false
	}
}

//附加日志RPC/心跳检测
type AppendEntriesArgs struct {
	Term         int     //领导人任期号
	LeaderId     int     //领导人Id
	PrevLogIndex int     //领导人前一个日志下标
	PrevLogTerm  int     //领导人前一个日志的任期
	Entries      []Entry //日志
	LeaderCommit int     //领导人已经提交的日志索引值
}

type AppendEntriesReply struct {
	Term    int  //follow的任期
	Success bool //是否在follow上更新成功
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//首先判读一致性
	fmt.Println("心跳检测")
	term, _ := rf.GetState()
	reply.Term = term
	/*
		if !isLeader || rf.appendEntriesCheck(args.Term, term) {
			fmt.Println("退出")
			return
		}*/
	rf.mu.Lock()
	rf.term = args.Term
	rf.status = follower
	atomic.StoreInt64(&rf.lastAppend, time.Now().UnixNano()/1e6)
	fmt.Println(args.LeaderId, "向", rf.me, "发送心跳")
	rf.mu.Unlock()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) appendEntriesCheck(leaderTerm, myTerm int) bool {
	if myTerm > leaderTerm {
		return false
	}
	return true
}

func (rf *Raft) voteCheck() bool {
	return true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//160ms就会有一次心跳，选举超时时间应远大于160，这里取[500,650]
		timeout := int64(rand.Intn(150) + 300)
		//当前raft是leader或者选举还未超时
		interval := time.Now().UnixNano()/1e6 - atomic.LoadInt64(&rf.lastAppend)
		//fmt.Println("interval:", interval, "timeout", timeout)
		if rf.getStatus() == leader {
			//fmt.Println(rf.me, "是leader")
			time.Sleep(time.Duration(timeout) * time.Millisecond)
		} else if interval < timeout {
			fmt.Println(rf.me, "还没有过期")
			time.Sleep(time.Duration(timeout-interval) * time.Millisecond)
		} else {
			fmt.Println(rf.me, "开始选举")
			rf.electLeader()
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			//重置选票
			rf.mu.Lock()
			rf.hasVote = true
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) electLeader() {
	//fmt.Println(rf.me, "开始选举")
	rf.parseRaftStatus(candidate)
	rf.mu.Lock()
	rf.term++
	args := &RequestVoteArgs{CandidateId: rf.me, Term: rf.term}
	rf.mu.Unlock()
	validNodesCounts := 0 //有效节点个数
	successCount := 0     // 被选上的票数
	survivalNode := make([]int, 0)
	//先对自己征票
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(rf.me, args, reply) {
		validNodesCounts++
		survivalNode = append(survivalNode, rf.me)
	}
	if reply.VoteGranted {
		successCount++
	}
	//对其它的节点征票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply = &RequestVoteReply{}
		if rf.sendRequestVote(i, args, reply) {
			validNodesCounts++
			survivalNode = append(survivalNode, i)
		}
		if reply.VoteGranted {
			successCount++
		}
	}
	//如果选票大于等于有效节点个数的一半，成功当选
	fmt.Println("当前存活节点:", survivalNode, "   ", rf.me, "获得选票数", successCount)
	if successCount > (validNodesCounts)/2 {
		fmt.Println(rf.me, "成功当选leader")
		rf.parseRaftStatus(leader)
		go rf.heartBeat()
	}
}

func (rf *Raft) heartBeat() {
	//每个160ms发送一次心跳包
	for rf.killed() == false {
		time.Sleep(160 * time.Millisecond)
		term, _ := rf.GetState()
		args := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(i, args, reply) {
				fmt.Println("心跳失败")
				continue
			}
			if !reply.Success && term < reply.Term {
				//心跳失败，可能原因就是自己的term落后，此时1应该更新term并且有由leader变为follower
				rf.term = reply.Term
				rf.parseRaftStatus(follower)
				return
			}
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
	fmt.Println("创建raft节点", me)
	//fmt.Println("me:", me, "persister", *persister)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.hasVote = true
	rf.status = follower
	atomic.StoreInt64(&rf.lastAppend, time.Now().UnixNano()/1e6)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) parseRaftStatus(status RaftStatus) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = status
}

func (rf *Raft) getStatus() RaftStatus {
	rf.mu.Lock()
	status := rf.status
	rf.mu.Unlock()
	return status
}
