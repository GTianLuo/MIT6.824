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
	"log"
	"math/rand"
	"sync"
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
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	term      int                 // 当前任期
	status    RaftStatus
	timer     *time.Timer
	hasVote   bool
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

func generateTimeOut() time.Duration {
	return time.Duration(800+rand.Intn(150)) * time.Millisecond
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
	PartAInfo(args.CandidateId, "向", rf.me, "征票")
	reply.Term = rf.term
	//若当前节点是leader，若对方term大于自己，卸职
	if rf.status == leader && args.Term > args.Term {
		fmt.Println(rf.me, "在收到", args.CandidateId, "的征票后卸职")
		rf.status = follower
		return

	}
	//fmt.Println(args.CandidateId, "向", rf.me, "征票")
	if rf.hasVote && rf.voteCheck(args.Term, reply.Term) {
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
	term, isLeader := rf.GetState()
	reply.Term = term
	PartAInfo(rf.me, ":", term, " ", args.LeaderId, ":", args.Term)
	if !rf.appendEntriesCheck(args.Term, term) {
		PartAInfo(rf.me, "丢弃来自", args.LeaderId, "的包")
		return
	}
	//当前节点如果是leader，说明此时有两个leader存在
	if isLeader && term > args.Term {
		return
	}
	rf.mu.Lock()
	rf.status = follower
	rf.term = args.Term
	rf.hasVote = true
	rf.timer.Reset(generateTimeOut())
	PartAInfo(args.LeaderId, "向", rf.me, "发送心跳")
	rf.mu.Unlock()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendRequestVote(send chan bool, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	send <- ok
}

func (rf *Raft) appendEntriesCheck(leaderTerm, currentTerm int) bool {
	if currentTerm > leaderTerm {
		return false
	}
	return true
}

func (rf *Raft) voteCheck(senderTerm int, myTerm int) bool {
	if senderTerm < myTerm {
		return false
	}
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
		rf.timer.Reset(generateTimeOut())
		<-rf.timer.C
		switch rf.getStatus() {
		case leader:
			continue
		case follower:
			//PartAInfo(rf.me, "开始选举")
			rf.mu.Lock()
			rf.hasVote = true
			rf.mu.Unlock()
			rf.electLeader()
		default:
			log.Println("invalid raft status")
		}
	}
}

func (rf *Raft) electLeader() {
	PartAInfo(rf.me, "开始选举 term:", rf.term)
	rf.parseRaftStatus(candidate)
	PartAInfo(rf.me, ":", rf.term)
	rf.mu.Lock()
	args := &RequestVoteArgs{CandidateId: rf.me, Term: rf.term}
	rf.mu.Unlock()
	var successCount int = 0 // 被选上的票数
	//对其它的节点征票
	replys := make(chan *RequestVoteReply, len(rf.peers))
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		wg.Add(1)
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			send := make(chan bool)
			go rf.sendRequestVote(send, server, args, reply)
			select {
			case <-time.After(time.Millisecond * 160):
				PartAInfo(rf.me, "向", server, "发送投票请求网络超时")
				go func() {
					<-send
				}()
				wg.Done()
				return
			case <-send:
			}
			replys <- reply
			wg.Done()
		}(i, args)
	}
	wg.Wait()
	close(replys)
	for r := range replys {
		if r.VoteGranted {
			successCount++
		} else if r.Term > args.Term {
			rf.mu.Lock()
			rf.term = r.Term
			rf.mu.Unlock()
			PartAInfo(rf.me, "任期落后，放弃选票")
			rf.parseRaftStatus(follower)
			return
		}
	}
	//如果选票大于全部节点个数的一半，成功当选
	PartAInfo(rf.me, "获得选票数", successCount)
	if successCount > len(rf.peers)/2 {
		PartAInfo(rf.me, "成功当选leader", rf.me, ":", rf.term)
		rf.parseRaftStatus(leader)
		go rf.heartBeat()
	} else {
		rf.parseRaftStatus(follower)
	}
}

func (rf *Raft) heartBeat() {
	//每个160ms发送一次心跳包
	for rf.killed() == false {
		term, isLeader := rf.GetState()
		if !isLeader {
			break
		}
		args := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				if !rf.sendAppendEntries(server, args, reply) {
					//PartAInfo(rf.me, "向", server, "发送心跳失败")
					return
				}
				if !reply.Success && reply.Term > term {
					//此leader已经落后于其它leader
					PartAInfo(rf.me, "卸职")
					rf.parseRaftStatus(follower)
					rf.mu.Lock()
					rf.term = reply.Term
					rf.mu.Unlock()
					return
				}
			}(i, args)
		}
		time.Sleep(time.Millisecond * 160)
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
	PartAInfo("创建raft节点", me)
	//fmt.Println("me:", me, "persister", *persister)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.hasVote = true
	rf.status = follower
	rf.timer = time.NewTimer(generateTimeOut())

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
	switch status {
	case rf.status:
		return
	case follower:
		rf.status = follower
		rf.hasVote = true
		rf.timer.Reset(generateTimeOut())
	case leader:
		rf.status = leader
	case candidate:
		rf.term++
		rf.status = candidate
	}
}

func (rf *Raft) getStatus() RaftStatus {
	rf.mu.Lock()
	status := rf.status
	rf.mu.Unlock()
	return status
}
