package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type RaftState int

const (
	RaftLeader RaftState = iota
	RaftCandidate
	RaftFollower
)

// 在lab3 raft里说明里，提到心跳间隔不能大于一秒10次，但是在lab4中，为了吞吐量，我这里把心跳间隔大大的缩短了
const TIME_INTERVAL_TO_SEND_HEART_BEAT = 20 // ms

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	// 存储了对每个节点进行RPC所需的相关信息，节点在这个数组的下标就是其唯一标识符/身份证
	// 实际上，这个程序并不会真正的在网络上通信，而是利用了一个框架，利用goroutine和channel来模拟网络上通信的行为
	// 具体的行为包括：延迟，丢包，宕机，乱序到达
	peers []*labrpc.ClientEnd // RPC end points of all peers
	// 这个持久化器只是把数据存到内存里，模拟把数据存到磁盘里。
	// 可以通过策略模式把tester.Persister这个类型改成一个api兼容的interface
	// 改成真正的把数据存到磁盘里的持久化器，api保持一致就好
	persister *tester.Persister // Object to hold this peer's persisted state
	me        int               // this peer's index into peers[]

	//
	applyCh chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState RaftState

	// Persistent state on all servers:
	currentTerm int
	// 其意义是: 自己在本任期内的投票对象，当任期更新(增大)时，它也要更新为none(-1)
	voteFor int
	logs    []LogEntry // 论文建议是1-indexed，在这里用空entry填充下标为0的位置

	// Volatile state on all servers
	// 我想把它改成持久化存储，并用它来实现选主，似乎用它来选主的话不必把它持久化
	// 它的更新的话只需要在appendEntry RPC里，
	// If leaderCommit > commitIndex,set commitIndex=min(leaderCommit,index of last new entry)
	// 不能用它来选主，因为leader的commitIndex传到follower需要RPC调用，一个log在leader提交了，但此时follower不知道这个log提交了
	commitIndex int
	// 上一个被实际执行的log的下标，用来确定哪些log已经实际执行了，为什么它不需要持久化存储啊？
	// 已经apply的日志再apply一遍不会出问题吗？还是应用层有防范措施？
	lastApplied int

	// Volatile state on leaders:
	// 正常情况下来讲，nextIndex[i] 应该等于 matchIndex[i] + 1
	// 当一个节点刚成为主的时候，其会初始化nextIndex为自己的last log index + 1，matchIndex为0
	// 之后会随着和每个follower的交互递减nextIndex，直到抵达follower真正的nextIndex，抵达之后
	// 设置matchIndex为nextIndex - 1
	// nextIndex是用来对每个follower进行探测的，matchIndex是用来决定哪个log该被提交的
	// matchIndex初始化为0的意义是等到确认了每个follower的log进度，再开始提交。正因其初始化为0
	// 所以log数组的下标才从1开始
	nextIndex  []int // 要发送给每一个节点的下一个log的下标
	matchIndex []int // 每一个节点最新接受成功的log的下标，用来决定log的提交

	// 上次重置计时器的时间，每次收到领导者的心跳和投票给candidate都会重置计时器
	electionTimerStart time.Time
	timeout            time.Duration // 超时时间 400 - 700

	gotVotes int // 在一轮投票中的得票数

	leaderID int // 当前该raft节点认为的leader是谁，-1代表不知道leader是谁

	// 用于触发leader发送心跳包和AppendEntries RPC的channel
	// 实际上我不需要他们，我只要在状态发送切换的时候，主动启动相应的go routine就好了
	// chBecomeLeader chan int
	// chNotLeader    chan int

	// snapshot的之后一个log的index，也就是下一个应该被snapshot的log的index，也可以看作snapshot的长度（包括dummy Log）
	// snapshot之前，0位是dummy log，snapshot之后，0位就是真的log了
	// snapshotIndex int
	// snapshot  []byte
	// snapshotTerm  int
	// 还有一个策略，那就是snaoshotIndex设置为snapshot的最后一个log的index，snapshot之后的log依旧是1-indexed的
	// snapshot之后，log的第0位就是snapshot的最后一个log, 这样就不需要snapshotTerm这个字段了
	// 并且GetLastLogIndex()，index()这些函数也能完美适配，就这么办了！
	snapshotIndex int
	snapshot      []byte

	// 用于通知apply协程对log进行apply的，当前运行机制是，commit协程每5ms跑一次，如果跑完之后有能apply的，就唤醒apply协程
	// 而不是双方各自睡一会抢锁。
	// 我真蠢啊，只有leader能commit，但follower也得apply啊
	// 或许可以leader和follower区别对待，在leader上启用这个channel，follower上就正常sleep？写起来加锁解锁太麻烦，太丑了，太怪了
	// applyNotify  chan int

	// 用于通知heartBear协程向follower发AppendEntries RPC的，每当上层调用Start，Append了一个log，就通过这个进行通知
	applyAppend chan int
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.logs) - 1 + rf.snapshotIndex
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// 把全局的log index转换为snapshot后的logs数组中的index, 返回这个index
// 我假设了index是合法的，如果是leader未snapshot的部分follower snapshot了，那append entries的时候，prevlogindex就会触碰
// 到follower的snapshot部分了，也就是不合法了，这种情况会发生吗？
func (rf *Raft) index(i int) int {
	return i - rf.snapshotIndex
}

// 把全局的log index转换为snapshot后的logs数组中的index，返回这个index下的log
func (rf *Raft) log(index int) LogEntry {
	return rf.logs[rf.index(index)]
}

// 每次状态切换都重置一些东西
// 1 raftState肯定要改变
// 2 什么状态切换需要重置election Timer呢？

// 在持有锁的情况下调用
// 当且仅当收到RPC request或reply中的term大于自己的时，被调用
func (rf *Raft) updateTermAndBecomeFollower(term int) {
	// 更新任期并转换为follower
	rf.currentTerm = term
	if rf.raftState == RaftLeader {
		// 如果是从leader变回follower，启动ticker goroutine/election Timer
		// rf.chNotLeader <- 1
		go rf.ticker()
	}
	rf.raftState = RaftFollower

	rf.leaderID = -1

	rf.voteFor = -1 // 当任期增大时，把voteFor变为none
	rf.persist()    // 持久化

	rf.gotVotes = 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.raftState == RaftLeader
}

func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 调用persist的时候应该是持有锁的
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// e.Encode(rf.raftState) // 不保存了，让raft节点从crash中恢复的时候都是follower
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotIndex)
	// e.Encode(rf.commitIndex)  // 它似乎不必持久化
	// e.Encode(rf.lastApplied)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var raftState RaftState  // 不保存了，让raft节点从crash中恢复的时候都是follower
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var snapshotIndex int
	// var commitIndex   uint64
	// var lastApplied int
	if /*d.Decode(&raftState) != nil || */ d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil || d.Decode(&snapshotIndex) != nil {
		// error
	} else {
		// rf.raftState = raftState
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		// rf.commitIndex = commitIndex
		// rf.lastApplied = lastApplied
		rf.snapshotIndex = snapshotIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 由使用raft的上层服务进行调用, 上层服务本身已经完成了快照化，只是把index和快照发到这里来
// index是为了让raft同步snapshotIndex，实际的快照数据是为了让leader同步给其他follower
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Server %d got a snapshot call with index %d", rf.me, index)
	if index < rf.snapshotIndex {
		return
	}
	if index > rf.GetLastLogIndex() {
		log.Fatalf("snapshot contains logs exceed the boundary")
	}
	rf.logs = rf.logs[rf.index(index):] // 包括这个snapshot的最后一个log，并把它作为0位的log
	rf.snapshotIndex = index            // 这一行不能和上一行调换顺序，否则index函数会出错，因为index利用了snapshotIndex来计算
	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// CommitIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// optimization that backs up nextIndex 所用到的字段
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // follower's log length
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int // not used, just sent all snapshot at once
	Data              []byte
	Done              bool
}

type InstallSnapshotArgsForDebug struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
}

func convert(arg *InstallSnapshotArgs) InstallSnapshotArgsForDebug {
	return InstallSnapshotArgsForDebug{
		Term:              arg.Term,
		LeaderID:          arg.LeaderID,
		LastIncludedIndex: arg.LastIncludedIndex,
		LastIncludedTerm:  arg.LastIncludedTerm,
	}
}

type InstallSnapshotReply struct {
	Term int
}

// 处理AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("server %d receive a AppendEntried RPC from server %d, args is %v", rf.me, args.LeaderID, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// 到此为止，对方的term不必自己小，可以认为对方就是current leader
	// 可以认为收到leader的心跳，重置electionTimer，并设置term和follower
	rf.resetElectionTimer()
	// 之前我在这里是无脑调用rf.updateTermAndBecomeFollower(args.Term)
	// 因为到这里已经确定对方是leader了，无论自己term如何，是不是follower，变成这样就好了
	// 但我忽略了这个函数还会把voteFor设置为-1，这就可能导致一次任期出现两个leader
	if args.Term > rf.currentTerm || rf.raftState != RaftFollower {
		rf.updateTermAndBecomeFollower(args.Term)
	}

	rf.leaderID = args.LeaderID // 设置leader ID

	if args.PrevLogIndex < rf.snapshotIndex {
		// 当prev log index落在当前节点的快照中
		reply.Success = false
		reply.XLen = rf.snapshotIndex + 1
		reply.XIndex = -1
		reply.XTerm = -1
		return
	}

	if args.PrevLogIndex > rf.GetLastLogIndex() || args.PrevLogTerm != rf.log(args.PrevLogIndex).Term {
		reply.Success = false
		if args.PrevLogIndex > rf.GetLastLogIndex() {
			// 自己的log太短了
			reply.XLen = len(rf.logs) + rf.snapshotIndex // 考虑到snapshot的长度
			reply.XIndex = -1
			reply.XTerm = -1
		} else {
			// 上一个log的term不匹配
			reply.XTerm = rf.log(args.PrevLogIndex).Term
			XIndex := args.PrevLogIndex
			for XIndex > rf.snapshotIndex && rf.log(XIndex).Term == rf.log(args.PrevLogIndex).Term {
				XIndex--
			}
			if XIndex == rf.snapshotIndex {
				// 这个term的log跨过了snapshotIndex
				// 让leader的处理走XLen的分支
				reply.XLen = rf.snapshotIndex + 1
				reply.XIndex = -1
				reply.XTerm = -1
			} else {
				reply.XIndex = XIndex + 1
				reply.XLen = -1
			}
		}
		return
	}

	// 到目前为止，确认了leader发过来的log index是和自己匹配的，把发过来的entries追加上
	reply.Success = true
	localLogIndex := args.PrevLogIndex + 1
	entriesIndex := 0
	for localLogIndex <= rf.GetLastLogIndex() && entriesIndex < len(args.Entries) &&
		rf.log(localLogIndex).Term == args.Entries[entriesIndex].Term {
		// 先定位到要追加entry的位置
		localLogIndex++
		entriesIndex++
	}
	if entriesIndex < len(args.Entries) {
		// 只要leader发过来的entries还有剩余，截断并追加
		rf.logs = rf.logs[0:rf.index(localLogIndex)]
		rf.logs = append(rf.logs, args.Entries[entriesIndex:]...)
	}

	// for entriesIndex < len(args.Entries) {
	// 	if localLogIndex > rf.GetLastLogIndex() {
	// 		rf.logs = append(rf.logs, args.Entries[entriesIndex])
	// 	} else {
	// 		rf.logs[localLogIndex] = args.Entries[entriesIndex]
	// 	}
	// 	entriesIndex++
	// 	localLogIndex++
	// }
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
	}
	// 持久化数据
	rf.persist()
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// 候选人任期小于自己的任期
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// 收到一个任期比自己大的任期，可能是正常的候选人自增任期，也可能由于网络问题自己落后了
		// 更新任期并转换为follower
		rf.updateTermAndBecomeFollower(args.Term)
		// rf.resetElectionTimer()
		reply.Term = rf.currentTerm
	}
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		// 已经给其他人投过票了
		reply.VoteGranted = false
		return
	}

	if rf.voteFor == args.CandidateId {
		// 自己之前为这个候选人投过票了，可能由于回复丢包了，对方没有收到
		reply.VoteGranted = true
		return
	}

	// 接下来判断对方的log是否比自己新

	// 这是我自己的判断对方的log比自己新的方法的方法
	// if args.CommitIndex >= rf.commitIndex {
	// 	reply.VoteGranted = true
	// 	return
	// }
	if args.LastLogTerm > rf.GetLastLogTerm() {
		log.Printf("server %d get a RequestVote RPC from server %d and vote for it on term %d", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId // 记录自己的投票对象
		rf.resetElectionTimer()       // 投出一票，重置election Timer
		rf.persist()                  // 持久化
		return
	} else if args.LastLogTerm < rf.GetLastLogTerm() {
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex >= rf.GetLastLogIndex() {
		log.Printf("server %d get a RequestVote RPC from server %d and vote for it on term %d", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.resetElectionTimer() // 投出一票，重置election Timer
		rf.persist()
		return
	}
	reply.VoteGranted = false
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("server %d handle a InstallSnapshot RPC from %d. args is %v", rf.me, args.LeaderID, convert(args))
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTermAndBecomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	// 相当于收到leader的心跳，重置timer; 设置leader ID
	rf.resetElectionTimer()

	rf.leaderID = args.LeaderID
	
	if args.LastIncludedIndex <= rf.snapshotIndex {
		// 本地已经有更新的snapshot了
		// rf.snapshot = args.Data
		// rf.persist()
		return
	}

	if rf.GetLastLogIndex() > args.LastIncludedIndex {
		// 当snapshot的内容是within当前raft的log中的时候，截断log，存储
		keepFrom := rf.index(args.LastIncludedIndex + 1)
		newLog := make([]LogEntry, 0, len(rf.logs)-keepFrom+1)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm})
		newLog = append(newLog, rf.logs[keepFrom:]...)
		rf.logs = newLog
	} else { // 小于等于
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	// 更新commitIndex和lastApplied（if needed）
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	// 持久化数据
	rf.persist()
	// 通知上层服务
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	resendTimes := 3
	var ok bool
	// 最多发三次投票请求
	for resendTimes > 0 {
		ok = rf.peers[server].Call("Raft.RequestVote", args, &reply)
		if ok {
			break
		}
		resendTimes--
	}
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 发出时的任期小于当前任期，旧时代的RPC的回复，不处理，直接丢弃
		return
	}
	if reply.Term < rf.currentTerm {
		// 收到小于当前任期的RPC回复，旧时代的RPC的回复，不处理，直接返回
		return
	}
	if reply.Term > rf.currentTerm {
		rf.updateTermAndBecomeFollower(reply.Term)
		return
	}
	// 只处理任期相同的RPC回复
	if rf.raftState == RaftCandidate && reply.VoteGranted && rf.gotVotes > 0 {
		rf.gotVotes++
		if rf.gotVotes > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if !rf.peers[server].Call("Raft.AppendEntries", args, &reply) {
		return
	}
	// 由于发送和收到RPC之间，状态可能发生变化，同时，由于我的策略是每次都向follower发送nextIndex
	// 后的全部logEntries，所以我采取了如下的幂等策略：when success，取应该值和实际值的最大值
	// when fail，取应该值和实际值的最小值
	matchIndexShouleBe := args.PrevLogIndex + len(args.Entries)
	nextIndexShouldBe := matchIndexShouleBe + 1
	// nextIndexWhenFail := args.PrevLogIndex

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 发出时的任期小于当前任期，旧时代的RPC的回复，不处理，直接丢弃
		return
	}
	if reply.Term < rf.currentTerm {
		// 收到小于当前任期的RPC回复，旧时代的RPC的回复，不处理，直接返回
		return
	}
	if reply.Term > rf.currentTerm {
		// 自己是旧时代的leader了
		rf.updateTermAndBecomeFollower(reply.Term)
		return
	}
	// 只处理任期相同的RPC回复
	// 在这个RPC发出和收到回复的过程中，leader可能又向这个follower发送了一次RPC并提前收到了回复
	// 也就是说，发送这个RPC时的状态和收到这个RPC时的状态可能已经不一样了
	if reply.Success {
		rf.nextIndex[server] = max(rf.nextIndex[server], nextIndexShouldBe)
		rf.matchIndex[server] = max(rf.matchIndex[server], matchIndexShouleBe)
	} else {
		// rf.nextIndex[server] = min(rf.nextIndex[server], nextIndexWhenFail)
		// 加快找到匹配的log
		if reply.XLen != -1 {
			rf.nextIndex[server] = reply.XLen
		} else if reply.XIndex != -1 {
			index := rf.lastIndexForTerm(reply.XTerm)
			if index == 0 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				rf.nextIndex[server] = index + 1
			}
		}
		if rf.nextIndex[server] == 0 {
			log.Printf("Now the server %d get a AppendEntriesReply from server %d, and the term is %d", rf.me, server, rf.currentTerm)
			log.Printf("The next index for server %d turn to 0", server)
			log.Printf("The args is %v", *args)
			log.Printf("The reply is: %v", reply)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if !rf.peers[server].Call("Raft.InstallSnapshot", args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 发出时的任期小于当前任期，旧时代的RPC的回复，不处理，直接丢弃
		return
	}
	if reply.Term < rf.currentTerm {
		// 收到小于当前任期的RPC回复，旧时代的RPC的回复，不处理，直接返回
		return
	}
	if reply.Term > rf.currentTerm {
		// 自己是旧时代的leader了
		rf.updateTermAndBecomeFollower(reply.Term)
		return
	}
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	if rf.nextIndex[server] < args.LastIncludedIndex+1 {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) lastIndexForTerm(term int) int {
	for i := rf.GetLastLogIndex(); i > rf.snapshotIndex; i-- {
		if rf.log(i).Term == term {
			return i
		}
	}
	return 0
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// Start是上层服务调用的接口，作用是提交一条命令到 Raft 日志，相当于客户端进行了写操作
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftState != RaftLeader {
		return 0, 0, false
	}
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.currentTerm})
	log.Printf("raft server %d receive a log: %v on term %d", rf.me, command, rf.currentTerm)

	rf.persist() // 持久化数据

	return rf.GetLastLogIndex(), rf.currentTerm, true
}

// 从candidate转换为leader，在收到RequestForVote RPC回复中调用
// 从检查票数到成为leader的过程必须中一直持有锁，否则可能会出现一个节点成为两次主
func (rf *Raft) becomeLeader() {
	log.Printf("server %d become leader on term %d", rf.me, rf.currentTerm)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.gotVotes <= len(rf.peers)/2 {
		log.Fatalf("Raft node %d become leader with votes less than majority", rf.me)
	}
	if rf.raftState != RaftCandidate {
		log.Fatalf("Raft node %d which is not a candidate bacomes a leader", rf.me)
	}
	// 还是有必要重置一下electionTimer的，因为可能有一个非常隐蔽的race condition：
	// 既成为leader之后，ticker中恰好超时了，于是开始了新一次选举, 或许我可以在startElection中规避这种情况
	rf.resetElectionTimer()
	rf.gotVotes = 0
	rf.raftState = RaftLeader
	rf.voteFor = -1 //

	rf.leaderID = rf.me

	rf.persist() // 持久化数据

	// 初始化nextIndex和matchIndex
	lastLogIndex := rf.GetLastLogIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	// 不能把leader自己的matchIndex置零，因为如果只有三个节点，且有一个follower断连，那么就拥有也提交不了
	// 如果没有节点断连，那置零没问题，但有断连，置零就会出问题
	rf.matchIndex[rf.me] = rf.GetLastLogIndex()

	// rf.chBecomeLeader <- 1 // 给sendAppendEntries goroutine发信号
	// 开启leader才有的goroutine
	go rf.heartBeat()
	go rf.commit()
}

// 只会在ticker中election timer超时的时候调用，被调用的时候是持有锁的
func (rf *Raft) startAElection() {
	// rf.mu.Lock()
	if rf.raftState == RaftLeader {
		return
	}
	rf.currentTerm++
	log.Printf("server %d starting election for new term: %d", rf.me, rf.currentTerm)
	rf.raftState = RaftCandidate
	// 投给自己
	rf.gotVotes = 1
	rf.voteFor = rf.me

	rf.persist() // 持久化数据

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm:  rf.GetLastLogTerm(),
	}
	// rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args)
	}
}

// 只会在RequestForVote和AppendEntries RPC中，以及成为leader，开始一次选举的时候被调用
// 被调用的时候应该是持有锁的
func (rf *Raft) resetElectionTimer() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.electionTimerStart = time.Now()
	rf.timeout = time.Duration(400+rand.Int63()%300) * time.Millisecond // 400 - 700 ms
}

// 一个检测是否应该开启选举的goroutinue，当自己不是leader的时候, 该计时器停用
// 从old leader故障到选出新leader的时间不应超过5秒
func (rf *Raft) ticker() {
	// for true {
	// 	// 当系统刚启动或者不再是leader的时候会往这里发一个信号
	// 	<-rf.chNotLeader

	// }

	// 需不需要在ticker一开始，也就是不是leader的时候，重置一下election Timer？
	// 一定要重置，因为定时器的计时逻辑是now 减掉上一次的 now，距离上一次重置定时器可能已经
	// 过去好久了，因为这可能是好几个任期之前，自己不是leader的时候的事情了
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.raftState == RaftLeader {
			rf.mu.Unlock()
			break
		}

		if time.Since(rf.electionTimerStart) > rf.timeout {
			rf.startAElection()
			// 当自己开启了一个选举，自然也要重置自己的计时器，不然就要连着开选举了
			rf.resetElectionTimer()
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 150 milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// leader发送AppendEntries RPC的和发送HeartBeat的goroutine
func (rf *Raft) heartBeat() {
	// for true {
	// 	// 当本raft节点成为leader之后会往这里发一个信号
	// 	<-rf.chBecomeLeader

	// }

	for true {
		rf.mu.Lock()
		if rf.raftState != RaftLeader {
			rf.mu.Unlock()
			break
		}

		for i, nextIndex := range rf.nextIndex {
			if i == rf.me {
				continue
			}
			// 如果index超了就会自动发送空entries-即心跳
			if nextIndex <= 0 {
				log.Printf("Now the leader is %d, and the term is: %d", rf.me, rf.currentTerm)
				log.Printf("The nextIndex for server %d is %d", i, nextIndex)
			}
			if nextIndex <= rf.snapshotIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.snapshotIndex,
					LastIncludedTerm:  rf.logs[0].Term,
					Offset:            0, // not used
					Data:              rf.snapshot,
					Done:              true,
				}
				log.Printf("Server %d send a InstallSnaoshot RPC to server %d, arg is: %v", rf.me, i, convert(&args))
				go rf.sendInstallSnapshot(i, &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.snapshotIndex,
					LastIncludedTerm:  rf.logs[0].Term,
					Offset:            0, // not used
					Data:              rf.snapshot,
					Done:              true,
				})
				continue
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.log(nextIndex - 1).Term,
				Entries:      rf.logs[rf.index(nextIndex):],
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(i, &args)
		}
		rf.mu.Unlock()

		time.Sleep(TIME_INTERVAL_TO_SEND_HEART_BEAT * time.Millisecond)
	}
}

// leader后台commit的 goroutine
func (rf *Raft) commit() {
	// for true {
	// 	// 当本raft节点成为leader之后会往这里发一个信号
	// 	<-rf.chBecomeLeader

	// }
	for true {
		rf.mu.Lock()
		if rf.raftState != RaftLeader {
			rf.mu.Unlock()
			break
		}

		// 更新leader自己的matchIndex
		rf.matchIndex[rf.me] = rf.GetLastLogIndex()

		// 检查是否可以提交
		// 把 matchIndex 排序，取中位数就是多数派已复制的最大位置
		sorted := append([]int{}, rf.matchIndex...)
		sort.Ints(sorted)

		n := len(sorted)
		majorityIndex := sorted[n/2] // 中位数

		// 检查这个位置的 entry 是否为当前任期，仅提交自己任期内的log
		if majorityIndex > rf.commitIndex && rf.log(majorityIndex).Term == rf.currentTerm {
			log.Printf("server %d commits log %d on term %d, it's value %v", rf.me, majorityIndex, rf.currentTerm, rf.log(majorityIndex).Command)
			rf.commitIndex = majorityIndex
		}

		rf.mu.Unlock()

		// time.Sleep(2 * time.Millisecond) // 睡个 5 ms
	}
}

// 后台不断尝试apply的 goroutine
func (rf *Raft) apply() {
	for {
		// rf.mu.Lock()
		// if rf.raftState == RaftLeader {
		// 	rf.mu.Unlock()
		// 	<- rf.applyNotify
		// 	rf.mu.Lock()
		// }
		rf.mu.Lock()
		start := rf.lastApplied + 1
		end := rf.commitIndex
		msgs := make([]raftapi.ApplyMsg, end-start+1)
		if rf.lastApplied < rf.commitIndex {
			for i := start; i <= end; i++ {
				msgs[i-start] = raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log(i).Command,
					CommandIndex: i,
				}
				// log.Printf("log %d has been applied on node %d, it's value %v", i, rf.me, rf.logs[i].Command)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			// log.Printf("server %d applied log %d", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
		}
		// if rf.raftState == RaftFollower {
		// 	time.Sleep(5 * time.Millisecond) // 睡个 5 ms
		// }
		// time.Sleep(2 * time.Millisecond) // 睡个 5 ms
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.raftState = RaftFollower
	rf.currentTerm = 0
	rf.voteFor = -1
	// 由于时log是1-indexed，所以在0下标处增加一个dummy entry
	rf.logs = make([]LogEntry, 0, 1)
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.gotVotes = 0

	rf.leaderID = -1

	rf.snapshotIndex = 0

	// rf.applyNotify = make(chan int)

	// rf.chBecomeLeader = make(chan int)
	// rf.chNotLeader = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	if rf.snapshotIndex != 0 {
		// 快照不为空，说明是从crash中恢复的，且crash之前也有快照。
		// 正确设置commitIndex和lastApplied
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
	}

	// start ticker goroutine to start elections

	// 在ticker之前初始化计时器
	rf.electionTimerStart = time.Now()
	rf.timeout = time.Duration(400+rand.Int63()%300) * time.Millisecond // 400 - 700 ms

	go rf.ticker()
	go rf.apply()
	// 发信号启动 ticker/election timer
	// rf.chNotLeader <- 1

	return rf
}
