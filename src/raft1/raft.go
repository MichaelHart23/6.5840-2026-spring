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

func init() {
	labgob.Register(LogEntry{})
	labgob.Register(map[string]string{}) // 如果 Command 是 map
	labgob.Register(int(0))              // 如果 Command 是 int
	labgob.Register("")                  // 如果 Command 是 string
}

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

const TIME_INTERVAL_TO_SEND_HEART_BEAT = 120 // ms

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
	commitIndex int
	// 上一个被实际执行的log的下标，用来确定哪些log已经实际执行了，我怎么感觉这个变量也要持久化存储啊？
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

	gotVotes int

	// 用于触发leader发送心跳包和AppendEntries RPC的channel
	// 实际上我不需要他们，我只要在状态发送切换的时候，主动启动相应的go routine就好了
	// chBecomeLeader chan int
	// chNotLeader    chan int
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// 每次状态切换都重置一些东西
// 1 raftState肯定要改变
// 2 什么状态切换需要重置election Timer呢？

// 在持有锁的情况下调用
// 当收到RPC request或reply中的term大于自己的时，被调用
func (rf *Raft) updateTermAndBecomeFollower(term int) {
	// 更新任期并转换为follower
	rf.currentTerm = term
	if rf.raftState == RaftLeader {
		// 如果是从leader变回follower，启动ticker goroutine/election Timer
		// rf.chNotLeader <- 1
		go rf.ticker()
	}
	rf.raftState = RaftFollower

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// // 调用persist的时候应该是持有锁的
	// // rf.mu.Lock()
	// // defer rf.mu.Unlock()
	// e.Encode(rf.raftState)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.voteFor)
	// e.Encode(rf.logs)
	// // e.Encode(rf.commitIndex)  // 它似乎不必持久化
	// e.Encode(rf.lastApplied)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var raftState RaftState
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	// var commitIndex   uint64
	var lastApplied int
	if d.Decode(&raftState) != nil || d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil || d.Decode(&lastApplied) != nil {
		// error
	} else {
		rf.raftState = raftState
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		// rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	//
	CommitIndex int
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
}

// 处理AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// 到此为止，对方的term不必自己小，可以认为对方就是current leader
	// 可以认为收到leader的心跳，重置electionTimer，并设置term和follower
	rf.resetElectionTimer()
	rf.updateTermAndBecomeFollower(args.Term)

	if args.PrevLogIndex > rf.GetLastLogIndex() || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}

	// 到目前为止，确认了leader发过来的log index是和自己匹配的，把发过来的entries追加上
	reply.Success = true
	localLogIndex := args.PrevLogIndex + 1
	entriesIndex := 0
	for localLogIndex <= rf.GetLastLogIndex() && entriesIndex < len(args.Entries) &&
		rf.logs[localLogIndex].Term == args.Entries[entriesIndex].Term {
		// 先定位到要追加entry的位置
		localLogIndex++
		entriesIndex++
	}
	for entriesIndex < len(args.Entries) {
		if localLogIndex > rf.GetLastLogIndex() {
			rf.logs = append(rf.logs, args.Entries[entriesIndex])
		} else {
			rf.logs[localLogIndex] = args.Entries[entriesIndex]
		}
		entriesIndex++
		localLogIndex++
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
	}
	// 持久化数据
	rf.persist()
}

// example RequestVote RPC handler.
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
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.resetElectionTimer() // 投出一票，重置election Timer
		rf.persist()
		return
	}
	reply.VoteGranted = false
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
	if reply.Term < args.Term {
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}
	// 由于发送和收到RPC之间，状态可能发生变化，同时，由于我的策略是每次都向follower发送nextIndex
	// 后的全部logEntries，所以我采取了如下的幂等策略：when success，取应该值和实际值的最大值
	// when fail，取应该值和实际值的最小值
	matchIndexShouleBe := args.PrevLogIndex + len(args.Entries)
	nextIndexShouldBe := matchIndexShouleBe + 1
	nextIndexWhenFail := args.PrevLogIndex

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < args.Term {
		// 收到小于当前任期的RPC回复，旧时代的RPC的回复，不处理，直接返回
		return
	}
	if reply.Term > rf.currentTerm { // 自己是旧时代的leader了
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
		// 后续会改成更快的得知该follower的log在哪里匹配的方式
		// 一种思路是：当matchIndex = 0，那么就在appendEntriesArgs里打一个标记
		// 发的是nextIndex前面的一堆entries，让follower自己找，找到了把匹配的index返回
		rf.nextIndex[server] = min(rf.nextIndex[server], nextIndexWhenFail)
	}
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftState != RaftLeader {
		return 0, 0, false
	}
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.currentTerm})

	return rf.GetLastLogIndex(), rf.currentTerm, true
}

// 从candidate转换为leader，在收到RequestForVote RPC回复中调用
// 从检查票数到成为leader的过程必须中一直持有锁，否则可能会出现一个节点成为两次主
func (rf *Raft) becomeLeader() {
	// log.Printf("[%d] become leader for term %d", rf.me, rf.currentTerm)
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
	// log.Printf("[%d] starting election for term %d", rf.me, rf.currentTerm)
	rf.raftState = RaftCandidate
	// 投给自己
	rf.gotVotes = 1
	rf.voteFor = rf.me

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.GetLastLogIndex(), LastLogTerm: rf.GetLastLogTerm(), CommitIndex: rf.commitIndex}
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

		for i, index := range rf.nextIndex {
			if i == rf.me {
				continue
			}
			// 如果index超了就会自动发送空entries-即心跳
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me,
				PrevLogIndex: index - 1, PrevLogTerm: rf.logs[index-1].Term,
				Entries: rf.logs[index:], LeaderCommit: rf.commitIndex}
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

		// 检查这个位置的 entry 是否为当前任期
		if majorityIndex > rf.commitIndex && rf.logs[majorityIndex].Term == rf.currentTerm {
			// log.Printf("log %d has been commited, it's value %v", majorityIndex, rf.logs[majorityIndex].Command)
			rf.commitIndex = majorityIndex
		}

		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond) // 睡个 300 ms
	}
}

// 后台不断尝试apply的 goroutine
func (rf *Raft) apply() {
	for {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			start := rf.lastApplied + 1
			end := rf.commitIndex
			msgs := make([]raftapi.ApplyMsg, end-start+1)
			for i := start; i <= end; i++ {
				msgs[i-start] = raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i,
				}
				// log.Printf("log %d has been replied on node %d, it's value %v", i, rf.me ,rf.logs[i].Command)
			}
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // 睡个 300 ms
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

	// Your initialization code here (3A, 3B, 3C).
	rf.raftState = RaftFollower
	rf.currentTerm = 0
	rf.voteFor = -1
	// 由于时log是1-indexed，所以在0下标处增加一个dummy entry
	rf.logs = make([]LogEntry, 0, 1)
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.gotVotes = 0

	// rf.chBecomeLeader = make(chan int)
	// rf.chNotLeader = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
