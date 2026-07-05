package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	GetLeader() (int)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// You'll find the Snapshot fields useful later in the lab.
// Exactly one of CommandValid and SnapshotValid should be true.

// raft applies log by this，收到InstallSnapshot RPC的时候也调用这个
//raft通过这个，通知上层哪些log已经提交，或者snapshot了哪些log
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 当follower收到来自leader的InstallSnapshot RPC后，通过以下参数通知上层
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
