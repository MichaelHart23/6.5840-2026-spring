package rsm

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// RSM通知底层raft写入log的信息载体，也即是raft.Start的参数类型
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int // 用来标识每一个OP的标识符，用于在收到raft的apply msg的时候，确定是哪一个OP被applied了
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.

// rsm和上层service通信的接口
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// replicated state machine，是上层service(比如kv server)和底层raft交互的中间层
type RSM struct {
	mu           sync.Mutex
	me           int                   // 和底层raft中的me相同
	rf           raftapi.Raft          // 底层raft
	applyCh      chan raftapi.ApplyMsg // raft通过这个，通知rsm哪些log已经提交(applied)，或者snapshot了哪些log
	maxraftstate int                   // snapshot if log grows this big——当raft log变得这多多的时候，调用snapshot
	sm           StateMachine          // rsm和上层service通信的接口
	notify       map[int]chan Op       // 一个将log/OP与submit协程channel对应起来的map
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg, 100), // 用一个有容量的buffer
		sm:           sm,
		notify:       make(map[int]chan Op),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if persister.SnapshotSize() > 0 {
		// 快照恢复
		rsm.sm.Restore(persister.ReadSnapshot())
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) reader() {
	for {
		msg := <-rsm.applyCh
		rsm.mu.Lock()
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if ok {
				res := rsm.sm.DoOp(op.Req)
				op.Req = res // 把op的Req字段改成submit需要返回的结果
				// 无论此时是不是leader，都查看一下notify里有没有对应的chan，因为可能当时是leader，现在不再是了
				ch, ok := rsm.notify[msg.CommandIndex]
				if ok {
					// log.Printf("rsm server %d reader sending to notify[%d], op.Id=%d", rsm.me, msg.CommandIndex, op.Id)
					ch <- op
				}
				if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > rsm.maxraftstate {
					rsm.mu.Unlock()
					// 解锁执行，这不需要在锁着的时候执行
					rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
					continue
				}
			} else {
				log.Fatalf("In RSM %d, Convert Command to Op failed", rsm.me)
			}
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		} else {
			log.Fatalf("In RSM %d, ApplyMsg both not true", rsm.me)
		}
		rsm.mu.Unlock()
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
// 一个submit向raft写入一个log，reader收到这个log的apply信息后，通知submit返回
// 也就是说，需要有一个把一个submit协程和其写入的log(OP)联系到一起的方式
// 答案是：一个OP id与chan的map,
// 改成用logIndex匹配了，如果用nextID匹配的话，crash recovery之后nextID会清零，但raft重新apply的id还是用的之前的id，会冲突
// 那nextID还有作用吗？
// 需要nextID和logIndex双重鉴定，如果rsm调用了Start写入了一个log，在这个log提交之前，本server失去了leader身份，那么这个写入
// 却未提交的log就会被覆盖，如果仅通过logIndex做匹配的话，那就会把覆盖后的log匹配到覆盖之前的log。所以需要唯一id做进一步检验
// 但我通过在Submit收到msg后再检查本server是否为leader避免了这种情况的发生
// 而且id可能会出现巧合般重复的情况，要不我用随机数？
// 在submit里对leader的和term的检查太过严格了，想象这么一个场景：一个作为leader的server提交了一个log，但在这个log apply
// 之前失去了leader身份，之后这个server apply这个log的时候，submit应该返回成功，但在这种情况下会返回失败

// raft论文的section 8中提到了一个场景：一个被commit的写命令在返回给client之前，leader crash了，这样这条命令已经被执行，但
// client认为其没有被执行，于是重新写，于是就会出现一个命令被同时执行两次的情况。怎么达成幂等性呢？
// 为client的每一个request加一个id，似乎lab没有要求实现这个？因为它根本就每个register接口，我要自己
// 搞一个
// 不对，没有提供client向server建立连接/注册的接口，幂等性是通过version来达成的
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	id := int(rand.Int63()) // 一个非负唯一标识
	op := Op{Id: id, Req: req}
	logIndex, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		// 不是leader
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, rsm.rf.GetLeader()
	}
	ch := make(chan Op)
	// 用logIndex做匹配
	rsm.notify[logIndex] = ch
	rsm.mu.Unlock()

	for {
		select {
		case returnOp := <-ch:
			rsm.mu.Lock()
			defer rsm.mu.Unlock()

			delete(rsm.notify, logIndex)
			if returnOp.Id == -1 {
				// -1说明是被清理
				// log.Printf("rsm server %d Submit got poison pill for index %d", rsm.me, logIndex)
				return rpc.ErrWrongLeader, rsm.rf.GetLeader()
			}
			// curTerm, stillLeader := rsm.rf.GetState()
			// 如果前后id不匹配了，说明自己不再是leader了。任期变了，或不再是leader也是同样的情况
			if returnOp.Id != id {
				for _, otherCh := range rsm.notify {
					// 泪与血的debug教训：无阻塞发送position pill
					select {
					case otherCh <- Op{Id: -1}:
					default:
					}
				}
				return rpc.ErrWrongLeader, rsm.rf.GetLeader()
			}
			return rpc.OK, returnOp.Req
		case <-time.After(100 * time.Millisecond):
			curTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || curTerm != term {
				// 如果一个Submit迟迟没有收到raft的apply，且term发生了变化，
				// 那么，那有理由认为，这个log并没有被commit，所以返回给客户端WrongLeader
				rsm.mu.Lock()
				delete(rsm.notify, logIndex)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
