package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	mu    sync.Mutex
	table map[string]VersionedValue
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// DoOp需要返回什么呢？直接返回对应的Reply。
// Reply里的Err和submit返回的Err作用不同，如果submit返回的Err是WrongLeader，那就给client返回WrongLeader
// 如果submit返回的Err是OK，那就直接返回DoOp返回的reply，也就是submit返回的第二个参数
func (kv *KVServer) DoOp(req any) any {
	// log.Printf("DoOp: req type = %T, value = %+v", req, req)
	// 不用加锁？，因为不太会出现reader协程在要给DoOp执行完之前就重新调用一次DoOp的可能
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("kv server %d Do this OP: %v", kv.me, req)

	switch args := req.(type) {
	case rpc.GetArgs:
		vv, ok := kv.table[args.Key]
		if ok {
			return rpc.GetReply{
				Value:   vv.Value,
				Version: vv.Version,
				Err:     rpc.OK,
			}
		}
		return rpc.GetReply{
			Err: rpc.ErrNoKey,
		}
	case rpc.PutArgs:
		key := args.Key
		versionedValue, ok := kv.table[key]
		if ok {
			if versionedValue.Version == args.Version {
				// 被修改过一次，版本号加一
				kv.table[key] = VersionedValue{args.Value, args.Version + 1}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			} else {
				return rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		} else {
			if args.Version == 0 {
				kv.table[key] = VersionedValue{args.Value, 1}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			} else {
				// 想要修改一个不存在的key
				return rpc.PutReply{
					Err: rpc.ErrNoKey,
				}
			}
		}
	default:
		log.Fatalf("KVServer.DoOp: req is neither GET nor PUT")
	}

	return nil
}

// 把当前KVServer的数据/状态快照化, 只存table可以吗？
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[string]VersionedValue
	if d.Decode(&table) != nil {
		// error
	} else {
		kv.table = table
	}
}

// 当server不是leader时，告诉client自己认为的leader。
// 这个功能的实现方法(暂时没实现)：
// 1 在GetReply和PutReply都加上一个term的字段
// 2 raft的GetState 和 Start 都返回其认为的leader。或专门有一个GetLeader的接口
// 3 Submit返回WrongLeader时，第二个参数返回leaderID
// 4 当Server的Get和Put收到Submit的WrongLeader时，把一起返回的leaderID写入GetReply或PutReply

// Get和Put是不用持有锁的，读写操作都在DoOp里进行了

// 由于要实现线性一致性，所以get只能返回已提交的数据
// 读操作也要写log吗？要的
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 通过submit拿到: 1 是否找错leader; 2 DoOp的返回值，也即reply本身
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		var ok bool
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	var ok bool
	*reply, ok = res.(rpc.GetReply)
	if !ok {
		log.Fatalf("a GET req get a reply is not GetReply")
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 通过submit拿到: 1 是否找错leader; 2 DoOp的返回值，也即reply本身
	err, res := kv.rsm.Submit(*args)
	log.Printf("server %d Put Submit returned err=%v, res=%v", kv.me, err, res)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		var ok bool
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	var ok bool
	*reply, ok = res.(rpc.PutReply)
	if !ok {
		log.Fatalf("a PUT req get a reply is not PutReply")
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
// 为什么这个函数需要返回一个any切片？
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{
		me:    me,
		table: make(map[string]VersionedValue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
