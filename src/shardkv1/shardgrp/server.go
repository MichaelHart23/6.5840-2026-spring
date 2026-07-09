package shardgrp

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ShardState int

const (
	Noshard ShardState = iota
	ValidShard
	FrozenShard
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid // 这个KVServer所属的group的gid

	mu    sync.Mutex
	table map[string]VersionedValue

	// num    shardcfg.Tnum                 // 在和controller的RPC通信中见过的最新的Tnum，也即版本号，小于这个版本号的RPC不处理
	shards map[shardcfg.Tshid]ShardState // 本kvserver管理哪些shard，以及这个shard是否被冻结
	// 如果只用一个全局num的话会出问题
	shardsNums  map[shardcfg.Tshid]shardcfg.Tnum // 每个shard见过的最大Num
}

func (kv *KVServer) DoOp(req any) any {
	// log.Printf("kv server %d on Group %d Do this OP: %v", kv.me, kv.gid, req)

	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&args)
	case rpc.PutArgs:
		return kv.doPut(&args)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(&args)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(&args)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(&args)
	default:
		log.Fatalf("KVServer.DoOp: req is neither GET nor PUT")
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.shards)
	e.Encode(kv.shardsNums)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[string]VersionedValue
	var shards map[shardcfg.Tshid]ShardState
	var shardsNums map[shardcfg.Tshid]shardcfg.Tnum
	if d.Decode(&table) != nil || d.Decode(&shards) != nil || d.Decode(&shardsNums) != nil {
		log.Fatalf("shardgrp server: Decode fail")
	} else {
		kv.table = table
		kv.shardsNums = shardsNums
		kv.shards = shards
	}
}

func (kv *KVServer) containShardAndNoFrozen(shid shardcfg.Tshid) bool {
	shardState, ok := kv.shards[shid]
	if !ok || shardState == Noshard || shardState == FrozenShard {
		return false
	}
	return true
}

func (kv *KVServer) containShard(shid shardcfg.Tshid) bool {
	shardState, ok := kv.shards[shid]
	if !ok || shardState == Noshard {
		return false
	}
	return true
}

// kvserver在Get/Put判断这个key属不属于自己管理的shard，或者在Install/Freeze/Delete里判断Num对不对的上。都不能直接在RPC
// 函数里判断，而是要借助于RSM层判断是否是leader，以及RSM提交给DoOp之后判断Num对不对的上
// 因为这个server不一定是主啊

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	var ok bool
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	*reply, ok = res.(rpc.GetReply)
	if !ok {
		log.Fatalf("a GET req get a reply is not GetReply")
	}
}

func (kv *KVServer) doGet(args *rpc.GetArgs) rpc.GetReply {
	// log.Printf("kv server %d on Group %d Do Get: %v", kv.me, kv.gid, *args)
	reply := rpc.GetReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.containShardAndNoFrozen(shardcfg.Key2Shard(args.Key)) {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	vv, ok := kv.table[args.Key]
	if ok {
		reply.Value = vv.Value
		reply.Version = vv.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	var ok bool
	err, res := kv.rsm.Submit(*args)
	// log.Printf("server %d Put Submit returned err=%v, res=%v", kv.me, err, res)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	*reply, ok = res.(rpc.PutReply)
	if !ok {
		log.Fatalf("a PUT req get a reply is not PutReply")
	}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) rpc.PutReply {
	// log.Printf("kv server %d on Group %d Do Put: %v", kv.me, kv.gid, *args)
	reply := rpc.PutReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.containShardAndNoFrozen(shardcfg.Key2Shard(args.Key)) {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

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
}

func (kv *KVServer) packShardData(shard shardcfg.Tshid) []byte {
	data := make(map[string]VersionedValue)
	for k, v := range kv.table {
		if shardcfg.Key2Shard(k) == shard {
			data[k] = v
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(data)
	return w.Bytes()
}

func (kv *KVServer) unpackShardData(data []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var kvmap map[string]VersionedValue
		if d.Decode(&kvmap) != nil {
			// 解码失败
			log.Fatalf("doInstallShard: failed to decode shard state")
		}
		// merge 进自己的 table
		for k, v := range kvmap {
			// 写入table，也可能是覆盖？
			kv.table[k] = v
		}
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	var ok bool
	err, res := kv.rsm.Submit(*args)
	// log.Printf("server %d FreezeShard Submit returned err=%v, res=%v", kv.me, err, res)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	*reply, ok = res.(shardrpc.FreezeShardReply)
	if !ok {
		log.Fatalf("a FreezeShard req get a reply is not FreezeShardReply")
	}
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	log.Printf("kv server %d on Group %d Do FreezeShard: %v", kv.me, kv.gid, *args)
	reply := shardrpc.FreezeShardReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shid := args.Shard
	if args.Num < kv.shardsNums[shid] {
		reply.Num = kv.shardsNums[shid]
		reply.Err = rpc.OK
		return reply
	}
	kv.shardsNums[shid] = args.Num
	reply.Num = kv.shardsNums[shid]
	shardState, ok := kv.shards[shid]
	if !ok || shardState == Noshard {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	if shardState == FrozenShard {
		// 之前已经冻结过了，可能是控制器重试（上次的回复丢包了）
		// 应该把数据和 OK 重新返回一次，此路径不做处理
	}

	if shardState != FrozenShard {
		kv.shards[shid] = FrozenShard
	}

	data := kv.packShardData(shid)
	reply.State = data
	reply.Err = rpc.OK
	return reply
}

// Install the supplied state for the specified shard.
// 用于通知本kvserver，自己负责的shard
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	var ok bool
	err, res := kv.rsm.Submit(*args)
	// log.Printf("server %d FreezeShard Submit returned err=%v, res=%v", kv.me, err, res)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	*reply, ok = res.(shardrpc.InstallShardReply)
	if !ok {
		log.Fatalf("a InstallShard req get a reply is not shardrpc.InstallShardReply")
	}
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	log.Printf("kv server %d on Group %d Do InstallShard, Shid %d, Num: %d", kv.me, kv.gid, args.Shard, args.Num,)
	reply := shardrpc.InstallShardReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shid := args.Shard
	if args.Num <= kv.shardsNums[shid] {
		reply.Err = rpc.OK
		return reply
	}
	kv.shardsNums[shid] = args.Num
	shardState, ok := kv.shards[shid]
	if !ok || shardState != Noshard {
		if shardState == FrozenShard {
			log.Fatalf("doInstallShard: Install a shard that is frozen")
		}
		if shardState == ValidShard {
			// 直接覆盖可以吗？
		}
	}

	kv.unpackShardData(args.State)

	kv.shards[shid] = ValidShard
	reply.Err = rpc.OK
	return reply
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	var ok bool
	err, res := kv.rsm.Submit(*args)
	// log.Printf("server %d FreezeShard Submit returned err=%v, res=%v", kv.me, err, res)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		reply.LeaderHint, ok = res.(int)
		if !ok {
			reply.LeaderHint = -1 // Submit可能在轮询的分支里返回nil，-1表示不知道
		}
		return
	}
	*reply, ok = res.(shardrpc.DeleteShardReply)
	if !ok {
		log.Fatalf("a DeleteShard req get a reply is not DeleteShardReply")
	}
}

func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	log.Printf("kv server %d on Group %d Do DeleteShard: %v", kv.me, kv.gid, *args)
	reply := shardrpc.DeleteShardReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shid := args.Shard
	// 唯独DeleteShard的判断是 <, 其余两个的判断都是<=，因为这个Num在Freeze的时候已经见过了
	if args.Num < kv.shardsNums[shid] {
		reply.Err = rpc.OK
		return reply
	}
	kv.shardsNums[shid] = args.Num
	shardState, ok := kv.shards[shid]
	if !ok || shardState == Noshard {
		reply.Err = rpc.OK
		return reply
	}
	if shardState == ValidShard {
		log.Fatalf("doDeleteShard: delete a valid shard")
	}
	// shardState == frozen shard
	kv.deleteShard(shid)
	kv.shards[shid] = Noshard
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) deleteShard(shid shardcfg.Tshid) {
	for key := range kv.table {
		if shardcfg.Key2Shard(key) == shid {
			delete(kv.table, key)
		}
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:    gid,
		me:     me,
		table:  make(map[string]VersionedValue),
		shardsNums: make(map[shardcfg.Tshid]shardcfg.Tnum),
		shards: make(map[shardcfg.Tshid]ShardState),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	if gid == shardcfg.Gid1 {
		// 是第一个初始化的server，拥有所有shard
		for i := range shardcfg.NShards {
			kv.shards[shardcfg.Tshid(i)] = ValidShard
		}
	}

	for i := range shardcfg.NShards {
			kv.shardsNums[shardcfg.Tshid(i)] = 0
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
