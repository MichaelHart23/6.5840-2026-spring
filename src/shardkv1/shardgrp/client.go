package shardgrp

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// 这个client是用来和特定group交互的
type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

const MaxRPCTimes = 25

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for i := 0; i < MaxRPCTimes; i++ {
		// log.Printf("ON Get In Shardgrp client")
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue // 不sleep，因为发送和接受成功了
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				return "", 0, rpc.ErrWrongGroup
			default:
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	rpcTimes := 0
	for {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue // 不sleep，因为发送和接受成功了
			case rpc.OK:
				return rpc.OK
			case rpc.ErrVersion:
				if rpcTimes == 0 {
					return rpc.ErrVersion
				}
				// 在得知对方是leader的情况下，已经不是第一次发送RPC了，有可能上一次的RPC server写入了，但回复丢包了
				return rpc.ErrMaybe
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
				log.Fatalf("cline Put: unknown reply type")
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
			// 发送失败了，这个指令可能已经执行，回复丢包了
			rpcTimes++
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	
	for {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue // 不sleep，因为发送和接受成功了
			case rpc.OK:
				return reply.State, rpc.OK
			case rpc.ErrWrongGroup:
				return reply.State, rpc.ErrWrongGroup
			default:
				log.Fatalf("client FreezeShard: unknown reply type: %v", reply.Err)
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
			// 发送失败了，这个指令可能已经执行，回复丢包了
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue // 不sleep，因为发送和接受成功了
			case rpc.OK:
				return rpc.OK
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
				log.Fatalf("client InstallShard: unknown reply type: %v", reply.Err)
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
			// 发送失败了，这个指令可能已经执行，回复丢包了
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue // 不sleep，因为发送和接受成功了
			case rpc.OK:
				return rpc.OK
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
				log.Fatalf("client DeleteShard: unknown reply type: %v", reply.Err)
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
			// 发送失败了，这个指令可能已经执行，回复丢包了
		}
		time.Sleep(10 * time.Millisecond)
	}
}
