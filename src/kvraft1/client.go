package kvraft

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:    clnt,
		servers: servers,
		leader:  0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
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
			default:
			}
		} else {
			// 给这个server发送失败，随机找另一个server发，不能吊死在一颗树上
			ck.leader = rand.Intn(len(ck.servers))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	rpcTimes := 0
	for {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		
		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				if reply.LeaderHint < 0 {
					ck.leader = rand.Intn(len(ck.servers))
				} else {
					ck.leader = reply.LeaderHint
				}
				continue   // 不sleep，因为发送和接受成功了
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
