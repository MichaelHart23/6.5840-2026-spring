package kvsrv

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
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
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	if ok {
		switch reply.Err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrVersion:
			return rpc.ErrVersion
		case rpc.ErrNoKey:
			return rpc.ErrNoKey
		default:
			log.Fatalf("cline Put: unknown reply type")
		}
	}
	time.Sleep(100 * time.Millisecond)
	// 发送失败，或是请求没有送到，或是回复没有收到，重传
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrVersion:
				// 可能是之前的发送server的收到了，修改了，但server的回复没收到
				return rpc.ErrMaybe
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			default:
				log.Fatalf("cline Put: unknown reply type")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
