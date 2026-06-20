package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	Value    string
	Version  rpc.Tversion
}
type KVServer struct {
	mu      sync.Mutex
	table   map[string]VersionedValue
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.table = make(map[string]VersionedValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	versionedValue, ok := kv.table[key]
	if (ok) {
		reply.Err = rpc.OK
		reply.Value = versionedValue.Value
		reply.Version = versionedValue.Version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	versionedValue, ok := kv.table[key]
	if (ok) {
		if versionedValue.Version == args.Version {
			// 被修改过一次，版本号加一
			kv.table[key] = VersionedValue{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.table[key] = VersionedValue{args.Value, 1}
			reply.Err = rpc.OK
		} else {  // 想要修改一个不存在的key
			reply.Err = rpc.ErrNoKey
		}
	}
}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
