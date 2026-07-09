package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

// 这个client是用来和整个集群交互的，它需要创建/调用 shardrpc里的client来进行读写
// shardrpc里的client要处理找leader，而这个不用，只用处理找到含特定shard的group就好
type Clerk struct {
	clnt   *tester.Clnt
	sck    *shardctrler.ShardCtrler
	rcks   map[tester.Tgid]*shardgrp.Clerk
	config *shardcfg.ShardConfig  // 缓存一个config，不用每次Get/Put都重新获取，只在WrongGroup的时候重新获取
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)

	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// 第二个参数表明是否要对ck.rcks进行更新，当收到WrongGroup恢复时，设置这个为真
func (ck *Clerk) getClientByShard(shid shardcfg.Tshid, isUpdate bool) *shardgrp.Clerk {
	if ck.config == nil || isUpdate {
		ck.config = ck.sck.Query()
	}
	gid, servers, ok := ck.config.GidServers(shid)
	if !ok {
		log.Fatalf("getClientByKey: no group for shard: %v", shid)
	}
	client, ok := ck.GetClerk(gid)
	if !ok || isUpdate {
		// 当前没有和这个group通信的client
		ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
		client = ck.rcks[gid]
	}
	return client
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shid := shardcfg.Key2Shard(key)
	isUpdate := false
	for {
		// log.Printf("ON Get Loop In ShardKV client")
		client := ck.getClientByShard(shid, isUpdate)
		val, ver, err := client.Get(key)
		switch err {
		case rpc.OK:
			return val, ver, rpc.OK
		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey
		case rpc.ErrWrongGroup:
			isUpdate = true
		default:
			log.Fatalf("shardkv client Get() got invalid err reply: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shid := shardcfg.Key2Shard(key)
	isUpdate := false
	for {
		client := ck.getClientByShard(shid, isUpdate)
		err := client.Put(key, value, version)
		switch err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrNoKey:
			return rpc.ErrNoKey
		case rpc.ErrVersion:
			if isUpdate {
				// 已经不是第一次发送了，可能是回复丢包了
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		case rpc.ErrMaybe:
			return rpc.ErrMaybe
		case rpc.ErrWrongGroup:
			isUpdate = true
		default:
			log.Fatalf("shardkv client Put() got invalid err reply: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
