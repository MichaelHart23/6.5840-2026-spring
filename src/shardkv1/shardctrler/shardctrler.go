package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"slices"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	CurConfigKey = "CurConfig"
	NewConfigKey = "NewConfig"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt            *tester.Clnt
	kvtest.IKVClerk // 嵌入接口

	killed int32 // set by Kill()
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0) // 这个srv应该就是一个kv server，而非kvraft
	// 把接口赋值，之后sck实例就可以直接调用接口所要求的函数，直接调用的Put和Get指向的是kvsrv，而非kvraft
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	sck.doRecovery()
}

func (sck *ShardCtrler) putOnce(key string, value string, version rpc.Tversion) bool {
	err := sck.Put(key, value, version)
	switch err {
	case rpc.ErrMaybe:
		val, ver, err := sck.Get(key)
		if err == rpc.OK && val == value && ver == version {
			return true
		}
		return false
	case rpc.OK:
		return true
	case rpc.ErrVersion:
		return false
	default:
		log.Fatalf("ShardCtrler putOnce: Unkonwn error type")
	}
	// not reach
	return false
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 对于错误，暂且直接报错
	if !sck.putOnce(CurConfigKey, cfg.String(), 0) {
		log.Fatalf("ShardCtrler InitConfig(): Put fail")
	}
}

func (sck *ShardCtrler) doRecovery() {
	curConfigStr, curVer, err := sck.Get(CurConfigKey)
	if err == rpc.ErrNoKey {
		log.Fatal("cur config not found")
	}
	curConfig := shardcfg.FromString(curConfigStr)
	newConfigStr, newVer, err := sck.Get(NewConfigKey)
	if err == rpc.ErrNoKey {
		// 当server里没有NewConfig的时候，一定不需要恢复
		newVer = 0
		return
	}

	newConfig := shardcfg.FromString(newConfigStr)

	if curVer-newVer == 1 && curConfig.Num == newConfig.Num {
		// 无需恢复
		return
	}
	if curVer-newVer != 1 && curVer-newVer != 0 {
		log.Fatalf("invalid config state with cur version: %v; next version: %v", curVer, newVer)
	}
	log.Printf("doRecovery: Start")
	sck.changConfig(curConfig, newConfig)
	sck.putOnce(CurConfigKey, newConfigStr, curVer)
	log.Printf("doRecovery: success")
}

// 不做该不该复原的判断，只把config从old变为new
func (sck *ShardCtrler) changConfig(curConfig, newConfig *shardcfg.ShardConfig) {
	clients2Cur := make(map[tester.Tgid]*shardgrp.Clerk)
	clients2New := make(map[tester.Tgid]*shardgrp.Clerk)

	for shid := shardcfg.Tshid(0); shid < shardcfg.NShards; shid++ {
		curGid, curServers, _ := curConfig.GidServers(shid)
		newGid, newServers, _ := newConfig.GidServers(shid)
		if curGid != newGid || !slices.Equal(curServers, newServers) {
			client2Cur, ok := clients2Cur[curGid]
			if !ok {
				client2Cur = shardgrp.MakeClerk(sck.clnt, curServers)
				clients2Cur[curGid] = client2Cur
			}
			client2New, ok := clients2New[newGid]
			if !ok {
				client2New = shardgrp.MakeClerk(sck.clnt, newServers)
				clients2New[newGid] = client2New
			}
			// 不用管返回的err，因为server那一端会正确处理重读的请求
			data, _ := client2Cur.FreezeShard(shid, newConfig.Num)
			// if err == rpc.ErrWrongGroup {
			// 	log.Fatalf("changeConfig: FreezeShard get a WrongGroup Reply on shard %d, with new num %d", shid, newConfig.Num)
			// }
			// if data == nil {
			// 	continue
			// }
			client2New.InstallShard(shid, data, newConfig.Num)
			// if err == rpc.ErrWrongGroup {
			// 	log.Fatalf("changeConfig: InstallShard get a WrongGroup Reply on shard %d, with new num %d", shid, newConfig.Num)
			// }
			client2Cur.DeleteShard(shid, newConfig.Num)
			// if err == rpc.ErrWrongGroup {
			// 	log.Fatalf("changeConfig: DeleteShard get a WrongGroup Reply on shard %d, with new num %d", shid, newConfig.Num)
			// }
		}
	}
	// for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
	// 	oldGid := curConfig.Shards[sh]
	// 	newGid := newConfig.Shards[sh]
	// 	if oldGid == newGid {
	// 		continue
	// 	}
	// 	srcck := shardgrp.MakeClerk(sck.clnt, curConfig.Groups[oldGid])
	// 	state, _ := srcck.FreezeShard(sh, newConfig.Num)
	// 	dstck := shardgrp.MakeClerk(sck.clnt, newConfig.Groups[newGid])
	// 	dstck.InstallShard(sh, state, newConfig.Num)
	// 	srcck.DeleteShard(sh, newConfig.Num)
	// }
}

func (sck *ShardCtrler) printConfig() {
	curConfigStr, curVer, err := sck.Get(CurConfigKey)
	if err == rpc.ErrNoKey {
		log.Fatal("cur config not found")
	}
	newConfigStr, newVer, err := sck.Get(NewConfigKey)

	curConfig := shardcfg.FromString(curConfigStr)

	log.Printf("CurConfig Num: %d, Version: %d", curConfig.Num, curVer)
	if err == rpc.ErrNoKey {
		log.Printf("No NewConfig")
	} else {
		newConfig := shardcfg.FromString(newConfigStr)
		log.Printf("NewConfig Num: %d, Version: %d", newConfig.Num, newVer)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// this involves moving shards to new shardgrps that are joining the system
// and moving shards away from shardgrps that are leaving the system
// 要分析前后config包含的group有哪些差异，然后把数据在group之间进行移动
func (sck *ShardCtrler) ChangeConfigTo(newConfig *shardcfg.ShardConfig) {
	// time.Sleep(4 * time.Second)
	// sck.printConfig()
	// time.Sleep(4 * time.Second)

	log.Printf("ChangConfigTo Start")
	curConfigStr, curVer, err := sck.Get(CurConfigKey)
	if err == rpc.ErrNoKey {
		log.Fatalf("cur config not found")
	}
	curConfig := shardcfg.FromString(curConfigStr)
	if newConfig.Num <= curConfig.Num {
		return
	}

	// originNewConfig, _, err := sck.Get(NewConfigKey)
	// if (err == rpc.OK && newConfig.Num <= shardcfg.FromString(originNewConfig).Num) {
	// 	return
	// }
	
	// 如果CAS Put失败，说明有其他Controller抢占了, 或者这个Config过时了
	if !sck.putOnce(NewConfigKey, newConfig.String(), curVer-1) {
		log.Printf("ChangeConfigTo: CAS Put fail")
		return
	}
	
	sck.changConfig(curConfig, newConfig)
	sck.putOnce(CurConfigKey, newConfig.String(), curVer)
	log.Printf("ChangConfigTo success")

	// time.Sleep(4 * time.Second)
	// sck.printConfig()
	// time.Sleep(4 * time.Second)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _, err := sck.Get(CurConfigKey)
	if err == rpc.ErrNoKey {
		// 没有config该咋办呢？报错还是返回空的config？
		log.Fatalf("cur config not found")
	}
	return shardcfg.FromString(config)
}
