package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// 利用带版本信息的kv server实现分布式锁的原理：
// 每一把锁都是一个kv server上的一个键值对，锁的名字就是key值，value表示锁被谁持有，version信息是实现互斥的关键
// 客户端首先通过Get RPC调用得到该锁的version和value，通过value来判断该锁是否被占有
// 如果该锁未被占有，以该version尝试Put——获取该锁，如果在该 client Put之前有其他client获取到了锁
// 那Put会失败，因为version不匹配。反之则获取锁成功

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	clientID string // 用于标识该client的，获取锁就是把锁的value改成它
	lockName string // 该锁在server上的键值
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, clientID: kvtest.RandValue(8), lockName: lockname}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockName)
		switch err {
		case rpc.ErrNoKey:
			err = lk.ck.Put(lk.lockName, lk.clientID, 0)
			switch err {
			case rpc.OK:
				return
			case rpc.ErrVersion: // 说明put失败了，被其他人抢先了，重试
			case rpc.ErrMaybe: // 可能成功了，也可能失败了，重试一下就知道了
			case rpc.ErrNoKey: // version为0的put不可能出现nokey的回复
				log.Fatalf("Put version 0 get a ErrNoKey reply on lock: %v", lk.lockName)
			default: // 未知回复
				log.Fatalf("Unknown Get RPC reply on lock: %v", lk.lockName)
			}
		case rpc.OK:
			switch value {
			case "": // 无人持有该锁，尝试获取
				err = lk.ck.Put(lk.lockName, lk.clientID, version)
				switch err {
				case rpc.OK:
					return
				case rpc.ErrVersion: // 说明put失败了，被其他人抢先了，重试
				case rpc.ErrMaybe: // 可能成功了，也可能失败了，重试一下就知道了
				case rpc.ErrNoKey: // 之前的get已经说明有这个key了，说明出错了
					log.Fatalf("lock key %v disappear", lk.lockName)
				default:
					log.Fatalf("Unknown Put RPC reply on lock: %v", lk.lockName)
				}
			case lk.clientID: // 已经持有该锁了
				return
			default: // 锁被别人持有，重试
			}
		case rpc.ErrVersion:
			log.Fatalf("Get RPC get a ErrVersion reply on lock: %v", lk.lockName)
		default: // Get RPC调用只可能得到以上两种回复
			log.Fatalf("Get RPC get a Unknown reply on lock %v", lk.lockName)
		}
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockName)
		switch err {
		case rpc.OK:
			if value != lk.clientID { // 自己不持有该锁
				return
			}
			err = lk.ck.Put(lk.lockName, "", version)
			switch err {
			case rpc.OK:
				return
			case rpc.ErrVersion: // 自己正持有着这个锁呢，版本对不上，说明出错了
				log.Fatalf("release a hold lock and get a ErrVersion on lock: %v", lk.lockName)
			case rpc.ErrMaybe: // 可能成功了，也可能失败了，重试一下就知道了
			case rpc.ErrNoKey: // 之前的get已经说明有这个key了，说明出错了
				log.Fatalf("lock key %v disappear", lk.lockName)
			default:
				log.Fatalf("Unknown Put RPC reply on lock: %v", lk.lockName)
			}
		case rpc.ErrNoKey: // 这个锁不存在，自己自然不持有该锁
			return
		// Get RPC调用只可能得到以上两种回复
		case rpc.ErrVersion:
			log.Fatalf("Get RPC get a ErrVersion reply on lock: %v", lk.lockName)
		default:
			log.Fatalf("Get RPC get a Unknown reply on lock %v", lk.lockName)
		}
	}

}
