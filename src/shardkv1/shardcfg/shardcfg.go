package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	tester "6.5840/tester1"
)

type Tshid int
type Tnum int

const (
	NShards  = 12 // The number of shards.
	NumFirst = Tnum(1)
)

const (
	Gid1 = tester.Tgid(1)
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func Key2Shard(key string) Tshid {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := Tshid(Tshid(h.Sum32()) % NShards)
	return shard
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// 一共有NShards个分片，这些分片由几个server group承载
type ShardConfig struct {
	// 相当于config的版本号，每次配置改变, 也就是Join和Leave函数被调用，它都会自增1
	Num    Tnum                     // config number
	Shards [NShards]tester.Tgid     // shard -> gid
	Groups map[tester.Tgid][]string // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		Groups: make(map[tester.Tgid][]string),
	}
	return c
}

func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

// mostgroup, mostn, leastgroup, leastn
// 分析哪个group承载着最多的shards，哪个group承载着最少的shards
func analyze(c *ShardConfig) (tester.Tgid, int, tester.Tgid, int) {
	counts := map[tester.Tgid]int{}
	// 统计每个group有几个shards
	for _, gid := range c.Shards {
		counts[gid] += 1
	}

	mostShardsNum := -1
	var mostGroup tester.Tgid = -1
	leastShardsNum := 257
	var leastGroup tester.Tgid = -1
	// Enforce deterministic ordering, map iteration
	// is randomized in go
	// 收集所有gid
	groups := make([]tester.Tgid, len(c.Groups))
	i := 0
	for gid := range c.Groups {
		groups[i] = gid
		i++
	}
	slices.Sort(groups)
	// 找到shards最多/最少的group，及其shards数
	for _, gid := range groups {
		if counts[gid] < leastShardsNum {
			leastShardsNum = counts[gid]
			leastGroup = gid
		}
		if counts[gid] > mostShardsNum {
			mostShardsNum = counts[gid]
			mostGroup = gid
		}
	}

	return mostGroup, mostShardsNum, leastGroup, leastShardsNum
}

// return GID of group with least number of
// assigned shards.
func least(c *ShardConfig) tester.Tgid {
	_, _, leastGroup, _ := analyze(c)
	return leastGroup
}

// balance assignment of shards to groups.
// modifies c.
func (c *ShardConfig) Rebalance() {
	// if no groups, un-assign all shards
	if len(c.Groups) < 1 {
		for s, _ := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// assign all unassigned shards
	for s, gid := range c.Shards {
		_, ok := c.Groups[gid]
		if ok == false {
			// 这个group已经不存在了，把这个group的shard分配给拥有shards最少的group
			leastGroup := least(c)
			c.Shards[s] = leastGroup
		}
	}

	// move shards from most to least heavily loaded
	for {
		mostGroup, mostShardsNum, leastGroup, leastShardsNum := analyze(c)
		if mostShardsNum < leastShardsNum+2 {
			// 最多和最少差距不大了
			break
		}
		// move 1 shard from mostGroup to leastGroup
		for s, g := range c.Shards {
			if g == mostGroup {
				c.Shards[s] = leastGroup
				break
			}
		}
	}
}

// 新加入一些group：kvraft组
func (cfg *ShardConfig) Join(servers map[tester.Tgid][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			// 已经存在这个gid的group了
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.Groups { // 遍历当前已经存在的group
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 { // 判断有没有server重名
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// new GID
		// modify cfg to reflect the Join()
		cfg.Groups[gid] = servers // 将这组kvraft加入
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

// 有一些group要离开
func (cfg *ShardConfig) Leave(gids []tester.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// already no GID!
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// modify op.Config to reflect the Leave()
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *ShardConfig) JoinBalance(servers map[tester.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) LeaveBalance(gids []tester.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

// 输入一个分片号，给出这个分片所在的group的gid，kvraft群
func (cfg *ShardConfig) GidServers(sh Tshid) (tester.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

// 判断一个group是否是成员
func (cfg *ShardConfig) IsMember(gid tester.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// are the groups as expected?
	// 这个cfg里的group和传入的groups要包含同样的gid
	for _, gid := range groups {
		_, ok := cfg.Groups[gid]
		if ok != true {
			fatalf(t, "missing group %v", gid)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, gid := range cfg.Shards {
			_, ok := cfg.Groups[gid]
			if ok == false {
				// 有一个shard对应的group不存在
				fatalf(t, "shard %v -> invalid group %v", s, gid)
			}
		}
	}

	// more or less balanced sharding?
	// 找出shards最多的group和最少的group，看它们的数量差是否超过一
	counts := map[tester.Tgid]int{}
	for _, gid := range cfg.Shards {
		counts[gid] += 1
	}
	min := 257
	max := 0
	for gid := range cfg.Groups {
		if counts[gid] > max {
			max = counts[gid]
		}
		if counts[gid] < min {
			min = counts[gid]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
