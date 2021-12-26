package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	resultChan           map[int]chan int
	lastAppliedIndex     int
	lastAppliedTerm      int
	lastAppliedRequestId map[int64]int64

	configs []Config // indexed by config num
}

type Op struct {
	Name  string
	Join  *JoinArgs
	Leave *LeaveArgs
	Move  *MoveArgs
}

func rebalance(config *Config) {
	groupNum := len(config.Groups)
	if groupNum == 0 {
		for i, _ := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	shardByGid := make(map[int][]int)
	unassigned := []int{}
	for shard, gid := range config.Shards {
		if gid == 0 {
			unassigned = append(unassigned, shard)
		} else if _, ok := config.Groups[gid]; !ok {
			unassigned = append(unassigned, shard)
		} else {
			shardByGid[gid] = append(shardByGid[gid], shard)
		}
	}
	// sort gid by count
	type Group struct {
		gid    int
		shards []int
	}
	groups := []Group{}
	for gid, _ := range config.Groups {
		groups = append(groups, Group{gid, shardByGid[gid]})
	}
	sort.Slice(groups, func(i, j int) bool {
		l1, l2 := len(groups[i].shards), len(groups[j].shards)
		if l1 != l2 {
			return l1 < l2
		}
		return groups[i].gid < groups[j].gid
	})
	floor := NShards / groupNum
	ceiling := (NShards + groupNum - 1) / groupNum
	floorCount := groupNum - NShards%groupNum
	targetCount := make([]int, groupNum)
	for i := 0; i < floorCount; i++ {
		targetCount[i] = floor
	}
	for i := floorCount; i < groupNum; i++ {
		targetCount[i] = ceiling
	}
	for i, j := 0, groupNum-1; i < floorCount; i++ {
		for len(groups[i].shards) < floor {
			if len(unassigned) > 0 {
				a := unassigned[0]
				unassigned = unassigned[1:]
				config.Shards[a] = groups[i].gid
				groups[i].shards = append(groups[i].shards, a)
			} else if len(groups[j].shards) > targetCount[j] {
				b := groups[j].shards
				groups[i].shards = append(groups[i].shards, b[0])
				groups[j].shards = b[1:]
				config.Shards[b[0]] = groups[i].gid
			} else if len(groups[j].shards) == targetCount[j] && j-1 > i {
				j--
			} else {
				panic(fmt.Sprintf("never happen: j=%d floorCount=%d  groups=%v", j, floorCount, groups))
			}
		}
	}
}

func (sc *ShardCtrler) makeNextConfig() Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Groups = make(map[int][]string)
	for gid, s := range oldConfig.Groups {
		newConfig.Groups[gid] = make([]string, len(s))
		copy(newConfig.Groups[gid], s)
	}
	for i, v := range oldConfig.Shards {
		newConfig.Shards[i] = v
	}
	return newConfig
}

func (sc *ShardCtrler) write(op Op) string {
	res := make(chan int)
	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(op)
	if isLeader {
		if ch, ok := sc.resultChan[index]; ok {
			ch <- 0
		}
		sc.resultChan[index] = res
	}
	sc.mu.Unlock()
	if isLeader {
		resultTerm := <-res
		sc.mu.Lock()
		delete(sc.resultChan, index)
		sc.mu.Unlock()
		if resultTerm != term {
			return COMMIT_OVERWRITTEN
		} else {
			return OK
		}
	}
	return WRONG_LEADER
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	for {
		res := sc.write(Op{Name: "Join", Join: args})
		if res == OK {
			return
		} else if res == WRONG_LEADER {
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	for {
		res := sc.write(Op{Name: "Leave", Leave: args})
		if res == OK {
			return
		} else if res == WRONG_LEADER {
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	for {
		res := sc.write(Op{Name: "Move", Move: args})
		if res == OK {
			return
		} else if res == WRONG_LEADER {
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) ReadMyWrite() bool {
	for {
		sc.mu.Lock()
		index, term, isLeader := sc.rf.Start(Op{Name: "HeartBeat"})
		if !isLeader {
			sc.mu.Unlock()
			return false
		}
		if ch, ok := sc.resultChan[index]; ok {
			ch <- 0
		}
		res := make(chan int)
		sc.resultChan[index] = res
		sc.mu.Unlock()
		resultTerm := <-res
		if resultTerm == term {
			return true
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if !sc.ReadMyWrite() {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if args.Num == -1 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else if args.Num >= len(sc.configs) || args.Num < -1 {
		panic("config index out of bound")
	} else {
		reply.Config = sc.configs[args.Num]
	}
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyJoin(args *JoinArgs) {
	config := sc.makeNextConfig()
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
	}
	rebalance(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyLeave(args *LeaveArgs) {
	config := sc.makeNextConfig()
	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
	}
	rebalance(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) applyMove(args *MoveArgs) {
	config := sc.makeNextConfig()
	config.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, config)
}
func (sc *ShardCtrler) applyOp(msg raft.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok {
		panic("failed to cast command to Op")
	}

	if msg.CommandIndex > sc.lastAppliedIndex+1 {
		panic(fmt.Sprintf("apply message out of order CommandIndex=%d, lastAppliedIndex=%d",
			msg.CommandIndex, sc.lastAppliedIndex))
	} else if msg.CommandIndex <= sc.lastAppliedIndex {
		// can happen if snapshot is installed betwee rf unlock and op being sent to apply channel
		return
	}
	sc.lastAppliedIndex++
	if msg.CommandTerm < sc.lastAppliedTerm {
		panic("term decreases")
	}
	sc.lastAppliedTerm = msg.CommandTerm
	switch {
	case op.Name == "Join":
		sc.applyJoin(op.Join)
	case op.Name == "Leave":
		sc.applyLeave(op.Leave)
	case op.Name == "Move":
		sc.applyMove(op.Move)
	}
	if sc.configs[len(sc.configs)-1].Num != len(sc.configs)-1 {
		panic(fmt.Sprintf("invlid configs: %v", sc.configs))
	}
	if ch, chFound := sc.resultChan[msg.CommandIndex]; chFound {
		ch <- msg.CommandTerm
	}
}

func (sc *ShardCtrler) runApplyLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			sc.mu.Lock()
			if msg.CommandValid {
				sc.applyOp(msg)
			} else {
				panic("snapshot not expected")
			}
			sc.mu.Unlock()
		case <-time.After(time.Millisecond * 100):
			term, isLeader := sc.rf.GetState()
			sc.mu.Lock()
			if isLeader && sc.lastAppliedTerm != term {
				sc.rf.Start(Op{Name: "HeartBeat"})
			}
			sc.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.resultChan = map[int]chan int{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastAppliedRequestId = make(map[int64]int64)
	go sc.runApplyLoop()

	// Your code here.

	return sc
}
