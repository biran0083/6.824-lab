package shardctrler

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	resultChan map[int]chan int

	configs []Config // indexed by config num
}

type Op struct {
	Name      string
	NewConfig Config
}

func rebalance(config *Config) {}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	config := sc.configs[len(sc.configs)-1]
	config.Num++
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
	}
	rebalance(&config)
	_, _, isLeader := sc.rf.Start(Op{Name: "Join", NewConfig: config})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
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
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, _, isLeader := sc.rf.Start(Op{Name: "HeartBeat"})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.Num == -1 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else if args.Num >= len(sc.configs) || args.Num < -1 {
		reply.Err = "config index out of bound"
	} else {
		reply.Config = sc.configs[args.Num]
	}
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

	// Your code here.

	return sc
}
