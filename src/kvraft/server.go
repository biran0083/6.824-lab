package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType    string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data                 map[string]string
	resultChan           map[int]chan int
	lastAppliedIndex     int
	lastAppliedTerm      int
	lastAppliedRequestId map[int64]int64
}

func (kv *KVServer) ReadMyWrite() bool {
	for {
		kv.mu.Lock()
		index, term, isLeader := kv.rf.Start(Op{"HeartBeat", "", "", 0, 0})
		if !isLeader {
			kv.mu.Unlock()
			return false
		}
		DPrintf("[%d] starting heart beat, index=%d", kv.me, index)
		if ch, ok := kv.resultChan[index]; ok {
			DPrintf("[%d] send invalid term to result chan waiting at index=%d", kv.me, index)
			ch <- 0
		}
		res := make(chan int)
		kv.resultChan[index] = res
		kv.mu.Unlock()
		resultTerm := <-res
		if resultTerm == term {
			DPrintf("[%d] heart beat term matches", kv.me)
			return true
		} else {
			DPrintf("[%d] heart beat term mismatches", kv.me)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.ReadMyWrite() {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("[%d] end read my write", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	if value, ok := kv.data[args.Key]; !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	res := make(chan int)
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.ClientId, args.RequestId})
	if isLeader {
		if ch, ok := kv.resultChan[index]; ok {
			ch <- 0
		}
		kv.resultChan[index] = res
	}
	kv.mu.Unlock()
	if isLeader {
		resultTerm := <-res
		kv.mu.Lock()
		delete(kv.resultChan, index)
		kv.mu.Unlock()
		if resultTerm != term {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyOp(msg raft.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok {
		panic("failed to cast command to Op")
	}

	if msg.CommandIndex > kv.lastAppliedIndex+1 {
		panic(fmt.Sprintf("apply message out of order CommandIndex=%d, lastAppliedIndex=%d", msg.CommandIndex, kv.lastAppliedIndex))
	} else if msg.CommandIndex <= kv.lastAppliedIndex {
		// can happen if snapshot is installed betwee rf unlock and op being sent to apply channel
		DPrintf("skip old op")
		return
	}
	kv.lastAppliedIndex++
	if msg.CommandTerm < kv.lastAppliedTerm {
		panic("term decreases")
	}
	kv.lastAppliedTerm = msg.CommandTerm
	switch {
	case op.OpType == "Put":
		reqId := kv.lastAppliedRequestId[op.ClientId]
		if reqId < op.RequestId {
			kv.data[op.Key] = op.Value
			kv.lastAppliedRequestId[op.ClientId] = op.RequestId
		} else if reqId == op.RequestId {
			DPrintf("duplicate request detected")
		} else {
			panic("ignore old request")
		}
	case op.OpType == "Append":
		reqId := kv.lastAppliedRequestId[op.ClientId]
		if reqId < op.RequestId {
			value, ok := kv.data[op.Key]
			if ok {
				kv.data[op.Key] = value + op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
			kv.lastAppliedRequestId[op.ClientId] = op.RequestId
		} else if reqId == op.RequestId {
			DPrintf("duplicate request detected")
		} else {
			panic("old request")
		}
	case op.OpType == "HeartBeat":
	default:
		panic("unknown op type")
	}
	if ch, chFound := kv.resultChan[msg.CommandIndex]; chFound {
		ch <- msg.CommandTerm
	}

}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	if !msg.SnapshotValid {
		panic("Snapshot invalid")
	}
	raw := msg.Snapshot
	index := msg.SnapshotIndex
	term := msg.SnapshotTerm
	if kv.rf.CondInstallSnapshot(term, index, raw) {
		kv.loadSnapshotData(raw)
	}
	DPrintf("[%d] snapshot applied", kv.me)
}

func (kv *KVServer) runApplyLoop() {
	for {
		if kv.killed() {
			break
		}
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			DPrintf("[%d] applying %v", kv.me, msg)
			if msg.CommandValid {
				kv.applyOp(msg)
			} else {
				kv.applySnapshot(msg)
			}
			kv.mu.Unlock()
		case <-time.After(time.Millisecond * 100):
			term, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && kv.lastAppliedTerm != term {
				kv.rf.Start(Op{"HeartBeat", "", "", 0, 0})
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) runSnapshotter(persister *raft.Persister) {
	for {
		if kv.killed() {
			break
		}
		select {
		case <-time.After(time.Millisecond * 100):
			stateSize := persister.RaftStateSize()
			if kv.maxraftstate != -1 && stateSize > kv.maxraftstate {
				//DPrintf("saving snapshot. state_size=%d", stateSize)
				kv.saveSnapshot()
			}
		}
	}
}

func (kv *KVServer) saveSnapshot() {
	kv.mu.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.lastAppliedTerm)
	e.Encode(kv.lastAppliedRequestId)
	DPrintf("[%d] saving snapshot data %v", kv.me, kv.data)
	DPrintf("[%d] saving snapshot last applied request id %v", kv.me, kv.lastAppliedRequestId)
	data := w.Bytes()
	kv.rf.Snapshot(kv.lastAppliedIndex, data)
	kv.mu.Unlock()
}

// called with kv.mu locked
func (kv *KVServer) loadSnapshotData(raw []byte) {
	r := bytes.NewBuffer(raw)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var lastAppliedIndex int
	var lastAppliedTerm int
	var lastAppliedRequestId map[int64]int64
	if d.Decode(&data) != nil ||
		d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&lastAppliedTerm) != nil ||
		d.Decode(&lastAppliedRequestId) != nil {
		log.Fatal("failed to decode state")
	} else {
		kv.data = data
		kv.lastAppliedIndex = lastAppliedIndex
		kv.lastAppliedTerm = lastAppliedTerm
		kv.lastAppliedRequestId = lastAppliedRequestId
		DPrintf("[%d] load snapshot data %v", kv.me, kv.data)
		DPrintf("[%d] lastAppliedRequestId %v", kv.me, kv.lastAppliedRequestId)
	}
}

func (kv *KVServer) loadSnapshot(persister *raft.Persister) {
	raw := persister.ReadSnapshot()
	if raw == nil || len(raw) < 1 { // bootstrap without any state?
		return
	}
	kv.mu.Lock()
	kv.loadSnapshotData(raw)
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.resultChan = make(map[int]chan int)
	kv.lastAppliedRequestId = map[int64]int64{}
	kv.loadSnapshot(persister)
	go kv.runApplyLoop()
	go kv.runSnapshotter(persister)

	return kv
}
