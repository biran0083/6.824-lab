package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	leaderId      int
	nextRequestId int64
	clientId      int64
	m             sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.nextRequestId = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	req := GetArgs{key}
	i := ck.leaderId
	for {
		resp := GetReply{}
		// DPrintf("sending Get to %d", i)
		if ck.servers[i].Call("KVServer.Get", &req, &resp) {
			if resp.Err == OK {
				ck.m.Lock()
				ck.leaderId = i
				ck.m.Unlock()
				return resp.Value
			} else if resp.Err == ErrWrongLeader {
				// DPrintf("Get hit wrong leader %d", i)
				i = (i + 1) % len(ck.servers)
			} else {
				panic("unexpeceted error code from Get")
			}
		} else {
			i = (i + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.m.Lock()
	i := ck.leaderId
	req := PutAppendArgs{key, value, op, ck.clientId, ck.nextRequestId}
	ck.nextRequestId++
	ck.m.Unlock()
	for {
		resp := PutAppendReply{}
		s := ck.servers[i]
		// DPrintf("sending PutAppend %v to %d", req, i)
		if s.Call("KVServer.PutAppend", &req, &resp) {
			// DPrintf("PutAppend response %v %v", req, resp)
			if resp.Err == OK {
				ck.m.Lock()
				ck.leaderId = i
				ck.m.Unlock()
				break
			} else if resp.Err == ErrWrongLeader {
				i = (i + 1) % len(ck.servers)
			}
		} else {
			i = (i + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
