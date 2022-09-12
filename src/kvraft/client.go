package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"mit-6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	recentLeader int
	requestId    int
	clientId     int64
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
	// You'll have to add code here.
	ck.recentLeader = 0
	ck.requestId = 0
	ck.clientId = nrand()
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

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	reply := GetReply{}
	ck.requestId++
	var ret string
findLeader:
	if ck.recentLeader == -1 {
		for i, s := range ck.servers {
			ok := s.Call("KVServer.Get", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				continue
			}
			ck.recentLeader = i
			if reply.Err == ErrNoKey {
				ret = ""
			} else {
				ret = reply.Value
			}
			break
		}
		if ck.recentLeader == -1 {
			time.Sleep(time.Millisecond * 50)
			goto findLeader
		}
	} else {
		s := ck.servers[ck.recentLeader]
		ok := s.Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.recentLeader = -1
			goto findLeader
		}
		if reply.Err == ErrNoKey {
			ret = ""
		} else {
			ret = reply.Value
		}
	}
	return ret
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
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	reply := PutAppendReply{}
findLeader:
	if ck.recentLeader == -1 {
		for i, s := range ck.servers {
			ok := s.Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				continue
			}
			ck.recentLeader = i
			break
		}
		if ck.recentLeader == -1 {
			time.Sleep(time.Millisecond * 50)
			goto findLeader
		}
	} else {
		s := ck.servers[ck.recentLeader]
		ok := s.Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.recentLeader = -1
			goto findLeader
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
