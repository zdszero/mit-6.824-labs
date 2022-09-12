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
	leader int
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
	ck.leader = -1
	// You'll have to add code here.
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
		Key: key,
	}
	reply := GetReply{}
	var ret string
findLeader:
	if ck.leader == -1 {
		for i, s := range ck.servers {
			ok := s.Call("KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				continue
			}
			ck.leader = i
			if reply.Err == ErrNoKey {
				ret = ""
			} else {
				ret = reply.Value
			}
			break
		}
	} else {
		s := ck.servers[ck.leader]
		ok := s.Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = -1
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
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := PutAppendReply{}
findLeader:
	if ck.leader == -1 {
		for {
			for i, s := range ck.servers {
				ok := s.Call("KVServer.PutAppend", &args, &reply)
				if !ok {
					continue
				}
				if reply.Err == ErrWrongLeader {
					continue
				}
				ck.leader = i
				break
			}
			if ck.leader == -1 {
				time.Sleep(time.Millisecond * 50)
			} else {
				break
			}
		}
	} else {
		s := ck.servers[ck.leader]
		ok := s.Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = -1
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
