package kvraft

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
	"mit-6.824/raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	done    chan bool
	storage map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cmd := "Get:" + args.Key
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	<-kv.done
	if val, ok := kv.storage[args.Key]; ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var cmd string
	cmd = fmt.Sprintf("%v:%v,%v", args.Op, args.Key, args.Value)
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	<-kv.done
	reply.Err = OK
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if !m.CommandValid {
			continue
		}
		cmd := fmt.Sprint(m.Command)
		if strings.HasPrefix(cmd, "Get") {
			// do nothing
			DPrintf("GET %v", cmd[4:])
		} else {
			index1 := strings.Index(cmd, ":")
			index2 := strings.Index(cmd, ",")
			key := cmd[index1+1 : index2]
			value := cmd[index2+1:]
			if strings.HasPrefix(cmd, "Put") {
				// "Put:Key,Value"
				DPrintf("PUT %v:%v", key, value)
				kv.storage[key] = value
			} else {
				// "Append:Key,Value"
				DPrintf("APP %v:%v", key, value)
				if origin, ok := kv.storage[key]; ok {
					kv.storage[key] = origin + value
				} else {
					kv.storage[key] = value
				}
			}
		}
		// notify the RPC to return OK
		kv.done <- true
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.done = make(chan bool)
	kv.storage = make(map[string]string)

	go kv.applier()

	return kv
}
