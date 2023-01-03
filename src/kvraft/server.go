package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
	"mit-6.824/raft"
)

const (
	Debug                  = 0
	ConsensusTimeout       = 500
	SnapshotCheckInterval  = 100
	SnapshotThresholdRatio = 0.8
)

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
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	waitApplyCh   map[int]chan Op
	lastRequestId map[int64]int
}

func (kv *KVServer) checkSnapshotInstall(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	ratio := float64(kv.rf.GetRaftStateSize()) / float64(kv.maxraftstate)
	if ratio > SnapshotThresholdRatio {
		snapshot := kv.makeSnapshot()
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.db); err != nil {
		log.Fatal("encode error:", err)
	}
	if err := e.Encode(kv.lastRequestId); err != nil {
		log.Fatal("encode error:", err)
	}
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastRequestId map[int64]int
	if err := d.Decode(&db); err != nil {
		log.Fatal("decode error:", err)
	}
	if err := d.Decode(&lastRequestId); err != nil {
		log.Fatal("decode error:", err)
	}
	kv.db = db
	kv.lastRequestId = lastRequestId
	// kv.printInfo("READING SNAPSHOT")
}

func (kv *KVServer) duplicateRequest(clientId int64, requestId int) bool {
	lastId, ok := kv.lastRequestId[clientId]
	if !ok {
		kv.lastRequestId[clientId] = -1
		return false
	}
	return requestId <= lastId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.dprintf("start (%v-%v) on index %d", args.ClientId, args.RequestId, index)
	raftIndexCh, ok := kv.waitApplyCh[index]
	if !ok {
		kv.waitApplyCh[index] = make(chan Op, 1)
		raftIndexCh = kv.waitApplyCh[index]
	}
	select {
	case commitOp := <-raftIndexCh:
		if commitOp.ClientId == op.ClientId && commitOp.RequestId == op.RequestId {
			kv.dprintf("index %d has been applied", index)
			if val, ok := kv.db[args.Key]; ok {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Millisecond * ConsensusTimeout):
		kv.dprintf("index %d times out", index)
		reply.Err = ErrWrongLeader
	}
	delete(kv.waitApplyCh, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	raftIndexCh, ok := kv.waitApplyCh[index]
	if !ok {
		kv.waitApplyCh[index] = make(chan Op, 1)
		raftIndexCh = kv.waitApplyCh[index]
	}
	kv.dprintf("start (%v-%v) on index %d", args.ClientId, args.RequestId, index)
	select {
	case commitOp := <-raftIndexCh:
		if commitOp.ClientId == op.ClientId && commitOp.RequestId == op.RequestId {
			kv.dprintf("index %d has been applied", index)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Millisecond * ConsensusTimeout):
		kv.dprintf("index %d times out", index)
		reply.Err = ErrWrongLeader
	}
	delete(kv.waitApplyCh, index)
}

func (kv *KVServer) dprintf(format string, a ...interface{}) {
	args := []interface{}{}
	args = append(args, kv.rf.GetId())
	args = append(args, a...)
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf("server(%d) "+format, args...)
	}
}

func (kv *KVServer) printInfo(msg string) {
	log.Println(msg)
	log.Printf("server(%d)", kv.rf.GetId())
	for k, v := range kv.lastRequestId {
		log.Printf("lastId[%v] = %v", k, v)
	}
	log.Printf("LastLogIndex = %d", kv.rf.LastIncludedIndex)
	for _, e := range kv.rf.Logs {
		if e.Command == nil {
			continue
		}
		cmd := e.Command.(Op)
		log.Printf("entry %d: %v-%v", e.LogIndex, cmd.ClientId, cmd.RequestId)
	}
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			b := bytes.NewBuffer(m.SnapshotData)
			d := labgob.NewDecoder(b)
			if err := d.Decode(&kv.db); err != nil {
				log.Fatalln("decode error:", err)
			}
			if err := d.Decode(&kv.lastRequestId); err != nil {
				log.Fatalln("decode error:", err)
			}
			// kv.printInfo("RECEIVE SNAPSHOT")
		}
		if m.CommandValid {
			op := m.Command.(Op)
			if !kv.duplicateRequest(op.ClientId, op.RequestId) {
				if op.RequestId != kv.lastRequestId[op.ClientId]+1 {
					kv.printInfo("ERROR")
					log.Fatalf("server(%d) execute client(%v) requests not in serial order,\nexpected: %v, actual %v", kv.rf.GetId(), op.ClientId, kv.lastRequestId[op.ClientId]+1, op.RequestId)
				}
				kv.lastRequestId[op.ClientId] = op.RequestId
				// kv.dprintf("kv.lastRequestId[%v] = %v", op.ClientId, op.RequestId)
				if op.Operation == "Get" {
					kv.dprintf("GET(%v-%v): %v", op.ClientId, op.RequestId, op.Key)
					// do nothing
				} else if op.Operation == "Put" {
					kv.dprintf("PUT(%v-%v): %v=%v", op.ClientId, op.RequestId, op.Key, op.Value)
					kv.db[op.Key] = op.Value
				} else {
					kv.dprintf("APP(%v-%v): APP: %v=%v", op.ClientId, op.RequestId, op.Key, op.Value)
					if origin, ok := kv.db[op.Key]; ok {
						kv.db[op.Key] = origin + op.Value
					} else {
						kv.db[op.Key] = op.Value
					}
					kv.dprintf("VAL: %v=%v", op.Key, kv.db[op.Key])
				}
				kv.checkSnapshotInstall(m.CommandIndex)
			}
			// notify the RPC to return OK
			if ch, ok := kv.waitApplyCh[m.CommandIndex]; ok {
				kv.dprintf("(%v-%v) index %d done", op.ClientId, op.RequestId, m.CommandIndex)
				// log.Printf("server(%d) (%v-%v) index %d done", kv.rf.GetId(), op.ClientId, op.RequestId, m.CommandIndex)
				ch <- op
			}
		}
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
	kv.db = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
