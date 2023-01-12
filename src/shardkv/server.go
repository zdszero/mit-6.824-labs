package shardkv

// import "mit-6.824/shardmaster"
import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
	"mit-6.824/raft"
	"mit-6.824/shardmaster"
)

//
// system setting
//
const (
	SnapshotThresholdRatio = 0.9
	ConsensusTimeout       = 500
	WaitPullTimeout        = 500
	PollConfigInterval     = 100
	CoordinateInterval     = 100
)

//
// raft log entry defination
//
type CommandType int

const (
	ClientRequest CommandType = iota
	ReconfigStart
	InsertShard
	RemoveShard
	ReconfigEnd
	EmptyEntry
)

func typeString(cmdtype CommandType) string {
	switch cmdtype {
	case ClientRequest:
		return "ClientRequest"
	case ReconfigStart:
		return "ReconfigStart"
	case InsertShard:
		return "insertShard"
	case RemoveShard:
		return "RemoveShard"
	case ReconfigEnd:
		return "ReconfigEnd"
	case EmptyEntry:
		return "EmptyEntry"
	}
	return "Unknown"
}

type RaftLogCommand struct {
	CommandType
	Data interface{}
}

func isSameSlice(l []int, r []int) bool {
	if len(l) != len(r) {
		return false
	}
	length := len(l)
	for i := 0; i < length; i++ {
		if l[i] != r[i] {
			return false
		}
	}
	return true
}

func isSameCommand(l RaftLogCommand, r RaftLogCommand) bool {
	if l.CommandType != r.CommandType {
		return false
	}
	cmdtype := l.CommandType
	switch cmdtype {
	case ClientRequest:
		largs := l.Data.(ClientOp)
		rargs := r.Data.(ClientOp)
		return largs.Args.GetClientId() == rargs.Args.GetClientId() &&
			largs.Args.GetOpId() == rargs.Args.GetOpId()
	case ReconfigStart:
		largs := l.Data.(shardmaster.Config)
		rargs := r.Data.(shardmaster.Config)
		return largs.Num == rargs.Num
	case ReconfigEnd:
		largs := l.Data.(int)
		rargs := r.Data.(int)
		return largs == rargs
	case RemoveShard:
		largs := l.Data.(RemoveShardsArgs)
		rargs := r.Data.(RemoveShardsArgs)
		return largs.CfgNum == rargs.CfgNum &&
			isSameSlice(largs.ShardNums, rargs.ShardNums)
	case InsertShard:
		largs := l.Data.(PullShardsReply)
		rargs := r.Data.(PullShardsReply)
		return largs.CfgNum == rargs.CfgNum
	case EmptyEntry:
		return true
	}
	return false
}

func newRaftLogCommand(cmdtype CommandType, data interface{}) RaftLogCommand {
	return RaftLogCommand{
		CommandType: cmdtype,
		Data:        data,
	}
}

//
// shard data and status defination
//
type ShardStatus int

const (
	Serving ShardStatus = iota
	Pulling
	Pulled
	Erasing
	Erased
)

type Shard struct {
	Status ShardStatus
	KV     map[string]string
}

func copyShard(s Shard) Shard {
	kv := make(map[string]string)
	for k, v := range s.KV {
		kv[k] = v
	}
	return Shard{
		Status: s.Status,
		KV:     kv,
	}
}

func newShard(status ShardStatus) Shard {
	return Shard{
		Status: status,
		KV:     make(map[string]string),
	}
}

//
// client operation defination
//
type OpMethod int

const (
	GetOp OpMethod = iota
	PutOp
	AppendOp
)

type ClientOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method OpMethod
	Args   ClerkArgs
}

type requestResult struct {
	err   Err
	value interface{} // for get Operation
}

type commandEntry struct {
	cmd     RaftLogCommand
	replyCh chan requestResult
}

type RemoveShardOp struct {
	ShardNums []int
	CfgNum    int
}

//
// shard kv defination
//

type ClientTbl map[int64]int

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	mck          *shardmaster.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead              int64
	RemovedShardNums  []int
	InsertShardNums   []int
	RemoveGroup       map[int][]int
	InsertGroup       map[int][]int
	PrevCfg           shardmaster.Config
	CurrCfg           shardmaster.Config
	Shards            map[int]Shard        // shard -> DB: (key -> value)
	ShardRefTbl       map[int]ClientTbl    // client id -> last op id
	commandTbl        map[int]commandEntry // index -> wait channel
	waitPullTbl       map[int]chan bool
	configPollTrigger chan bool
}

func (kv *ShardKV) dprintf(format string, a ...interface{}) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	args := []interface{}{}
	args = append(args, kv.gid)
	args = append(args, kv.rf.GetId())
	args = append(args, a...)
	DPrintf("server(%d-%d) "+format+"\n", args...)
}

func (kv *ShardKV) checkSnapshotInstall(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	ratio := float64(kv.rf.GetRaftStateSize()) / float64(kv.maxraftstate)
	if ratio > SnapshotThresholdRatio {
		kv.takeSnapshot(index)
	}
}

func (kv *ShardKV) takeSnapshot(index int) {
	snapshot := kv.snapshotData()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) snapshotData() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	var e error
	if e = enc.Encode(kv.RemovedShardNums); e != nil {
		log.Fatalln("Failed to encode currCfg")
	}
	if e = enc.Encode(kv.InsertShardNums); e != nil {
		log.Fatalln("Failed to encode prevCfg")
	}
	if e = enc.Encode(kv.RemoveGroup); e != nil {
		log.Fatalln("Failed to encode shards")
	}
	if e = enc.Encode(kv.InsertGroup); e != nil {
		log.Fatalln("Failed to encode clientTbl")
	}
	if e = enc.Encode(kv.CurrCfg); e != nil {
		log.Fatalln("Failed to encode currCfg")
	}
	if e = enc.Encode(kv.PrevCfg); e != nil {
		log.Fatalln("Failed to encode prevCfg")
	}
	if e = enc.Encode(kv.Shards); e != nil {
		log.Fatalln("Failed to encode shards")
	}
	if e = enc.Encode(kv.ShardRefTbl); e != nil {
		log.Fatalln("Failed to encode clientTbl")
	}
	// kv.dprintf("encode data: %v", w.Bytes())
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil {
		return
	}
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	var currCfg shardmaster.Config
	var prevCfg shardmaster.Config
	var shards map[int]Shard
	var shardRefTbl map[int]ClientTbl
	var e error
	var removedShardNums []int
	var insertShardNums []int
	var removeGroup map[int][]int
	var insertGroup map[int][]int
	// kv.dprintf("decode data: %v", data)
	if e = dec.Decode(&removedShardNums); e != nil {
		log.Fatalln("Failed to decode currCfg")
	}
	if e = dec.Decode(&insertShardNums); e != nil {
		log.Fatalln("Failed to decode prevCfg")
	}
	if e = dec.Decode(&removeGroup); e != nil {
		log.Fatalln("Failed to decode shards")
	}
	if e = dec.Decode(&insertGroup); e != nil {
		log.Fatalln("Failed to decode clientTbl")
	}
	if e = dec.Decode(&currCfg); e != nil {
		log.Fatalln("Failed to decode currCfg")
	}
	if e = dec.Decode(&prevCfg); e != nil {
		log.Fatalln("Failed to decode prevCfg")
	}
	if e = dec.Decode(&shards); e != nil {
		log.Fatalln("Failed to decode shards")
	}
	if e = dec.Decode(&shardRefTbl); e != nil {
		log.Fatalln("Failed to decode clientTbl")
	}
	kv.RemovedShardNums = removedShardNums
	kv.InsertShardNums = insertShardNums
	kv.RemoveGroup = removeGroup
	kv.InsertGroup = insertGroup
	kv.CurrCfg = currCfg
	kv.PrevCfg = prevCfg
	kv.Shards = shards
	kv.ShardRefTbl = shardRefTbl
}

///////////////////////////////
//           RPCs            //
///////////////////////////////

func (kv *ShardKV) checkShardAvailable(si int) Err {
	if kv.CurrCfg.Shards[si] != kv.gid {
		return ErrWrongGroup
	}
	shard, ok := kv.Shards[si]
	if !ok {
		return ErrInMigration
	}
	if shard.Status != Serving {
		return ErrInMigration
	}
	return OK
}

// 1. check not killed
// 2. check is leader
// 3. check config number, trigger config poll if necessary
// 4. start raft consensus, wait for reply from applier
func (kv *ShardKV) commonHandler(cmd RaftLogCommand) (e Err, r interface{}) {
	if kv.killed() {
		e = ErrShutdown
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		e = ErrWrongLeader
		return
	}
	// lock bofore rf.Start()
	// to avoid raft finish to quickly and apply before kv.commandTbl has set applyCh
	kv.mu.Lock()
	switch cmd.CommandType {
	case ClientRequest:
		cliOp := cmd.Data.(ClientOp)
		if cliOp.Args.GetCfgNum() > kv.CurrCfg.Num {
			// kv.dprintf("not ready, cli %d my %d", cliOp.Args.GetCfgNum(), curCfgNum)
			kv.mu.Unlock()
			kv.triggerConfigPoll()
			e = ErrNotReady
			return
		}
		if cliOp.Args.GetCfgNum() < kv.CurrCfg.Num {
			// kv.dprintf("outdated config")
			kv.mu.Unlock()
			e = ErrOutdatedConfig
			return
		}
		if kv.CurrCfg.Num == 0 {
			kv.mu.Unlock()
			e = ErrWrongGroup
			return
		}
		if err := kv.checkShardAvailable(cliOp.Args.GetShard()); err != OK {
			// kv.dprintf("not available")
			kv.mu.Unlock()
			e = err
			return
		}
	}

	index, term, isLeader := kv.rf.Start(cmd)
	if term == 0 {
		kv.mu.Unlock()
		e = ErrInitElection
		return
	}
	if !isLeader {
		kv.mu.Unlock()
		e = ErrWrongLeader
		return
	}
	c := make(chan requestResult)
	kv.commandTbl[index] = commandEntry{cmd: cmd, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				e = ErrShutdown
				return
			}
			e, r = result.err, result.value
			return
		case <-time.After(ConsensusTimeout * time.Millisecond):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("start %d but not leader", index)
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
			kv.dprintf("%d:%s consensus timeout, wait again, last applied: %d", index, typeString(cmd.CommandType), kv.rf.LastApplied())
		}
	}

	go func() { <-c }()
	if kv.killed() {
		e = ErrShutdown
	}
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	e, r := kv.commonHandler(newRaftLogCommand(ClientRequest, ClientOp{Method: GetOp, Args: *args}))
	reply.Err = e
	if e == OK {
		reply.Value = r.(string)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.commonHandler(newRaftLogCommand(ClientRequest, ClientOp{Method: args.Method, Args: *args}))
}

//
// apply functions
//
func (kv *ShardKV) isOpDuplicate(op ClientOp) bool {
	if op.Method == GetOp {
		return false
	}
	cliTbl, ok := kv.ShardRefTbl[op.Args.GetShard()]
	if !ok {
		return false
	}
	lastOpId, ok := cliTbl[op.Args.GetClientId()]
	if !ok {
		return false
	}
	if op.Args.GetOpId() <= lastOpId {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) setShardRefTbl(op ClientOp) {
	si := op.Args.GetShard()
	cliTbl, ok := kv.ShardRefTbl[si]
	if !ok {
		kv.ShardRefTbl[si] = make(ClientTbl)
		cliTbl = kv.ShardRefTbl[si]
	}
	cliTbl[op.Args.GetClientId()] = op.Args.GetOpId()
}

func (kv *ShardKV) applier() {
	defer func() {
		kv.mu.Lock()
		// close all pending RPC handler reply channel to avoid goroutine resource leak
		for _, ce := range kv.commandTbl {
			close(ce.replyCh)
		}
		kv.mu.Unlock()
	}()
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(m.SnapshotData)
			for _, ce := range kv.commandTbl {
				ce.replyCh <- requestResult{err: ErrWrongLeader}
			}
			kv.commandTbl = make(map[int]commandEntry)
			kv.mu.Unlock()
			continue
		}
		if !m.CommandValid {
			continue
		}
		// kv.dprintf("apply %d", m.CommandIndex)
		cmd := m.Command.(RaftLogCommand)
		var reply requestResult
		reply.err = OK
		switch cmd.CommandType {
		case ClientRequest:
			op := cmd.Data.(ClientOp)
			reply = kv.applyClientRequest(op)
		case ReconfigStart:
			cfg := cmd.Data.(shardmaster.Config)
			kv.applyConfigStart(cfg)
		case ReconfigEnd:
			cfgNum := cmd.Data.(int)
			kv.applyConfigEnd(cfgNum)
		case RemoveShard:
			args := cmd.Data.(RemoveShardsArgs)
			kv.applyRemoveShards(args)
		case InsertShard:
			reply := cmd.Data.(PullShardsReply)
			kv.applyInsertShards(reply)
		case EmptyEntry:
			kv.dprintf("* empty entry")
			break
		}
		// kv.dprintf("apply %d end", m.CommandIndex)
		kv.mu.Lock()
		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			delete(kv.commandTbl, m.CommandIndex)
		}
		kv.mu.Unlock()
		if ok {
			if !isSameCommand(ce.cmd, cmd) {
				kv.dprintf("%d not same command", m.CommandIndex)
				ce.replyCh <- requestResult{err: ErrWrongLeader}
			} else {
				ce.replyCh <- reply
			}
			// ce.replyCh <- reply
		}
		kv.checkSnapshotInstall(m.CommandIndex)
	}
}

func (kv *ShardKV) applyClientRequest(op ClientOp) (reply requestResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if e := kv.checkShardAvailable(op.Args.GetShard()); e != OK {
		reply.err = e
		return
	}
	if kv.isOpDuplicate(op) {
		reply.err = OK
		return
	}
	si := op.Args.GetShard()
	kv.setShardRefTbl(op)
	switch {
	case op.Method == GetOp:
		args := op.Args.(GetArgs)
		db, ok := kv.Shards[si]
		if !ok {
			reply.err = ErrNoKey
			return
		}
		value, ok := db.KV[args.Key]
		if !ok {
			reply.err = ErrNoKey
			return
		}
		reply.value = value
		reply.err = OK
		// kv.dprintf("get %v:%v from shard %v", args.Key, value, si)
	case op.Method == PutOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			log.Fatalf("shard %d not exist when put", si)
		}
		db.KV[args.Key] = args.Value
		// kv.dprintf("put %v:%v on shard %v", args.Key, args.Value, si)
	case op.Method == AppendOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			log.Fatalf("shard %d not exist when append", si)
		}
		db.KV[args.Key] += args.Value
		kv.dprintf("append %v (cli %d, op %d)", args.Value, op.Args.GetClientId(), op.Args.GetOpId())
		// kv.dprintf("append %v:%v on shard %v", args.Key, args.Value, si)
	}
	reply.err = OK
	return
}

func (kv *ShardKV) applyConfigStart(cfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.dprintf("-> config %d start", cfg.Num)
	if cfg.Num <= kv.CurrCfg.Num {
		return
	}
	if cfg.Num != kv.CurrCfg.Num+1 {
		log.Fatalf("Receive config number %d, curr is %d", cfg.Num, kv.CurrCfg.Num)
	}
	// save previous config
	kv.PrevCfg = kv.CurrCfg
	kv.CurrCfg = cfg
	// config num == 0 means no config
	oldshards := kv.PrevCfg.CollectShards(kv.gid)
	newshards := kv.CurrCfg.CollectShards(kv.gid)
	// remove group to who
	removed, removeGroup := getDiffShards(newshards, oldshards, kv.CurrCfg.Shards[:])
	// to pull the new group from who
	inserted, insertGroup := getDiffShards(oldshards, newshards, kv.PrevCfg.Shards[:])
	// start pulling and sending at the same time
	if len(removeGroup) > 0 && len(insertGroup) > 0 {
		panic("Error: both transfer and accept shards in one config change")
	}
	if len(insertGroup) > 0 {
		kv.dprintf("insert group: %v", insertGroup)
		for _, si := range inserted {
			if _, ok := kv.Shards[si]; ok {
				log.Fatalf("server(%d-%d) pulling shard %d when shard is not empty", kv.gid, kv.rf.GetId(), si)
			} else {
				kv.Shards[si] = newShard(Pulling)
			}
		}
	}
	if len(removeGroup) > 0 {
		kv.dprintf("remove group: %v", removeGroup)
		for _, si := range removed {
			if shard, ok := kv.Shards[si]; ok {
				shard.Status = Erasing
				kv.Shards[si] = shard
			} else {
				log.Fatalf("server(%d-%d) removing shards when shard is empty", kv.gid, kv.rf.GetId())
			}
		}
		kv.waitPullTbl[kv.CurrCfg.Num] = make(chan bool, 1)
	}
	kv.RemovedShardNums = removed
	kv.InsertShardNums = inserted
	kv.InsertGroup = insertGroup
	kv.RemoveGroup = removeGroup
}

func (kv *ShardKV) applyConfigEnd(cfgNum int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.dprintf("* apply config end")
	kv.PrevCfg = kv.CurrCfg
	if kv.CurrCfg.Num != cfgNum {
		log.Fatalf("Config end with %d while curr is %d", cfgNum, kv.CurrCfg.Num)
	}
	if len(kv.InsertGroup) > 0 {
		for _, si := range kv.InsertShardNums {
			if shard, ok := kv.Shards[si]; ok {
				if shard.Status != Pulled {
					log.Fatalf("server(%d-%d) shard %d status is not pulled when apply config end", kv.gid, kv.rf.GetId(), si)
				}
				shard.Status = Serving
				kv.Shards[si] = shard
			}
		}
	}
	if len(kv.RemoveGroup) > 0 {
		for _, si := range kv.RemovedShardNums {
			if shard, ok := kv.Shards[si]; !ok || shard.Status != Erased {
				log.Fatalf("group(%d-%d) shard %d status is not erased when apply config end", kv.gid, kv.rf.GetId(), si)
			}
			delete(kv.Shards, si)
			delete(kv.ShardRefTbl, si)
		}
		delete(kv.waitPullTbl, kv.CurrCfg.Num)
	}
	kv.dprintf("-> config %d end", cfgNum)
}

func (kv *ShardKV) isAllRemoved() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	allRemoved := true
	for _, si := range kv.RemovedShardNums {
		if shard, ok := kv.Shards[si]; !ok || shard.Status != Erased {
			allRemoved = false
			break
		}
	}
	return allRemoved
}

func (kv *ShardKV) applyRemoveShards(args RemoveShardsArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, si := range args.ShardNums {
		if shard, ok := kv.Shards[si]; ok {
			shard.Status = Erased
			kv.Shards[si] = shard
		}
	}
	select {
	case kv.waitPullTbl[kv.CurrCfg.Num] <- true:
	default:
	}
	kv.dprintf("remove shards %v", args.ShardNums)
}

func (kv *ShardKV) applyInsertShards(reply PullShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sis := []int{}
	for si, shard := range reply.Shards {
		shard.Status = Pulled
		kv.Shards[si] = shard
		sis = append(sis, si)
	}
	for si, refTbl := range reply.ShardRefTbl {
		kv.ShardRefTbl[si] = refTbl
	}
	kv.dprintf("* insert shards %v", sis)
}

type PullShardsArgs struct {
	CfgNum    int
	ShardNums []int
}

type PullShardsReply struct {
	Err         Err
	CfgNum      int
	Shards      map[int]Shard
	ShardRefTbl map[int]ClientTbl
}

func copyRefTbl(src ClientTbl) ClientTbl {
	cpy := make(ClientTbl)
	for k, v := range src {
		cpy[k] = v
	}
	return cpy
}

func (kv *ShardKV) PullShards(args *PullShardsArgs, reply *PullShardsReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	currCfgNum := kv.CurrCfg.Num
	kv.mu.Unlock()
	if args.CfgNum < currCfgNum {
		kv.dprintf("duplicate pull")
		reply.Err = ErrDuplicate
		return
	}
	if args.CfgNum > currCfgNum {
		kv.dprintf("not ready")
		reply.Err = ErrNotReady
		return
	}
	// 0 still remove shards
	// 1, 2  remove shards
	if kv.checkStatus(args.ShardNums, Erased) {
		kv.dprintf("%v already removed when pulled", args.ShardNums)
		reply.Err = ErrDuplicate
	}
	if !kv.checkStatus(args.ShardNums, Erasing) {
		kv.dprintf("%v status is not erasing when pulled", args.ShardNums)
		reply.Err = ErrNotReady
		return
	}
	replyShards := make(map[int]Shard)
	shardRefTbl := make(map[int]ClientTbl)
	kv.mu.Lock()
	for _, si := range args.ShardNums {
		replyShards[si] = copyShard(kv.Shards[si])
		shardRefTbl[si] = copyRefTbl(kv.ShardRefTbl[si])
	}
	kv.mu.Unlock()
	reply.Err = OK
	reply.CfgNum = currCfgNum
	reply.Shards = replyShards
	reply.ShardRefTbl = shardRefTbl
}

func (kv *ShardKV) checkStatus(sis []int, status ShardStatus) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var s ShardStatus
	if shard, ok := kv.Shards[sis[0]]; ok {
		s = shard.Status
	}
	for i := 1; i < len(sis); i++ {
		if shard, ok := kv.Shards[sis[i]]; ok {
			if shard.Status != s {
				log.Fatalf("shard 0 and %d status is not equal", i)
			}
		}
	}
	if s == status {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) pullShards(groupShards map[int][]int) bool {
	kv.dprintf("pull shards %v ...", kv.InsertGroup)
	// gid -> pulling shard numbers
	wg := sync.WaitGroup{}
	success := true
	for g, sis := range groupShards {
		if kv.checkStatus(sis, Pulled) {
			kv.dprintf("%v already pulled", sis)
			continue
		}
		wg.Add(1)
		kv.mu.Lock()
		servers := kv.PrevCfg.Groups[g]
		cfgNum := kv.CurrCfg.Num
		kv.mu.Unlock()
		go func(servers []string, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := PullShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := PullShardsReply{}
			for {
				for _, sn := range servers {
					end := kv.make_end(sn)
				Recall:
					// kv.dprintf("call(pull)")
					ok := end.Call("ShardKV.PullShards", &args, &reply)
					if !ok {
						continue
					}
					if reply.Err == OK {
						e, _ := kv.commonHandler(newRaftLogCommand(InsertShard, reply))
						if e != OK {
							success = false
						}
						return
					}
					if reply.Err == ErrDuplicate {
						return
					}
					if reply.Err == ErrNotReady {
						time.Sleep(time.Millisecond * 100)
						goto Recall
					}
					// ErrWrongLeader
				}
				time.Sleep(time.Millisecond * 100)
			}
		}(servers, sis, cfgNum)
	}
	wg.Wait()
	return success
}

type RemoveShardsArgs struct {
	CfgNum    int
	ShardNums []int
}

type RemoveShardsReply struct {
	Err Err
}

func (kv *ShardKV) RemoveShards(args *RemoveShardsArgs, reply *RemoveShardsReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	currCfgNum := kv.CurrCfg.Num
	kv.mu.Unlock()
	if args.CfgNum < currCfgNum {
		kv.dprintf("duplicate(remove)")
		reply.Err = ErrDuplicate
		return
	}
	if args.CfgNum > currCfgNum {
		kv.dprintf("duplicate(not ready)")
		reply.Err = ErrNotReady
		return
	}
	if kv.checkStatus(args.ShardNums, Erased) {
		kv.dprintf("%v already removed", args.ShardNums)
		reply.Err = ErrDuplicate
		return
	}
	if !kv.checkStatus(args.ShardNums, Erasing) {
		kv.dprintf("shards %v status is not erasing", args.ShardNums)
		reply.Err = ErrNotReady
		return
	}
	kv.dprintf("start to remove %v", args.ShardNums)
	e, _ := kv.commonHandler(newRaftLogCommand(RemoveShard, *args))
	if e != OK {
		kv.dprintf("remove %v failed: %v", args.ShardNums, reply.Err)
		reply.Err = e
		return
	}
	kv.dprintf("remove %v success", args.ShardNums)
	reply.Err = OK
}

func (kv *ShardKV) removeShards(groupShards map[int][]int) {
	kv.dprintf("remove shards ...")
	wg := sync.WaitGroup{}
	for g, sis := range groupShards {
		wg.Add(1)
		kv.mu.Lock()
		servers := kv.PrevCfg.Groups[g]
		cfgNum := kv.CurrCfg.Num
		kv.mu.Unlock()
		go func(servers []string, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := RemoveShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := RemoveShardsReply{}
			for {
				for _, sn := range servers {
					end := kv.make_end(sn)
				Recall:
					kv.dprintf("call(remove)")
					ok := end.Call("ShardKV.RemoveShards", &args, &reply)
					if !ok {
						continue
					}
					if reply.Err == OK || reply.Err == ErrDuplicate {
						return
					}
					if reply.Err == ErrNotReady {
						kv.dprintf("peer not ready")
						time.Sleep(time.Millisecond * 100)
						goto Recall
					}
					// ErrWrongLeader
				}
				time.Sleep(time.Millisecond * 100)
			}
		}(servers, sis, cfgNum)
	}
	wg.Wait()
}

// find the shards that in ss2 but not in ss1
func getDiffShards(ss1 []int, ss2 []int, s2g []int) (sis []int, group map[int][]int) {
	set := make(map[int]bool)
	group = make(map[int][]int)
	for _, si := range ss1 {
		set[si] = true
	}
	for _, si := range ss2 {
		g := s2g[si]
		if ok := set[si]; !ok {
			group[g] = append(group[g], si)
			sis = append(sis, si)
		}
	}
	return
}

func isNullGroup(grp map[int][]int) bool {
	if len(grp) != 1 {
		return false
	}
	ret := false
	for k := range grp {
		if k == 0 {
			ret = true
			break
		}
	}
	return ret
}

// apply new config
func (kv *ShardKV) installConfig() bool {
	kv.mu.Lock()
	removeGrp := kv.RemoveGroup
	insertGrp := kv.InsertGroup
	kv.mu.Unlock()
	if len(removeGrp) == 0 && len(insertGrp) == 0 {
		e, _ := kv.commonHandler(newRaftLogCommand(ReconfigEnd, kv.CurrCfg.Num))
		if e != OK {
			return false
		}
	}
	if len(removeGrp) > 0 {
		if isNullGroup(removeGrp) {
			// no machine will pull the shards
			args := RemoveShardsArgs{
				CfgNum:    kv.CurrCfg.Num,
				ShardNums: removeGrp[0],
			}
			e, _ := kv.commonHandler(newRaftLogCommand(RemoveShard, args))
			if e != OK {
				return false
			}
		} else {
			for !kv.isAllRemoved() {
				// kv.dprintf("wait for %v on %d ...", removeGrp, kv.CurrCfg.Num)
				// <-kv.waitPullTbl[kv.CurrCfg.Num]
				// kv.dprintf("wait finished")
				time.Sleep(time.Millisecond * 50)
			}
		}
		e, _ := kv.commonHandler(newRaftLogCommand(ReconfigEnd, kv.CurrCfg.Num))
		if e != OK {
			return false
		}
	}
	if len(insertGrp) > 0 {
		if isNullGroup(insertGrp) {
			shards := make(map[int]Shard)
			for _, si := range insertGrp[0] {
				shards[si] = newShard(Pulled)
			}
			reply := PullShardsReply{
				Err:    OK,
				Shards: shards,
			}
			e, _ := kv.commonHandler(newRaftLogCommand(InsertShard, reply))
			if e != OK {
				kv.dprintf("pull shards failed: %v", e)
				return false
			}
		} else {
			success := kv.pullShards(insertGrp)
			if !success {
				return false
			}
			kv.removeShards(insertGrp)
		}
		e, _ := kv.commonHandler(newRaftLogCommand(ReconfigEnd, kv.CurrCfg.Num))
		if e != OK {
			kv.dprintf("reconfig end failed")
			return false
		}
	}
	return true
}

func (kv *ShardKV) triggerConfigPoll() {
	select {
	case kv.configPollTrigger <- true:
	default:
	}
}

func (kv *ShardKV) reconfig() {
	// apply log entries to latest point
	e, _ := kv.commonHandler(newRaftLogCommand(EmptyEntry, 0))
	if e != OK {
		return
	}
	kv.dprintf("reconfig %d ...", kv.CurrCfg.Num)
	success := kv.installConfig()
	if success {
		kv.dprintf("reconfig %d success ...", kv.CurrCfg.Num)
	} else {
		kv.dprintf("reconfig %d failed ...", kv.CurrCfg.Num)
	}
}

func (kv *ShardKV) configPoller() {
	for !kv.killed() {
		select {
		case _, ok := <-kv.configPollTrigger:
			if !ok {
				return
			}
		case <-time.After(PollConfigInterval * time.Millisecond):
			if kv.killed() {
				return
			}
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		// reconfig unfinished
		if kv.CurrCfg.Num != kv.PrevCfg.Num {
			kv.mu.Unlock()
			kv.reconfig()
			continue
		}
		nextCfgNum := kv.CurrCfg.Num + 1
		kv.mu.Unlock()
		cfg := kv.mck.Query(nextCfgNum)
		if cfg.Num == 0 {
			continue
		}
		if cfg.Num != nextCfgNum {
			continue
		}
		e, _ := kv.commonHandler(newRaftLogCommand(ReconfigStart, cfg))
		if e != OK {
			continue
		}
		kv.reconfig()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt64(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt64(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RaftLogCommand{})
	labgob.Register(ClientOp{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(PullShardsArgs{})
	labgob.Register(PullShardsReply{})
	labgob.Register(RemoveShardsArgs{})
	labgob.Register(RemoveShardsReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(masters)
	kv.CurrCfg = shardmaster.Config{Num: 0}
	kv.Shards = make(map[int]Shard)
	kv.ShardRefTbl = make(map[int]ClientTbl)
	kv.commandTbl = make(map[int]commandEntry)
	kv.waitPullTbl = make(map[int]chan bool)
	kv.configPollTrigger = make(chan bool, 1)

	kv.readSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	go kv.configPoller()
	kv.configPollTrigger <- true // trigger the very first polling

	return kv
}
