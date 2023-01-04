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
	PollConfigInterval     = 100
	CoordinateInterval     = 100
)

//
// raft log entry defination
//
type CommandType int

const (
	ClientRequest CommandType = iota
	ConfigChangeStart
	InsertShard
	RemoveShard
	ConfigChangeEnd
)

type RaftLogCommand struct {
	CommandType
	Data interface{}
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
	Waiting
	Erasing
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
	PrevCfg           shardmaster.Config
	CurrCfg           shardmaster.Config
	Shards            map[int]Shard        // shard -> DB: (key -> value)
	ClientTbl         map[int64]int        // client id -> last op id
	commandTbl        map[int]commandEntry // index -> wait channel
	waitPullCh        chan int
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
	if e = enc.Encode(kv.CurrCfg); e != nil {
		log.Fatalln("Failed to encode currCfg")
	}
	if e = enc.Encode(kv.PrevCfg); e != nil {
		log.Fatalln("Failed to encode prevCfg")
	}
	if e = enc.Encode(kv.Shards); e != nil {
		log.Fatalln("Failed to encode shards")
	}
	if e = enc.Encode(kv.ClientTbl); e != nil {
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
	var clientTbl map[int64]int
	var e error
	// kv.dprintf("decode data: %v", data)
	if e = dec.Decode(&currCfg); e != nil {
		log.Fatalln("Failed to decode currCfg")
	}
	if e = dec.Decode(&prevCfg); e != nil {
		log.Fatalln("Failed to decode prevCfg")
	}
	if e = dec.Decode(&shards); e != nil {
		log.Fatalln("Failed to decode shards")
	}
	if e = dec.Decode(&clientTbl); e != nil {
		log.Fatalln("Failed to decode clientTbl")
	}
	kv.CurrCfg = currCfg
	kv.PrevCfg = prevCfg
	kv.Shards = shards
	kv.ClientTbl = clientTbl
}

///////////////////////////////
//           RPCs            //
///////////////////////////////

func (kv *ShardKV) checkShardStatus(si int) Err {
	kv.mu.Lock()
	shard, ok := kv.Shards[si]
	kv.mu.Unlock()
	if !ok {
		return ErrWrongGroup
	}
	if shard.Status == Serving {
		return OK
	} else if shard.Status == Pulling {
		return ErrInMigration
	} else {
		return ErrWrongGroup
	}
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
	kv.mu.Lock()
	curCfgNum := kv.CurrCfg.Num
	kv.mu.Unlock()
	switch cmd.CommandType {
	case ClientRequest:
		cliOp := cmd.Data.(ClientOp)
		if cliOp.Args.GetCfgNum() > curCfgNum {
			kv.triggerConfigPoll()
			e = ErrNotReady
			return
		}
		if cliOp.Args.GetCfgNum() < curCfgNum {
			e = ErrOutdatedConfig
			return
		}
		if curCfgNum == 0 {
			e = ErrWrongGroup
			return
		}
		if err := kv.checkShardStatus(cliOp.Args.GetShard()); err != OK {
			e = err
			return
		}
	}

	index, term, isLeader := kv.rf.Start(cmd)
	if term == 0 {
		e = ErrInitElection
		return
	}
	if !isLeader {
		e = ErrWrongLeader
		return
	}

	c := make(chan requestResult)
	kv.mu.Lock()
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
			if t, _ := kv.rf.GetState(); term != t {
				DPrintf("start %d but not leader", index)
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	// avoid applier from blocking and resource leak
	go func() { <- c }()

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
	lastOpId, ok := kv.ClientTbl[op.Args.GetClientId()]
	if !ok {
		return false
	}
	if op.Args.GetOpId() <= lastOpId {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) applier() {
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
		cmd := m.Command.(RaftLogCommand)
		var reply requestResult
		kv.mu.Lock()
		switch cmd.CommandType {
		case ClientRequest:
			op := cmd.Data.(ClientOp)
			if kv.isOpDuplicate(op) {
				break
			}
			reply = kv.applyClientRequest(op)
		case ConfigChangeStart:
			cfg := cmd.Data.(shardmaster.Config)
			if cfg.Num <= kv.CurrCfg.Num {
				break
			}
			if cfg.Num != kv.CurrCfg.Num+1 {
				log.Fatalf("Receive config number %d, curr is %d", cfg.Num, kv.CurrCfg.Num)
			}
			// save previous config
			kv.PrevCfg = kv.CurrCfg
			kv.CurrCfg = cfg
			kv.dprintf("config %d start", cfg.Num)
		case ConfigChangeEnd:
			cfgNum := cmd.Data.(int)
			// mark the config change as applied
			kv.PrevCfg = kv.CurrCfg
			if kv.CurrCfg.Num != cfgNum {
				log.Fatalf("Config end with %d while curr is %d", cfgNum, kv.CurrCfg.Num)
			}
			kv.dprintf("config %d end", cfgNum)
		case RemoveShard:
			args := cmd.Data.(RemoveShardsArgs)
			for _, si := range args.ShardNums {
				delete(kv.Shards, si)
			}
			kv.dprintf("remove shards %v", args.ShardNums)
		case InsertShard:
			reply := cmd.Data.(PullShardsReply)
			sis := []int{}
			for si, shard := range reply.Shards {
				shard.Status = Serving
				kv.Shards[si] = shard
				sis = append(sis, si)
			}
			kv.dprintf("insert shards %v", sis)
		}
		if ce, ok := kv.commandTbl[m.CommandIndex]; ok {
			ce.replyCh <- reply
		}
		kv.mu.Unlock()
		kv.checkSnapshotInstall(m.CommandIndex)
	}
}

func (kv *ShardKV) applyClientRequest(op ClientOp) (reply requestResult) {
	si := op.Args.GetShard()
	kv.ClientTbl[op.Args.GetClientId()] = op.Args.GetOpId()
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
		kv.dprintf("get %v:%v from shard %v", args.Key, value, si)
	case op.Method == PutOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			log.Fatalf("shard %d not exist when put", si)
		}
		db.KV[args.Key] = args.Value
		kv.dprintf("put %v:%v on shard %v", args.Key, args.Value, si)
	case op.Method == AppendOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			log.Fatalf("shard %d not exist when append", si)
		}
		value, ok := db.KV[args.Key]
		if ok {
			db.KV[args.Key] = value + args.Value
		} else {
			db.KV[args.Key] = args.Value
		}
		kv.dprintf("append %v:%v on shard %v", args.Key, args.Value, si)
	}
	reply.err = OK
	return
}

type PullShardsArgs struct {
	CfgNum    int
	ShardNums []int
}

type PullShardsReply struct {
	Err    Err
	Shards map[int]Shard
}

func (kv *ShardKV) PullShards(args *PullShardsArgs, reply *PullShardsReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.mu.Unlock()
	if args.CfgNum != kv.CurrCfg.Num {
		log.Fatalf("PullShards RPC called with different config number: %d, %d", args.CfgNum, kv.CurrCfg.Num)
	}
	prepared := true
	for _, si := range args.ShardNums {
		if kv.Shards[si].Status != Waiting {
			prepared = false
			break
		}
	}
	if !prepared {
		reply.Err = ErrNotReady
		return
	}
	replyShards := make(map[int]Shard)
	for _, si := range args.ShardNums {
		replyShards[si] = copyShard(kv.Shards[si])
		if shard, ok := kv.Shards[si]; ok {
			shard.Status = Erasing
			kv.Shards[si] = shard
		}
	}
	reply.Err = OK
	reply.Shards = replyShards
}

func (kv *ShardKV) getServers(gid int) []string {
	var cfg shardmaster.Config
	if _, ok := kv.CurrCfg.Groups[gid]; ok {
		cfg = kv.CurrCfg
	} else {
		cfg = kv.PrevCfg
	}
	servers := cfg.Groups[gid]
	if len(servers) == 0 {
		panic("get servers return empty")
	}
	return cfg.Groups[gid]
}

func (kv *ShardKV) pullShards(groupShards map[int][]int) {
	// gid -> pulling shard numbers
	wg := sync.WaitGroup{}
	for g, sis := range groupShards {
		wg.Add(1)
		kv.mu.Lock()
		servers := kv.getServers(g)
		cfgNum := kv.CurrCfg.Num
		kv.mu.Unlock()
		go func(servers []string, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := PullShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := PullShardsReply{}
			done := false
			for {
				for _, sn := range servers {
					end := kv.make_end(sn)
					ok := end.Call("ShardKV.PullShards", &args, &reply)
					if ok && reply.Err == OK {
						kv.commonHandler(newRaftLogCommand(InsertShard, reply))
						done = true
						break
					}
				}
				if done {
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
		}(servers, sis, cfgNum)
	}
	wg.Wait()
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
	func (){
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if args.CfgNum != kv.CurrCfg.Num {
			log.Fatalf("PullShards RPC called with different config number: %d, %d", args.CfgNum, kv.CurrCfg.Num)
		}
		prepared := true
		for _, si := range args.ShardNums {
			if kv.Shards[si].Status != Erasing {
				prepared = false
				break
			}
		}
		if !prepared {
			reply.Err = ErrNotReady
			return
		}
	}()
	kv.commonHandler(newRaftLogCommand(RemoveShard, *args))
	kv.waitPullCh <- len(args.ShardNums)
	reply.Err = OK
}

func (kv *ShardKV) removeShards(groupShards map[int][]int) {
	wg := sync.WaitGroup{}
	for g, sis := range groupShards {
		wg.Add(1)
		kv.mu.Lock()
		servers := kv.getServers(g)
		cfgNum := kv.CurrCfg.Num
		kv.mu.Unlock()
		go func(servers []string, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := RemoveShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := RemoveShardsReply{}
			done := false
			for {
				for _, sn := range servers {
					end := kv.make_end(sn)
					ok := end.Call("ShardKV.RemoveShards", &args, &reply)
					if ok && reply.Err == OK {
						done = true
						break
					}
				}
				if done {
					break
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

type CoordinateArgs struct {
	CfgNum int
}

type CoordinateReply struct {
	Err Err
}

func (kv *ShardKV) CoordinateConfig(args *CoordinateArgs, reply *CoordinateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	kv.mu.Unlock()
	if args.CfgNum < kv.CurrCfg.Num {
		reply.Err = ErrDuplicate
	} else if args.CfgNum > kv.CurrCfg.Num {
		reply.Err = ErrNotReady
	} else {
		if kv.CurrCfg.Num != kv.PrevCfg.Num {
			reply.Err = OK
		} else {
			reply.Err = ErrFinished
		}
	}
}

// if all group's leader current config number is equal to my config number
//   then return true
// else return false
func (kv *ShardKV) coordinateConfig(gids []int) Err {
	for _, gid := range gids {
		servers := kv.getServers(gid)
		for _, srv := range servers {
			end := kv.make_end(srv)
			args := CoordinateArgs{CfgNum: kv.CurrCfg.Num}
			var reply CoordinateReply
			ok := end.Call("ShardKV.CoordinateConfig", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err != OK {
				return reply.Err
			}
			break
		}
	}
	return OK
}

// apply new config
func (kv *ShardKV) reconfig() {
	kv.mu.Lock()
	prevCfg := kv.PrevCfg
	newCfg := kv.CurrCfg
	kv.mu.Unlock()
	// config done, return
	if prevCfg.Num == newCfg.Num {
		return
	}
	// leader is responsible for adding migration log entry
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	// config num == 0 means no config
	oldshards := prevCfg.CollectShards(kv.gid)
	newshards := newCfg.CollectShards(kv.gid)
	// remove group to who
	removed, removeGroup := getDiffShards(newshards, oldshards, newCfg.Shards[:])
	// to pull the new group from who
	inserted, insertGroup := getDiffShards(oldshards, newshards, prevCfg.Shards[:])
	// start pulling and sending at the same time
	if len(removeGroup) > 0 && len(insertGroup) > 0 {
		panic("Error: both transfer and accept shards in one config change")
	}
	gids := []int{}
	for g := range removeGroup {
		if g > 0 {
			gids = append(gids, g)
		}
	}
	for g := range insertGroup {
		if g > 0 {
			gids = append(gids, g)
		}
	}
	var err Err
	for {
		err = kv.coordinateConfig(gids)
		if err == OK || err == ErrFinished {
			break
		}
		// when restarting, the lastest config in log might not be applied
		// wait until this group reach the lastest consensus config of whole system
		time.Sleep(time.Millisecond * CoordinateInterval)
	}
	if err == ErrFinished {
		kv.dprintf("config %d finished", kv.CurrCfg.Num)
		return
	}
	if len(removeGroup) == 0 && len(insertGroup) == 0 {
		kv.commonHandler(newRaftLogCommand(ConfigChangeEnd, newCfg.Num))
	}
	if len(removeGroup) > 0 {
		kv.dprintf("remove group: %v", removeGroup)
		if isNullGroup(removeGroup) {
			// no machine will pull the shards
			args := RemoveShardsArgs{
				CfgNum:    newCfg.Num,
				ShardNums: removeGroup[0],
			}
			kv.commonHandler(newRaftLogCommand(RemoveShard, args))
		} else {
			kv.mu.Lock()
			for _, si := range removed {
				if shard, ok := kv.Shards[si]; ok {
					shard.Status = Waiting
					kv.Shards[si] = shard
				}
			}
			kv.mu.Unlock()
			// wait for shards to be pulled by other machines
			done := 0
			total := 0
			for _, grp := range removeGroup {
				total += len(grp)
			}
			for done < total {
				cnt := <-kv.waitPullCh
				done += cnt
			}
		}
		kv.commonHandler(newRaftLogCommand(ConfigChangeEnd, newCfg.Num))
	}
	if len(insertGroup) > 0 {
		kv.dprintf("insert group: %v", insertGroup)
		kv.mu.Lock()
		for _, si := range inserted {
			if shard, ok := kv.Shards[si]; ok {
				shard.Status = Pulling
				kv.Shards[si] = shard
			}
		}
		kv.mu.Unlock()
		if isNullGroup(insertGroup) {
			shards := make(map[int]Shard)
			for _, si := range insertGroup[0] {
				shards[si] = newShard(Serving)
			}
			reply := PullShardsReply{
				Err:    OK,
				Shards: shards,
			}
			kv.commonHandler(newRaftLogCommand(InsertShard, reply))
		} else {
			kv.pullShards(insertGroup)
			kv.removeShards(insertGroup)
		}
		kv.commonHandler(newRaftLogCommand(ConfigChangeEnd, newCfg.Num))
	}
}

func (kv *ShardKV) triggerConfigPoll() {
	select {
	case kv.configPollTrigger <- true:
	default:
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
		// only accept the next shard
		kv.mu.Lock()
		nextCfgNum := kv.CurrCfg.Num + 1
		kv.mu.Unlock()
		cfg := kv.mck.Query(nextCfgNum)
		if cfg.Num == 0 {
			continue
		}
		if cfg.Num != nextCfgNum {
			continue
		}
		kv.commonHandler(newRaftLogCommand(ConfigChangeStart, cfg))
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
	kv.ClientTbl = make(map[int64]int)
	kv.commandTbl = make(map[int]commandEntry)
	kv.waitPullCh = make(chan int, 1)
	kv.configPollTrigger = make(chan bool, 1)

	kv.readSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	go kv.configPoller()
	kv.configPollTrigger <- true // trigger the very first polling

	return kv
}
