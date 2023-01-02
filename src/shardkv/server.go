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
	Erasing
	Waiting
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
	Method   OpMethod
	Args     ClerkArgs
}

type requestResult struct {
	err   Err
	value interface{} // for get Operation
}

type commandEntry struct {
	cmd     RaftLogCommand
	replyCh chan requestResult
}

type cache struct {
	opId   int
	result requestResult
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
	ClientTbl         map[int64]cache        // client id -> last op id
	commandTbl        map[int]commandEntry // index -> wait channel
	waitPullCh        chan int
	configPollTrigger chan bool
}

func (kv *ShardKV) dprintf(leaderRequired bool, format string, a ...interface{}) {
	_, isLeader := kv.rf.GetState()
	if leaderRequired && !isLeader {
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
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	if enc.Encode(kv.CurrCfg) != nil ||
		enc.Encode(kv.Shards) != nil ||
		enc.Encode(kv.ClientTbl) != nil {
			log.Fatalln("encode snapshot failed")
		}
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cfg shardmaster.Config
	var shards map[int]Shard
	var clientTbl map[int64]cache
	if d.Decode(&cfg) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&clientTbl) == nil {
		log.Fatalln("read broken snapshot")
	}
	kv.CurrCfg = cfg
	kv.Shards = shards
	kv.ClientTbl = clientTbl
}

///////////////////////////////
//           RPCs            //
///////////////////////////////

func (kv *ShardKV) checkShardStatus(si int) Err {
	shard, ok := kv.Shards[si]
	if !ok {
		return ErrWrongGroup
	}
	if shard.Status == Serving {
		return OK
	} else if shard.Status == Waiting {
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
	switch cmd.CommandType {
	case ClientRequest:
		cliOp := cmd.Data.(ClientOp)
		opCache, ok := kv.ClientTbl[cliOp.Args.GetClientId()]
		if ok && opCache.result.err == OK && opCache.opId > cliOp.Args.GetOpId() {
			e = OK
			r = opCache.result
			return
		}
		if cliOp.Args.GetCfgNum() > kv.CurrCfg.Num {
			kv.triggerConfigPoll()
			e = ErrNotReady
			return
		}
		if cliOp.Args.GetCfgNum() < kv.CurrCfg.Num {
			e = ErrOutdatedConfig
			return
		}
		if kv.CurrCfg.Num == 0 {
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
	kv.commandTbl[index] = commandEntry{cmd: cmd, replyCh: c}

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
				kv.dprintf(false, "start %d but not leader", index)
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

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
	opCache, ok := kv.ClientTbl[op.Args.GetClientId()]
	if !ok {
		return false
	}
	if op.Args.GetOpId() <= opCache.opId {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.readSnapshot(m.SnapshotData)
			continue
		}
		if !m.CommandValid {
			continue
		}
		cmd := m.Command.(RaftLogCommand)
		var reply requestResult
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
				continue
			}
			if cfg.Num != kv.CurrCfg.Num+1 {
				log.Fatalf("Receive config number %d, curr is %d", cfg.Num, kv.CurrCfg.Num)
			}
			// save previous config
			kv.PrevCfg = kv.CurrCfg
			kv.CurrCfg = cfg
			// change shard status
			for si, g := range cfg.Shards {
				if g == kv.gid {
					if shard, ok := kv.Shards[si]; ok {
						shard.Status = Serving
					} else {
						kv.Shards[si] = newShard(Serving)
					}
				}
			}
			kv.dprintf(true, "config %d start", cfg.Num)
		case ConfigChangeEnd:
			cfgNum := cmd.Data.(int)
			// mark the config change as applied
			kv.PrevCfg = kv.CurrCfg
			if kv.CurrCfg.Num != cfgNum {
				log.Fatalf("Config end with %d while curr is %d", cfgNum, kv.CurrCfg.Num)
			}
			kv.dprintf(true, "config %d end", cfgNum)
		case RemoveShard:
			args := cmd.Data.(RemoveShardsArgs)
			for _, si := range args.ShardNums {
				delete(kv.Shards, si)
			}
			kv.dprintf(true, "remove shards %v", args.ShardNums)
		case InsertShard:
			reply := cmd.Data.(PullShardsReply)
			sis := []int{}
			for si, shard := range reply.Shards {
				shard.Status = Serving
				kv.Shards[si] = shard
				sis = append(sis, si)
			}
			kv.dprintf(true, "insert shards %v", sis)
		}
		if ce, ok := kv.commandTbl[m.CommandIndex]; ok {
			ce.replyCh <- reply
		}
		kv.checkSnapshotInstall(m.CommandIndex)
	}
}

func (kv *ShardKV) applyClientRequest(op ClientOp) (reply requestResult) {
	si := op.Args.GetShard()
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
		opCache, ok := kv.ClientTbl[op.Args.GetClientId()]
		if !ok {
			kv.ClientTbl[op.Args.GetClientId()] = cache{
				opId:   op.Args.GetOpId(),
				result: reply,
			}
		} else {
			opCache.opId = op.Args.GetOpId()
			opCache.result = reply
		}
		kv.dprintf(true, "get %v:%v from shard %v", args.Key, value, si)
	case op.Method == PutOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			db = newShard(Serving)
			kv.Shards[si] = db
		}
		db.KV[args.Key] = args.Value
		kv.dprintf(true, "put %v:%v on shard %v", args.Key, args.Value, si)
	case op.Method == AppendOp:
		args := op.Args.(PutAppendArgs)
		db, ok := kv.Shards[si]
		if !ok {
			db = newShard(Serving)
			kv.Shards[si] = db
		}
		value, ok := db.KV[args.Key]
		if ok {
			db.KV[args.Key] = value + args.Value
		} else {
			db.KV[args.Key] = args.Value
		}
		kv.dprintf(true, "append %v:%v on shard %v", args.Key, args.Value, si)
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
	if kv.CurrCfg.Num != args.CfgNum {
		panic("PullShards RPC called with different config number")
	}
	replyShards := make(map[int]Shard)
	for _, si := range args.ShardNums {
		replyShards[si] = copyShard(kv.Shards[si])
	}
	reply.Err = OK
	reply.Shards = replyShards
}

func (kv *ShardKV) pullShards(groupShards map[int][]int, cfg shardmaster.Config) {
	// gid -> pulling shard numbers
	wg := sync.WaitGroup{}
	for g, sis := range groupShards {
		wg.Add(1)
		go func(gid int, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := PullShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := PullShardsReply{}
			servers := cfg.Groups[gid]
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
		}(g, sis, cfg.Num)
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
	if kv.CurrCfg.Num != args.CfgNum {
		panic("RemoveShards RPC called with different config number")
	}
	kv.commonHandler(newRaftLogCommand(RemoveShard, *args))
	kv.waitPullCh <- len(args.ShardNums)
	reply.Err = OK
}

func (kv *ShardKV) removeShards(groupShards map[int][]int, cfg shardmaster.Config) {
	wg := sync.WaitGroup{}
	for g, sis := range groupShards {
		wg.Add(1)
		go func(gid int, shardNums []int, cfgNum int) {
			defer wg.Done()
			args := RemoveShardsArgs{
				CfgNum:    cfgNum,
				ShardNums: shardNums,
			}
			reply := RemoveShardsReply{}
			servers := cfg.Groups[gid]
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
		}(g, sis, cfg.Num)
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
	Err    Err
	CfgNum int
}

func (kv *ShardKV) CoordinateConfig(args *CoordinateArgs, reply *CoordinateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	}
	reply.CfgNum = kv.CurrCfg.Num
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
		servs := kv.CurrCfg.Groups[gid]
		for _, srv := range servs {
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
	prevCfg := kv.PrevCfg
	newCfg := kv.CurrCfg
	// config done, return
	if prevCfg.Num == kv.CurrCfg.Num {
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
		kv.dprintf(true, "config finished")
		return
	}
	if len(removeGroup) == 0 && len(insertGroup) == 0 {
		kv.commonHandler(newRaftLogCommand(ConfigChangeEnd, newCfg.Num))
	}
	if len(removeGroup) > 0 {
		kv.dprintf(true, "remove group: %v", removeGroup)
		if isNullGroup(removeGroup) {
			// no machine will pull the shards
			args := RemoveShardsArgs{
				CfgNum:    newCfg.Num,
				ShardNums: removeGroup[0],
			}
			kv.commonHandler(newRaftLogCommand(RemoveShard, args))
		} else {
			for _, si := range removed {
				if shard, ok := kv.Shards[si]; ok {
					shard.Status = Erasing
				}
			}
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
		kv.dprintf(true, "insert group: %v", insertGroup)
		for _, si := range inserted {
			if shard, ok := kv.Shards[si]; ok {
				shard.Status = Pulling
			}
		}
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
			kv.pullShards(insertGroup, newCfg)
			kv.removeShards(insertGroup, newCfg)
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
		nextCfgNum := kv.CurrCfg.Num + 1
		cfg := kv.mck.Query(nextCfgNum)
		if cfg.Num == 0 {
			continue
		}
		if cfg.Num == kv.CurrCfg.Num {
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
	kv.ClientTbl = make(map[int64]cache)
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
