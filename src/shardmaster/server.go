package shardmaster

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
	"mit-6.824/raft"
)

const (
	ConsensusTimeout        = 500
	ErrWrongLeader   Err    = "ErrWrongLeader"
	ErrShutdown      Err    = "ErrShutdown"
	Join             string = "Join"
	Leave            string = "Leave"
	Move             string = "Move"
	Query            string = "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int64

	// Your data here.

	configs     []Config // indexed by config num
	waitApplyCh map[int]chan applyResult
	clientTbl   map[int64]applyResult
}

type applyResult struct {
	Err    Err
	OpId   int
	Result Config
}

type Op struct {
	// Your data here.
	Args     interface{}
	ClientId int64
	OpId     int
	Method   string
}

func (sm *ShardMaster) commonHandler(op Op) (e Err, r Config) {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		e = ErrWrongLeader
		return
	}
	sm.mu.Lock()
	indexWaitCh, ok := sm.waitApplyCh[index]
	if !ok {
		sm.waitApplyCh[index] = make(chan applyResult)
		indexWaitCh = sm.waitApplyCh[index]
	}
	sm.mu.Unlock()
	select {
	case result, ok := <-indexWaitCh:
		if !ok {
			e = ErrShutdown
		} else {
			e = result.Err
			r = result.Result
		}
	case <-time.After(time.Millisecond * ConsensusTimeout):
		e = ErrWrongLeader
	}
	sm.mu.Lock()
	delete(sm.waitApplyCh, index)
	sm.mu.Unlock()
	return
}

func deepCopy(src map[int][]string) map[int][]string {
	dst := make(map[int][]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func CopyConfig(lastCfg *Config) Config {
	cfg := Config{
		Num:    lastCfg.Num + 1,
		Groups: make(map[int][]string),
	}
	copy(cfg.Shards[:], lastCfg.Shards[:])
	for k, v := range lastCfg.Groups {
		cfg.Groups[k] = v
	}
	return cfg
}

type gidShardsPair struct {
	gid    int
	shards []int
}

type gidShardsPairList []gidShardsPair

func (a gidShardsPairList) Len() int      { return len(a) }
func (a gidShardsPairList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a gidShardsPairList) Less(i, j int) bool {
	llen, rlen := len(a[i].shards), len(a[j].shards)
	return (llen < rlen) || ((llen == rlen) && (a[i].gid < a[j].gid))
}

func (cfg *Config) getGroupLoad() map[int][]int {
	groupLoad := make(map[int][]int)
	if len(cfg.Groups) == 0 {
		return groupLoad
	}
	for gid := range cfg.Groups {
		groupLoad[gid] = []int{}
	}
	for shard, gid := range cfg.Shards {
		groupLoad[gid] = append(groupLoad[gid], shard)
	}
	return groupLoad
}

func getSortedGroupLoad(groupLoad map[int][]int) gidShardsPairList {
	sortedGroupLoad := gidShardsPairList{}
	for k, v := range groupLoad {
		sortedGroupLoad = append(sortedGroupLoad, gidShardsPair{k, v})
	}
	sort.Sort(sortedGroupLoad)
	return sortedGroupLoad
}

func (cfg *Config) rebalance(oldCfg *Config, joinGids []int, leaveGids []int) {
	groupLoad := oldCfg.getGroupLoad()
	nOldGroups := len(groupLoad)
	switch {
	case len(leaveGids) > 0:
		nNewGroups := nOldGroups - len(leaveGids)
		if nNewGroups == 0 {
			for i := 0; i < NShards; i++ {
				cfg.Shards[i] = 0
			}
			break
		}
		avgLoad := NShards / nNewGroups
		nHigher := NShards % nNewGroups
		newLoads := make([]int, nNewGroups)
		for i := 0; i < len(newLoads); i++ {
			newLoads[i] = avgLoad
		}
		for i := 0; i < nHigher; i++ {
			newLoads[nNewGroups-i-1]++
		}
		removeLoad := make(map[int][]int)
		for _, gid := range leaveGids {
			removeLoad[gid] = groupLoad[gid]
			delete(groupLoad, gid)
		}
		newGroupLoad := getSortedGroupLoad(groupLoad)
		removeGroupLoad := getSortedGroupLoad(removeLoad)
		i, j := 0, 0
		for i < len(newGroupLoad) {
			toGid := newGroupLoad[i].gid
			iLoad := len(newGroupLoad[i].shards)
			for iLoad < newLoads[i] {
				for len(removeGroupLoad[j].shards) == 0 {
					j++
				}
				shard := removeGroupLoad[j].shards[0]
				cfg.Shards[shard] = toGid
				removeGroupLoad[j].shards = removeGroupLoad[j].shards[1:]
				iLoad++
			}
			i++
		}
	case len(joinGids) > 0:
		sortedGroupLoad := getSortedGroupLoad(groupLoad)
		nNewGroups := nOldGroups + len(joinGids)
		avgLoad := NShards / nNewGroups
		nHigher := NShards % nNewGroups
		newLoads := make([]int, nNewGroups)
		for i := 0; i < len(newLoads); i++ {
			newLoads[i] = avgLoad
		}
		for i := 0; i < nHigher; i++ {
			newLoads[nNewGroups-i-1]++
		}
		sort.Ints(joinGids)
		// no config
		if nOldGroups == 0 {
			j := 0
			for i, newGid := range joinGids {
				iLoad := 0
				for iLoad < newLoads[i] {
					cfg.Shards[j] = newGid
					j++
					iLoad++
				}
			}
		} else {
			j := 0
			for i, newGid := range joinGids {
				iLoad := 0
				for iLoad < newLoads[i] {
					for len(sortedGroupLoad[j].shards) <= newLoads[len(joinGids)+j] {
						j++
					}
					shard := sortedGroupLoad[j].shards[0]
					cfg.Shards[shard] = newGid
					sortedGroupLoad[j].shards = sortedGroupLoad[j].shards[1:]
					iLoad++
				}
			}
		}
	}
}

func (sm *ShardMaster) dprintf(format string, a ...interface{}) {
	args := []interface{}{}
	args = append(args, sm.rf.GetId())
	args = append(args, a...)
	_, isLeader := sm.rf.GetState()
	if isLeader {
		DPrintf("server(%d) "+format, args...)
	}
}

func (sm *ShardMaster) applier() {
	var r Config
	for m := range sm.applyCh {
		if !m.CommandValid {
			continue
		}
		op := m.Command.(Op)
		lastOpResult, ok := sm.clientTbl[op.ClientId]
		if ok && op.OpId < lastOpResult.OpId {
			r = lastOpResult.Result
		} else {
			switch op.Method {
			case Join:
				joinArg := op.Args.(JoinArgs)
				lastCfg := sm.configs[len(sm.configs)-1]
				newCfg := CopyConfig(&lastCfg)
				joinGids := []int{}
				for k, v := range joinArg.Servers {
					joinGids = append(joinGids, k)
					newCfg.Groups[k] = v
				}
				newCfg.rebalance(&lastCfg, joinGids, nil)
				sm.configs = append(sm.configs, newCfg)
				// sm.dprintf("shards = %v", newCfg.Shards)
				r = Config{Num: -1}
			case Leave:
				leaveArg := op.Args.(LeaveArgs)
				lastCfg := sm.configs[len(sm.configs)-1]
				newCfg := CopyConfig(&lastCfg)
				leaveGids := []int{}
				for _, gid := range leaveArg.GIDs {
					leaveGids = append(leaveGids, gid)
					delete(newCfg.Groups, gid)
				}
				newCfg.rebalance(&lastCfg, nil, leaveGids)
				sm.configs = append(sm.configs, newCfg)
				// sm.dprintf("shards = %v", newCfg.Shards)
				r = Config{Num: -1}
			case Move:
				moveArg := op.Args.(MoveArgs)
				gid := moveArg.GID
				shard := moveArg.Shard
				lastCfg := sm.configs[len(sm.configs)-1]
				newCfg := CopyConfig(&lastCfg)
				newCfg.Shards[shard] = gid
				sm.configs = append(sm.configs, newCfg)
				r = Config{Num: -1}
			case Query:
				queryArg := op.Args.(QueryArgs)
				num := queryArg.Num
				if num < 0 || num > len(sm.configs) {
					// initial: len(config) == 0, num < 0
					num = len(sm.configs) - 1
				}
				if num < 0 {
					r = Config{Num: -1}
				} else {
					r = sm.configs[num]
				}
			default:
				panic(op.Args)
			}
			sm.clientTbl[op.ClientId] = applyResult{OpId: op.OpId, Result: r}
		}
		sm.mu.Lock()
		if ch, ok := sm.waitApplyCh[m.CommandIndex]; ok {
			ch <- applyResult{OpId: op.OpId, Result: r}
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	err, _ := sm.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId, Method: Join})
	reply.Err = err
	if err == "" {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	err, _ := sm.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId, Method: Leave})
	reply.Err = err
	if err == "" {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	err, _ := sm.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId, Method: Move})
	reply.Err = err
	if err == "" {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	err, cfg := sm.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId, Method: Query})
	reply.Err = err
	reply.Config = cfg
	if err == "" {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	atomic.StoreInt64(&sm.dead, 1)
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	return atomic.LoadInt64(&sm.dead) == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	// start with an initial config
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.waitApplyCh = make(map[int]chan applyResult)
	sm.clientTbl = make(map[int64]applyResult)

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.applier()

	return sm
}
