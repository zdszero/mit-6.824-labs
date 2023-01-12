package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"mit-6.824/labrpc"
	"mit-6.824/shardmaster"
)

const (
	ClientRefreshConfigInterval = 100
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me   int64
	opId int
}

func (ck *Clerk) buildCommonArgs(shard int, gid int) CommonArgs {
	opId := ck.opId
	ck.opId++
	return CommonArgs{
		CfgNum:   ck.config.Num,
		Shard:    shard,
		GID:      gid,
		ClientId: ck.me,
		OpId:     opId,
	}
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.me = nrand()
	ck.opId = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Common = ck.buildCommonArgs(shard, gid)
		sleepInterval := ClientRefreshConfigInterval
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
			Recall:
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					continue
				}
				if reply.Err == OK {
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					return ""
				}
				if reply.Err == ErrOutdatedConfig {
					sleepInterval = 0
					break
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == ErrNotReady || reply.Err == ErrInMigration {
					time.Sleep(time.Millisecond * time.Duration(sleepInterval))
					goto Recall
				}
				// ErrWrongLeader
			}
		}
		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, method OpMethod) {
	c := make(chan bool, 1)
	logEnable := false
	log := func(fmt string, args ... interface{}) {
		if logEnable {
			DPrintf(fmt, args...)
		}
	}
	defer func() { c <- true }()
	args := PutAppendArgs{}
	args.Method = method
	args.Key = key
	args.Value = value

	go func() {
		select {
		case <-c:
		case <-time.After(time.Second * 3):
			DPrintf("putappend(cli %d, op %d) %v on shard %d not finish in 3s", 
				args.GetClientId(), args.GetOpId(), key, key2shard(key))
			logEnable = true
		}
	}()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Common = ck.buildCommonArgs(shard, gid)
		sleepInterval := ClientRefreshConfigInterval
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
			Recall:
				log("call ...")
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					continue
				}
				if reply.Err == OK {
					return
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == ErrOutdatedConfig {
					sleepInterval = 0
					break
				}
				if reply.Err == ErrNotReady || reply.Err == ErrInMigration {
					time.Sleep(time.Millisecond * time.Duration(sleepInterval))
					goto Recall
				}
			}
		}
		time.Sleep(time.Millisecond * time.Duration(sleepInterval))
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
