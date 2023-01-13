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
	"log"
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
	me          int64
	opId        int
	groupLeader map[int]int
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
	ck.groupLeader = make(map[int]int)
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
	args.Common.ClientId = ck.me
	args.Common.OpId = ck.opId
	ck.opId++

	shard := key2shard(key)
	args.Common.Shard = shard
	for {
		gid := ck.config.Shards[shard]
		args.Common.GID = gid
		args.Common.CfgNum = ck.config.Num
		sleepInterval := ClientRefreshConfigInterval
		serverId := ck.groupLeader[gid]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for i, nServers := 0, len(servers); i < nServers;	{
				srv := ck.make_end(servers[serverId])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
					serverId = (serverId + 1) % nServers
					i++
					continue
				}
				ck.groupLeader[gid] = serverId
				if reply.Err == ErrNotReady || reply.Err == ErrInMigration || reply.Err == ErrInitElection {
					time.Sleep(time.Millisecond * time.Duration(sleepInterval))
					continue
				}
				if reply.Err == ErrOutdatedConfig {
					sleepInterval = 0
					break
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == ErrNoKey {
					return ""
				}
				if reply.Err == OK {
					return reply.Value
				}
				log.Fatalf("Unknown reply.Err: %v", reply.Err)
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
	args := PutAppendArgs{}
	args.Method = method
	args.Key = key
	args.Value = value
	args.Common.ClientId = ck.me
	args.Common.OpId = ck.opId
	ck.opId++

	shard := key2shard(key)
	args.Common.Shard = shard
	for {
		gid := ck.config.Shards[shard]
		args.Common.GID = gid
		args.Common.CfgNum = ck.config.Num
		sleepInterval := ClientRefreshConfigInterval
		serverId := ck.groupLeader[gid]
		if servers, ok := ck.config.Groups[gid]; ok {
			for i, nServers := 0, len(servers); i < nServers;	{
				srv := ck.make_end(servers[serverId])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
					serverId = (serverId + 1) % nServers
					i++
					continue
				}
				ck.groupLeader[gid] = serverId
				if reply.Err == ErrNotReady || reply.Err == ErrInMigration || reply.Err == ErrInitElection {
					time.Sleep(time.Millisecond * time.Duration(sleepInterval))
					continue
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == ErrOutdatedConfig {
					sleepInterval = 0
					break
				}
				if reply.Err == OK {
					return
				}
				log.Fatalf("Unknown reply.Err: %v", reply.Err)
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
