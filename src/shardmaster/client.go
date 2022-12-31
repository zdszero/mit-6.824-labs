package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"mit-6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me     int64
	leader int
	opId   int
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
	// Your code here.
	ck.me = nrand()
	ck.leader = 0
	ck.opId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.ClientId = ck.me
	args.OpId = ck.opId
	ck.opId++
	args.Num = num

	serverId := ck.leader
	originId := serverId
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[serverId].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leader = serverId
			return reply.Config
		}
		serverId = (serverId + 1) % len(ck.servers)
		if serverId == originId {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.ClientId = ck.me
	args.OpId = ck.opId
	ck.opId++
	args.Servers = servers

	serverId := ck.leader
	originId := serverId
	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[serverId].Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leader = serverId
			return
		}
		serverId = (serverId + 1) % len(ck.servers)
		if serverId == originId {
			time.Sleep(100 * time.Millisecond)
		}
	}

}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.ClientId = ck.me
	args.OpId = ck.opId
	ck.opId++
	args.GIDs = gids

	serverId := ck.leader
	originId := serverId
	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[serverId].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leader = serverId
			return
		}
		serverId = (serverId + 1) % len(ck.servers)
		if serverId == originId {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.ClientId = ck.me
	args.OpId = ck.opId
	ck.opId++
	args.Shard = shard
	args.GID = gid

	serverId := ck.leader
	originId := serverId
	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[serverId].Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leader = serverId
			return
		}
		serverId = (serverId + 1) % len(ck.servers)
		if serverId == originId {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
