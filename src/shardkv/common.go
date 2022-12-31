package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrNotReady       = "ErrNotReady"
	ErrDuplicate      = "ErrDuplicate"
	ErrFinished       = "ErrFinished"
	Debug             = 1
)

type Err string

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Method   OpMethod
	Key      string
	Value    string
	Shard    int
	GID      int
	ClinetId int64
	OpId     int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Shard    int
	GID      int
	ClinetId int64
	OpId     int
}

type GetReply struct {
	Err   Err
	Value string
}
