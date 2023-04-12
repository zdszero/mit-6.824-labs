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
	ErrShutdown       = "ErrShutdown"
	ErrInitElection   = "ErrInitElection"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrNotReady       = "ErrNotReady"
	ErrOutdatedConfig = "ErrOutdatedConfig"
	ErrInMigration    = "ErrInMigration"
	ErrDuplicate      = "ErrDuplicate"
	ErrFinished       = "ErrFinished"
	Debug             = 0
)

type Err string

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ClerkArgs interface {
	GetCfgNum() int
	GetGid() int
	GetShard() int
	GetClientId() int64
	GetOpId() int
}

type CommonArgs struct {
	CfgNum   int
	Shard    int
	GID      int
	ClientId int64
	OpId     int
}

type PutAppendArgs struct {
	Method OpMethod
	Key    string
	Value  string
	Common CommonArgs
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	Common CommonArgs
}

type GetReply struct {
	Err   Err
	Value string
}

func (args PutAppendArgs) GetCfgNum() int     { return args.Common.CfgNum }
func (args PutAppendArgs) GetGid() int        { return args.Common.GID }
func (args PutAppendArgs) GetShard() int      { return args.Common.Shard }
func (args PutAppendArgs) GetClientId() int64 { return args.Common.ClientId }
func (args PutAppendArgs) GetOpId() int       { return args.Common.OpId }

func (args GetArgs) GetCfgNum() int     { return args.Common.CfgNum }
func (args GetArgs) GetGid() int        { return args.Common.GID }
func (args GetArgs) GetShard() int      { return args.Common.Shard }
func (args GetArgs) GetClientId() int64 { return args.Common.ClientId }
func (args GetArgs) GetOpId() int       { return args.Common.OpId }
