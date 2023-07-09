package paxos

import (
	"fmt"
	"net"
	"net/rpc"
	"syscall"
)

type Err string

const (
	OK          = "OK"
	ErrRejected = "ErrRejected"
)

type PrepareArgs struct {
	Instance int
	Proposal int
}

type PrepareReply struct {
	Err      Err
	Instance int
	Proposal int
	Value    interface{}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.dprintf("Prepare: seq = %d, n = %d\n", args.Instance, args.Proposal)
	n, v, ok := px.prepareHandler(args.Instance, args.Proposal)
	if ok {
		reply.Err = OK
		reply.Proposal, reply.Value = n, v
	} else {
		reply.Err = ErrRejected
		reply.Proposal = n
	}
	return nil
}

type AcceptArgs struct {
	Instance int
	Proposal int
	Value    interface{}
}

type AcceptReply struct {
	Err Err
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.dprintf("Accept: seq = %d, n = %d v = %v\n", args.Instance, args.Proposal, args.Value)
	ok := px.acceptHandler(args.Instance, args.Proposal, args.Value)
	if !ok {
		reply.Err = ErrRejected
	} else {
		reply.Err = OK
	}
	return nil
}

type DecidedArgs struct {
	Sender  int
	DoneSeq int

	Instance int
	Value    interface{}
}

type DecidedReply struct {
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.dprintf("Decided: seq = %d, v = %v\n", args.Instance, args.Value)

	px.values[args.Instance] = args.Value
	if px.doneSeqs[args.Sender] < args.DoneSeq {
		px.doneSeqs[args.Sender] = args.DoneSeq
	}
	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
