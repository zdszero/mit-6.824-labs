package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
	Debug     = true
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	maxSeqSeen int
	doneSeqs   []int
	values     map[int]interface{}
	accpState  map[int]State // state for each seq
}

type State struct {
	// highest prepare request proposal number received by the acceptor
	// for that instance
	np int
	// highest accept request proposal number request number
	na int
	// value of highest accepted proposal
	va interface{}
}

func (px *Paxos) dprintf(fmt string, a ...interface{}) {
	fmt = "[%s] " + fmt
	args := []interface{}{px.self()}
	args = append(args, a...)
	if Debug {
		log.Printf(fmt, args...)
	}
}

func (px *Paxos) self() string {
	return px.peers[px.me]
}

func (px *Paxos) isSelf(peer string) bool {
	return px.self() == peer
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	px.dprintf("start %d:%v\n", seq, v)
	if seq < px.Min() {
		px.dprintf("seq < min, finish %d:%v", seq, v)
		return
	}
	px.mu.Lock()
	if seq > px.maxSeqSeen {
		px.maxSeqSeen = seq
	}
	px.mu.Unlock()
	go px.propose(seq, v)
}

// choose the next np for current seq's state
func (px *Paxos) chooseProposalNumber(seq int) int {
	px.mu.Lock()
	defer px.mu.Unlock()
	n := px.accpState[seq].np
	return n + 1
}

// acceptor's prepare(n) handler:
//
//	if n > n_p
//	  n_p = n
//	  reply prepare_ok(n, n_a, v_a)
//	else
//	  reply prepare_reject
func (px *Paxos) prepareHandler(seq int, n int) (na int, v interface{}, ok bool) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq > px.maxSeqSeen {
		px.maxSeqSeen = seq
	}
	state := px.accpState[seq]
	if n > state.np {
		state.np = n
		na, v = state.na, state.va
		ok = true
		px.accpState[seq] = state
		return
	} else {
		ok = false
		return
	}
}

// return:
// na int, v interface{}, ok bool
func (px *Paxos) prepare(peer string, seq int, n int) (int, interface{}, bool) {
	if px.isSelf(peer) {
		return px.prepareHandler(seq, n)
	} else {
		args := PrepareArgs{
			Instance: seq,
			Proposal: n,
		}
		reply := PrepareReply{}
		ok := call(peer, "Paxos.Prepare", &args, &reply)
		if !ok {
			return 0, nil, false
		}
		if reply.Err == OK {
			return reply.Proposal, reply.Value, true
		} else {
			return reply.Proposal, nil, false
		}
	}
}

func (px *Paxos) updateProposalNumber(seq int, n int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	state := px.accpState[seq]
	if n > state.np {
		state.np = n
		px.accpState[seq] = state
		return true
	}
	return false
}

// acceptor's accept(n, v) handler:
//
//	if n >= n_p
//	  n_p = n
//	  n_a = n
//	  v_a = v
//	  reply accept_ok(n)
//	else
//	  reply accept_reject
func (px *Paxos) acceptHandler(seq int, n int, v interface{}) bool {
	px.mu.Lock()
	defer px.mu.Unlock()
	state := px.accpState[seq]
	if n >= state.np {
		state.np = n
		state.na = n
		state.va = v
		px.accpState[seq] = state
		return true
	} else {
		return false
	}
}

func (px *Paxos) accept(peer string, seq int, n int, v interface{}) bool {
	if px.isSelf(peer) {
		return px.acceptHandler(seq, n, v)
	} else {
		args := AcceptArgs{
			Instance: seq,
			Proposal: n,
			Value:    v,
		}
		reply := AcceptReply{}
		ok := call(peer, "Paxos.Accept", &args, &reply)
		if !ok {
			return false
		}
		return reply.Err == OK
	}
}

func (px *Paxos) decided(peer string, seq int, v interface{}) {
	if px.isSelf(peer) {
		px.values[seq] = v
	} else {
		args := DecidedArgs{
			Sender:   px.me,
			DoneSeq:  px.doneSeqs[px.me],
			Instance: seq,
			Value:    v,
		}
		reply := DecidedReply{}
		call(peer, "Paxos.Decided", &args, &reply)
	}
}

// proposer(v):
//
//	while not decided:
//	  choose n, unique and higher than any n seen so far
//	  send prepare(n) to all servers including self
//	  if prepare_ok(n, n_a, v_a) from majority:
//	    v' = v_a with highest n_a; choose own v otherwise
//	    send accept(n, v') to all
//	    if accept_ok(n) from majority:
//	      send decided(v') to all
func (px *Paxos) propose(seq int, v interface{}) {
	for !px.isDecided(seq) {
		n := px.chooseProposalNumber(seq)

		prepared := false
		v1 := v
		func() {
			okcnt := 0
			maxna := 0
			for _, peer := range px.peers {
				na, va, ok := px.prepare(peer, seq, n)
				if ok {
					if na > maxna {
						maxna = na
						v1 = va
					}
					okcnt++
				} else {
					if px.updateProposalNumber(seq, na) {
						return
					}
				}
			}
			px.dprintf("prepare ok count = %d\n", okcnt)
			if okcnt > len(px.peers)/2 {
				prepared = true
				return
			} else {
				return
			}
		}()
		if !prepared {
			continue
		}

		accepted := false
		func() {
			okcnt := 0
			for _, peer := range px.peers {
				ok := px.accept(peer, seq, n, v1)
				if ok {
					okcnt++
				}
			}
			px.dprintf("accept ok count = %d\n", okcnt)
			if okcnt > len(px.peers)/2 {
				accepted = true
			}
		}()
		if !accepted {
			continue
		}

		for _, peer := range px.peers {
			px.decided(peer, seq, v1)
		}

		break
	}
	px.dprintf("decided %d:%v", seq, v)
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.doneSeqs[px.me] {
		px.doneSeqs[px.me] = seq
	}
	px.doMemShrink()
}

func (px *Paxos) doMemShrink() int {
	mins := px.doneSeqs[px.me]
	for _, s := range px.doneSeqs {
		if s < mins {
			mins = s
		}
	}
	for seq := range px.accpState {
		if seq <= mins {
			delete(px.accpState, seq)
			delete(px.values, seq)
		}
	}
	return mins
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeqSeen
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.doMemShrink() + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}
	px.mu.Lock()
	v, ok := px.values[seq]
	px.mu.Unlock()
	if ok {
		return Decided, v
	}
	return Pending, nil
}

func (px *Paxos) isDecided(seq int) bool {
	f, _ := px.Status(seq)
	return f == Decided
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.maxSeqSeen = -1
	px.doneSeqs = make([]int, len(peers))
	for i := range px.doneSeqs {
		px.doneSeqs[i] = -1
	}
	px.values = make(map[int]interface{})
	px.accpState = make(map[int]State)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
