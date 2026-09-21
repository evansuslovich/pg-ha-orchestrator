package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Role int

const (
	Follower  Role = iota // 0
	Candidate             // 1
	Leader                // 2
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

type LogEntry struct {
	Term    int // term at which LogEntry was created
	Command string
}

type Node struct {
	id               int
	name             string
	electionTimeout  int // milliseconds
	heartbeatTimeout int // millseconds
	role             Role

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state on all servers
	commitIndex int //index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	state      map[string]int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	address        string // TCP address
	stop           chan struct{}
	addresses      []string // other node addresses
}

type EmptyArgs struct{}

type ViewResponse struct {
	Node string
}

func (node *Node) View(args EmptyArgs, response *ViewResponse) error {
	response.Node = fmt.Sprintf(`===== Node %d (%s) =====
address:           %s
role:              %s
electionTimeout:   %d ms
heartbeatTimeout:  %d ms
addresses:         %q

-- persistent state --
currentTerm:       %d
votedFor:          %d
logs:              %v

-- volatile state --
commitIndex:       %d
lastApplied:       %d

-- leader state --
nextIndex:         %v
matchIndex:        %v
state:             %v
========================
`,
		node.id, node.name,
		node.address, node.role, node.electionTimeout, node.heartbeatTimeout, node.addresses,
		node.currentTerm, node.votedFor, node.logs,
		node.commitIndex, node.lastApplied,
		node.nextIndex, node.matchIndex, node.state,
	)
	return nil
}

type NodeArgs struct {
	Id              int
	Name            string
	Addresses       []string
	ElectionTimeout int
}

func getAddressInfo(args *NodeArgs) (string, []string) {
	this_address := args.Addresses[args.Id-1]
	other_addresses := make([]string, len(args.Addresses)-1)
	idx := 0
	for _, addr := range args.Addresses {
		if addr != this_address {
			other_addresses[idx] = addr
			idx += 1
		}
	}
	return this_address, other_addresses
}

func StartServer(args *NodeArgs) (*Node, error) {
	this_address, other_addresses := getAddressInfo(args)
	debugf("this address: %s . other_addresses: %q\n", this_address, other_addresses)

	node := &Node{
		id:               args.Id,
		name:             args.Name,
		electionTimeout:  1000 + rand.N(args.ElectionTimeout),
		heartbeatTimeout: 250,
		role:             Follower,
		currentTerm:      1,
		state:            make(map[string]int),
		electionTimer:    time.NewTimer(time.Millisecond),
		heartbeatTimer:   time.NewTimer(time.Millisecond),
		address:          this_address,
		addresses:        other_addresses,
		stop:             make(chan struct{}),
	}

	// stop the heartbeat timer off the bat, this only runs for a the leader
	if !node.heartbeatTimer.Stop() {
		<-node.heartbeatTimer.C
	}

	server := rpc.NewServer()
	server.Register(node)

	// https://pkg.go.dev/net#Listen
	l, err := net.Listen("tcp", node.address)
	if err != nil {
		return nil, errors.New("failed to spin up: " + err.Error())
	}

	debugf("serving on %s", node.address)
	go server.Accept(l)
	return node, nil
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteResponse struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (node *Node) LastLogTerm() int {
	lastLogTerm := 0
	if len(node.logs) > 0 {
		lastLogTerm = node.logs[len(node.logs)-1].Term
	}
	return lastLogTerm
}

func (node *Node) RequestVote(candidate *RequestVoteArgs, response *RequestVoteResponse) error {
	// reject if candidate's term is old
	if candidate.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.VoteGranted = false
		return nil
	}

	// update node and make a Follower
	if candidate.Term > node.currentTerm {
		node.currentTerm = candidate.Term
		node.role = Follower
		node.votedFor = 0 // represents null
	} else {
		debugf("Candidate's (id: %d) term (%d) is less than node's (id: %d) term (%d)", candidate.CandidateId, candidate.Term, node.id, node.currentTerm)
	}

	myLastLogIndex := len(node.logs)

	logOk := candidate.LastLogTerm > node.LastLogTerm() ||
		(candidate.LastLogTerm == node.LastLogTerm() && candidate.LastLogIndex >= myLastLogIndex)

	if !logOk {
		debugf("Candidate lastLogTerm or lastLogIndex is out of sync")
		response.Term = node.currentTerm
		response.VoteGranted = false
		return nil
	}

	response.Term = node.currentTerm
	canVote := node.votedFor == 0 || node.votedFor == candidate.CandidateId
	// server only votes for candidate if logs candidate are up to date
	if canVote {
		node.votedFor = candidate.CandidateId
		// reset election timer
		node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
		response.VoteGranted = true
		debugf("node %d voted for candidate %d", node.id, candidate.CandidateId)
	}

	return nil
}

// If a follower receives no communication over a period of time (electionTimeout)
// then it assumes there is no viable leader and begins an election

type requestVoteResult struct {
	electionResult RequestVoteResponse
	err            error
}

func (node *Node) startElection() error {
	debugf("Node %d is starting an election", node.id)
	node.currentTerm += 1
	node.role = Candidate
	node.votedFor = node.id

	// contact all nodes
	requestVoteArgs := &RequestVoteArgs{
		Term:         node.currentTerm,
		CandidateId:  node.id,
		LastLogIndex: len(node.logs),
		LastLogTerm:  node.LastLogTerm(),
	}

	results := make(chan requestVoteResult, len(node.addresses))
	var startWg sync.WaitGroup

	for i := 0; i < len(node.addresses); i++ {
		startWg.Add(1)

		go func(id int) {
			defer startWg.Done()

			client, err := rpc.Dial("tcp", node.addresses[i])
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var response RequestVoteResponse
			if err := client.Call("Node.RequestVote", requestVoteArgs, &response); err != nil {
				log.Fatal("raft error:", err)
			}

			results <- requestVoteResult{electionResult: response, err: err}
		}(i + 1)
	}

	go func() {
		startWg.Wait()
		close(results)
	}()

	vote_count := 1

	for res := range results {
		if res.err != nil {
			return errors.New("encountered an error in starting an election: " + res.err.Error())
		}

		if res.electionResult.VoteGranted {
			vote_count += 1
		}

		if node.currentTerm < res.electionResult.Term {
			node.currentTerm = res.electionResult.Term
		}
	}

	// all peers + self
	population := len(node.addresses) + 1
	majority := population/2 + 1
	if vote_count >= majority {
		node.role = Leader
		debugf("Node %d won the election", node.id)
	}

	return nil
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of previous log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type appendEntriesResult struct {
	replicateResult AppendEntriesResponse
	err             error
}

func (node *Node) AppendEntries(leader *AppendEntriesArgs, response *AppendEntriesResponse) error {
	debugf("Node %d received heartbeat / append entries from Node %d", node.id, leader.LeaderId)
	node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
	return nil
}

func (node *Node) replicate() error {
	debugf("Node %d is replicating", node.id)

	// contact all nodes
	appendEntriesArgs := &AppendEntriesArgs{
		Term:         node.currentTerm,
		LeaderId:     node.id,
		PrevLogIndex: len(node.logs),
		PrevLogTerm:  node.LastLogTerm(),
		Entries:      node.logs,
		LeaderCommit: node.commitIndex,
	}

	results := make(chan appendEntriesResult, len(node.addresses))
	var startWg sync.WaitGroup

	for i := 0; i < len(node.addresses); i++ {
		startWg.Add(1)

		go func(id int) {
			defer startWg.Done()

			client, err := rpc.Dial("tcp", node.addresses[i])
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var response AppendEntriesResponse
			if err := client.Call("Node.AppendEntries", appendEntriesArgs, &response); err != nil {
				log.Fatal("raft error:", err)
			}

			results <- appendEntriesResult{replicateResult: response, err: err}
		}(i + 1)
	}

	go func() {
		startWg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return errors.New("encountered an error in replicating: " + res.err.Error())
		}
	}
	return nil
}

func (node *Node) Run() {
	fmt.Printf("%s starting \n", node.name)
	node.stop = make(chan struct{})

	for {
		// when heartbeat timer runs out, it always resets the electionTimer
		node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
		if node.role == Leader {
			node.heartbeatTimer.Reset(time.Duration(node.heartbeatTimeout) * time.Millisecond)
		}
		select {
		case <-node.electionTimer.C:
			fmt.Printf("node %s elapsed electionTimeout: %d\n", node.name, node.electionTimeout)
			node.startElection()
		case <-node.heartbeatTimer.C:
			fmt.Printf("node %s heartbeat timer: %d\n", node.name, node.heartbeatTimeout)
			node.replicate()
		case <-node.stop:
			fmt.Printf("node %s stopping\n", node.name)
			return
		}
	}
}

func (node *Node) Stop() {
	close(node.stop)
}
