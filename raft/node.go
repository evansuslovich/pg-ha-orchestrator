package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
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
	id            int
	name          string
	timeoutLength int // milliseconds
	role          Role

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

	timer     *time.Timer
	address   string // TCP address
	stop      chan struct{}
	addresses []string // other node addresses
}

type EmptyArgs struct{}

type ViewResponse struct {
	Node string
}

func (node *Node) View(args EmptyArgs, response *ViewResponse) error {
	response.Node = fmt.Sprintf(
		"==== Node %d (%s) ====\n"+
			"address:      %s\n"+
			"role:         %s\n"+
			"timeout:      %d ms\n"+
			"addresses:    %q\n"+
			"\n"+
			"-- persistent state --\n"+
			"currentTerm:  %d\n"+
			"votedFor:     %d\n"+
			"logs:          %v\n"+
			"\n"+
			"-- volatile state --\n"+
			"commitIndex:  %d\n"+
			"lastApplied:  %d\n"+
			"\n"+
			"-- leader state --\n"+
			"nextIndex:    %v\n"+
			"matchIndex:   %v\n"+
			"state:        %v\n"+
			"========================\n",
		node.id, node.name,
		node.address, node.role, node.timeoutLength, node.addresses,
		node.currentTerm, node.votedFor, node.logs,
		node.commitIndex, node.lastApplied,
		node.nextIndex, node.matchIndex, node.state,
	)
	return nil
}

type NodeArgs struct {
	Id            int
	Name          string
	Addresses     []string
	TimeoutLength int
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
		id:            args.Id,
		name:          args.Name,
		timeoutLength: 5000 + rand.N(args.TimeoutLength),
		role:          Follower,
		currentTerm:   1,
		state:         make(map[string]int),
		timer:         time.NewTimer(time.Millisecond),
		address:       this_address,
		addresses:     other_addresses,
		stop:          make(chan struct{}),
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

type StartElectionArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log
	LastLogTerm  int // term of candidate's last log entry
}

type StartElectionResponse struct {
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

func (node *Node) StartElection(candidate *StartElectionArgs, response *StartElectionResponse) error {
	// reject if candidate's term is old
	if candidate.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.VoteGranted = false
		return nil
	}

	// update node and make a Follower
	if candidate.Term > node.currentTerm {
		debugf("Candidate's (id: %d) term (%d)  is greater than node's (id: %d) ter (%d)", candidate.CandidateId, candidate.Term, node.id, node.currentTerm)
		node.currentTerm = candidate.Term
		node.role = Follower
		node.votedFor = 0 // represents null
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
		node.timer.Reset(time.Duration(node.timeoutLength) * time.Millisecond)
		response.VoteGranted = true
		debugf("Node id: %d is voting for Candidate id: %d", node.id, candidate.CandidateId)
	}

	return nil
}

// If a follower receives no communication over a period of time (electionTimeout)
// then it assumes there is no viable leader and begins an election
func (node *Node) beginElection() {
	node.currentTerm += 1
	node.role = Candidate
	node.votedFor = node.id

	// contact all nodes
	startElectionArgs := &StartElectionArgs{
		Term:         node.currentTerm,
		CandidateId:  node.id,
		LastLogIndex: len(node.logs),
		LastLogTerm:  node.LastLogTerm(),
	}

	// response to original sender
	var response StartElectionResponse
	vote_count := 1
	for _, address := range node.addresses {
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			log.Fatal("dialing:", err)
		}

		if err := client.Call("Node.StartElection", startElectionArgs, &response); err != nil {
			log.Fatal("raft error:", err)
		}

		if response.VoteGranted {
			vote_count += 1
		}
	}

	// all peers + self
	population := len(node.addresses) + 1
	majority := population/2 + 1
	if vote_count >= majority {
		node.role = Leader
		debugf("Node %s won the election", node.name)
	}
}

func (node *Node) Run() {
	fmt.Printf("%s starting \n", node.name)
	node.stop = make(chan struct{})
	for {
		start := time.Now()
		node.timer.Reset(time.Duration(node.timeoutLength) * time.Millisecond)

		select {
		case <-node.timer.C:
			elapsed := time.Since(start)
			fmt.Printf("%s elapsed for %s timeoutLength: %d\n", elapsed, node.name, node.timeoutLength)
			node.beginElection()
		case <-node.stop:
			fmt.Printf("%s stopping\n", node.name)
			return
		}
	}
}

func (node *Node) Stop() {
	close(node.stop)
}
