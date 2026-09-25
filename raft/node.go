package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"strconv"
	"strings"
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

func compareLogEntries(this LogEntry, other LogEntry) bool {
	return this.Term == other.Term && this.Command == other.Command
}

type NodeState int

const (
	Running NodeState = iota // 0
	Paused                   // 1
	Fresh                    // 2
)

func (s NodeState) String() string {
	return [...]string{"Running", "Paused", "Fresh"}[s]
}

type Condition struct {
	mu      sync.Mutex
	state   NodeState
	pauseCh chan struct{} // closed the instant Pause() is called, wakes a blocked Run()
}

type Node struct {
	id               int
	electionTimeout  int // milliseconds
	heartbeatTimeout int // millseconds
	role             Role
	condition        Condition
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer
	address          string   // TCP address
	addresses        []string // other node addresses

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state on all servers

	// index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	data       int
}

type EmptyArgs struct{}

type ViewResponse struct {
	Node string
}

func (node *Node) View(args EmptyArgs, response *ViewResponse) error {
	response.Node = fmt.Sprintf(`===== Node %d  =====
address:           %s
role:              %s
condition:         %s
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
data:              %d
========================
`,
		node.id,
		node.address, node.role, node.condition.state, node.electionTimeout, node.heartbeatTimeout, node.addresses,
		node.currentTerm, node.votedFor, node.logs,
		node.commitIndex, node.lastApplied,
		node.nextIndex, node.matchIndex, node.data,
	)
	return nil
}

type NodeArgs struct {
	Id              int
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
		electionTimeout:  1000 + rand.N(args.ElectionTimeout),
		heartbeatTimeout: 999,
		role:             Follower,
		condition:        Condition{state: Fresh, pauseCh: make(chan struct{})},
		currentTerm:      1,
		electionTimer:    time.NewTimer(time.Millisecond),
		heartbeatTimer:   time.NewTimer(time.Millisecond),
		address:          this_address,
		addresses:        other_addresses,
		data:             0,
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
	if node.condition.state == Paused {
		debugf("Node %d is paused", node.id)
		return nil
	}

	// reject if candidate's term is old
	if candidate.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.VoteGranted = false

		debugf("Node %d is rejecting Candidate %d due to stale terms", node.id, candidate.CandidateId)
		debugf("Node %d's term: %d > Candidate %d term: %d", node.id, node.currentTerm, candidate.CandidateId, candidate.Term)
		return nil
	}

	// update node and make a Follower
	if candidate.Term > node.currentTerm {
		node.currentTerm = candidate.Term
		node.role = Follower
		node.votedFor = 0 // represents null
		node.heartbeatTimer.Stop()
	} else {
		debugf("[Stale Candidate] Node %d term: %d < Candidate %d term %d", node.id, node.currentTerm, candidate.CandidateId, candidate.Term)
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
		debugf("Node %d voted for candidate %d", node.id, candidate.CandidateId)
	}

	return nil
}

type requestVoteResult struct {
	electionResult RequestVoteResponse
	err            error
}

func (node *Node) newLeader() {
	debugf("Node %d won the election", node.id)
	node.role = Leader
	node.nextIndex = make([]int, len(node.addresses))
	node.matchIndex = make([]int, len(node.addresses))

	for i := range node.addresses {
		// at most the size of the node's logs
		node.nextIndex[i] = len(node.logs) + 1
		// at least
		node.matchIndex[i] = 0
	}

	node.replicate(&SetArgs{Command: ""})
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
		node.newLeader()
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
	follower_idx    int
}

func (node *Node) AppendEntries(leader *AppendEntriesArgs, response *AppendEntriesResponse) error {
	if node.condition.state == Paused {
		debugf("Node %d is paused", node.id)
		return nil
	}

	// stale leader AppendEntries
	if leader.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.Success = false
		debugf("[Stale Leader] Node %d term %d is older than Node %d term: %d", leader.LeaderId, leader.Term, node.id, node.currentTerm)
		return nil
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	// stale Node
	if leader.Term > node.currentTerm {
		debugf("[Stale Node] Node %d term: %d is older than Node %d term %d", node.id, node.currentTerm, leader.LeaderId, leader.Term)
		node.currentTerm = leader.Term
		node.role = Follower
		node.votedFor = 0 // represents null
		node.heartbeatTimer.Stop()
	}

	// if the current node is in a candidate position and we receive an AppendEntries from a new leader
	if node.role == Candidate {
		node.role = Follower
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose terms matches prevLogTerm
	if leader.PrevLogIndex > len(node.logs) ||
		(leader.PrevLogIndex > 0 && node.logs[leader.PrevLogIndex-1].Term != leader.PrevLogTerm) {
		response.Term = node.currentTerm
		response.Success = false
		return nil
	}
	// If an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follows it. Append any new entires not already in the log

	// leader.logs
	// SET 10, Term 2 Index 1
	// SET 15, Term 2 Index 2
	// SET 20, Term 2 Index 3
	// leader.PrevLogIndex = 3

	// leader.Entries
	// SET 25, Term 2

	// node.logs
	// SET 10, Term 2 Index 1
	// SET 15, Term 2 Index 2
	// SET 20, Term 2 Index 3

	for entry_index, entry := range leader.Entries {
		// correct Terms
		slot := leader.PrevLogIndex + entry_index
		if slot < len(node.logs) {
			if node.logs[slot].Term == entry.Term {
				continue // already have it
			}
			// terms do not match
			node.logs = node.logs[:slot]
		}
		// once all terms are corrected, add entries
		node.logs = append(node.logs, leader.Entries[entry_index:]...)
		break
	}
	response.Term = node.currentTerm
	response.Success = true

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leader.LeaderCommit > node.commitIndex {
		node.commitIndex = min(leader.LeaderCommit, leader.PrevLogIndex+len(leader.Entries))
		node.applyCommitted()
	}

	debugf("Node %d sent heartbeat to Node %d", leader.LeaderId, node.id)
	node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
	return nil
}

func (node *Node) appendEntriesArgsFor(node_id int) *AppendEntriesArgs {
	prevLogIndex := node.nextIndex[node_id] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = node.logs[prevLogIndex-1].Term
	}
	entries := append([]LogEntry(nil), node.logs[prevLogIndex:]...)

	return &AppendEntriesArgs{
		Term:         node.currentTerm,
		LeaderId:     node.id,
		PrevLogIndex: prevLogIndex, // custom per follower
		PrevLogTerm:  prevLogTerm,  // custom per follower
		Entries:      entries,      // custom per follower
		LeaderCommit: node.commitIndex,
	}
}

func (node *Node) replicate(args *SetArgs) error {
	if node.role != Leader {
		debugf("[Stale Node] Node %d failed replicating: no longer a leader", node.id)
		return nil
	}

	if args.Command == "" {
		debugf("Node %d heartbeat", node.id)
	} else {
		debugf("Node %d is replicating command: %s", node.id, args.Command)
		new_log := LogEntry{Term: node.currentTerm, Command: args.Command}
		node.logs = append(node.logs, new_log)
	}

	results := make(chan appendEntriesResult, len(node.addresses))
	var startWg sync.WaitGroup

	followersAppendEntriesArgs := make([]*AppendEntriesArgs, len(node.addresses))
	for i := range followersAppendEntriesArgs {
		followersAppendEntriesArgs[i] = node.appendEntriesArgsFor(i)
	}

	for i := 0; i < len(node.addresses); i++ {
		startWg.Add(1)

		go func(id int) {
			defer startWg.Done()

			client, err := rpc.Dial("tcp", node.addresses[i])
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var response AppendEntriesResponse
			if err := client.Call("Node.AppendEntries", followersAppendEntriesArgs[i], &response); err != nil {
				log.Fatal("raft error:", err)
			}

			results <- appendEntriesResult{replicateResult: response, err: err, follower_idx: i}
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

		// get the initial arguments for the follower
		followersArgs := followersAppendEntriesArgs[res.follower_idx]

		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if res.replicateResult.Term > node.currentTerm {
			debugf("[Stale Leader] Node %d is converted to a 'Follower'", node.id)
			node.currentTerm = res.replicateResult.Term
			node.role = Follower
			node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
			// no longer leader: ignore remaining responses and don't commit anything
			return nil
		} else if res.replicateResult.Success {
			node.matchIndex[res.follower_idx] = followersArgs.PrevLogIndex + len(followersArgs.Entries)
			node.nextIndex[res.follower_idx] = node.matchIndex[res.follower_idx] + 1
		} else {
			// retry
			node.nextIndex[res.follower_idx] = max(1, node.nextIndex[res.follower_idx]-1)
		}

	}

	node.advanceCommitIndex()
	return nil
}

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (node *Node) applyCommitted() {
	for node.lastApplied < node.commitIndex {
		node.lastApplied += 1

		// log indices start at 1, node.logs starts at 0
		command, value, err := node.parseCommand(node.lastApplied - 1)
		if err != nil {
			debugf("Node %d failed to apply log %d: %v", node.id, node.lastApplied, err)
			continue
		}
		if command == "SET" {
			node.data = value
		}
	}
}

// index is the position in node.logs (0-based)
func (node *Node) parseCommand(index int) (string, int, error) {
	parts := strings.Fields(node.logs[index].Command)
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("invalid input format")
	}

	command := parts[0]

	value, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("failed to convert value to int: %w", err)
	}

	return command, value, nil
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
func (node *Node) advanceCommitIndex() {
	// all peers + self
	population := len(node.addresses) + 1
	majority := population/2 + 1

	for n := len(node.logs); n > node.commitIndex; n-- {
		// only count replicas for entries from the current term (§5.4.2);
		// entries below this have even older terms, so stop looking
		if node.logs[n-1].Term != node.currentTerm {
			break
		}

		replication_count := 1 // leader itself
		for _, match := range node.matchIndex {
			if match >= n {
				replication_count += 1
			}
		}

		if replication_count >= majority {
			node.commitIndex = n // commits every entry <= n as well
			node.applyCommitted()
			debugf("Node %d committed up to index %d", node.id, node.commitIndex)
			break
		}
	}
}

func (node *Node) Run() {
	debugf("Node %d running\n", node.id)

	node.condition.mu.Lock()
	pauseCh := node.condition.pauseCh
	node.condition.state = Running
	node.condition.mu.Unlock()

	for {
		// when heartbeat timer runs out, it always resets the electionTimer
		if node.role != Leader {
			node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
		}

		if node.role == Leader {
			node.heartbeatTimer.Reset(time.Duration(node.heartbeatTimeout) * time.Millisecond)
		}

		select {
		case <-node.electionTimer.C:
			debugf("Node %d elapsed electionTimeout: %d\n", node.id, node.electionTimeout)
			node.startElection()
		case <-node.heartbeatTimer.C:
			// debugf("Node %d heartbeat timer: %d\n", node.id, node.heartbeatTimeout)
			node.replicate(&SetArgs{Command: ""})
		case <-pauseCh:
			debugf("Node %d is paused\n", node.id)
			return
		}
	}
}

func (node *Node) Pause() {
	node.condition.mu.Lock()
	defer node.condition.mu.Unlock()
	if node.condition.state == Paused {
		return
	}
	debugf("Node %d pausing", node.id)
	node.condition.state = Paused
	close(node.condition.pauseCh)
}

func (node *Node) Resume() {
	node.condition.mu.Lock()
	defer node.condition.mu.Unlock()
	if node.condition.state != Paused {
		return
	}
	debugf("Node %d resuming", node.id)
	node.condition.state = Running
	node.condition.pauseCh = make(chan struct{})
	go node.Run()
}
