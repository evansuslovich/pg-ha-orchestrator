package raft

import (
	"errors"
	"fmt"
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

type NodeState int

const (
	Running NodeState = iota // 0
	Paused                   // 1
	Fresh                    // 2
)

func (s NodeState) String() string {
	return [...]string{"Running", "Paused", "Fresh"}[s]
}

type SpeedState int

const (
	Snail  SpeedState = iota // 0
	Slow                     // 1
	Medium                   // 2
	Fast                     // 3
)

func (s SpeedState) String() string {
	return [...]string{"Snail", "Slow", "Medium", "Fast"}[s]
}

var speedMap = map[string][2]int{
	// electionTimeout, heartbeatTimer
	"snail":  {8000, 4000},
	"slow":   {5000, 2500},
	"medium": {3000, 1500},
	"fast":   {1000, 500},
}

type Condition struct {
	mu      sync.Mutex
	state   NodeState
	speed   SpeedState
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
speed:             %s
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
		node.address, node.role, node.condition.state, node.condition.speed, node.electionTimeout, node.heartbeatTimeout, node.addresses,
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
		electionTimeout:  1000 + speedMap["slow"][1] + rand.N(args.ElectionTimeout),
		heartbeatTimeout: 999 + speedMap["slow"][1],
		role:             Follower,
		condition:        Condition{state: Fresh, speed: Slow, pauseCh: make(chan struct{})},
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

	l, err := net.Listen("tcp", node.address)
	if err != nil {
		return nil, errors.New("failed to spin up: " + err.Error())
	}

	debugf("serving on %s", node.address)
	go server.Accept(l)
	return node, nil
}

func (node *Node) LastLogTerm() int {
	lastLogTerm := 0
	if len(node.logs) > 0 {
		lastLogTerm = node.logs[len(node.logs)-1].Term
	}
	return lastLogTerm
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
			node.replicate(&EntriesArgs{})
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

func (node *Node) SetSpeed(speed string) error {
	node.condition.mu.Lock()
	defer node.condition.mu.Unlock()
	debugf("Node %d setting speed to '%s'", node.id, speed)
	switch speed {
	case "snail":
		node.condition.speed = Snail
	case "slow":
		node.condition.speed = Slow
	case "medium":
		node.condition.speed = Medium
	case "fast":
		node.condition.speed = Fast
	default:
		debugf("Speed: %s not found", speed)
		return nil
	}

	node.heartbeatTimeout = 999 + speedMap[speed][1]
	node.electionTimeout = node.heartbeatTimeout + rand.N(speedMap[speed][0])
	return nil
}
