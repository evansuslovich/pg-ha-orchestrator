package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"strconv"
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

type Node struct {
	id            int
	name          string
	timeoutLength int // milliseconds
	role          Role

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []string

	// volatile state on all servers
	commitIndex int //index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	state      map[string]int

	timer *time.Timer
	nodes []Node
}

type EmptyArgs struct{}

type ViewResponse struct {
	Node string
}

func (node *Node) View(args EmptyArgs, response *ViewResponse) error {
	response.Node = fmt.Sprintf("name: %s\ntimeout: %d ms\nrole: %s\n\n", node.name, node.timeoutLength, node.role)
	return nil
}

type NodeArgs struct {
	Id            int
	Name          string
	TimeoutLength int
}

func StartServer(args *NodeArgs) *Node {
	node := &Node{
		id:            args.Id,
		name:          args.Name,
		role:          Follower,
		timeoutLength: 100 + rand.N(args.TimeoutLength),
		state:         make(map[string]int),
		timer:         time.NewTimer(time.Millisecond),
	}
	server := rpc.NewServer()
	server.Register(node)

	// https://pkg.go.dev/net#Listen
	address := ":" + strconv.Itoa(1234+args.Id)
	l, err := net.Listen("tcp", address)
	if err != nil {
		errors.New("failed to spin up")
	}

	log.Printf("serving on %s", address)
	// log.Fatal(http.Serve(l, nil))
	go server.Accept(l)
	return node
}

// func (server *Node) Run(wg *sync.WaitGroup) {
// 	defer wg.Done()
//
// 	fmt.Printf("%s starting \n", server.name)
//
// 	start := time.Now()
// 	server.timer.Reset(time.Duration(server.timeoutLength) * time.Millisecond)
// 	// <- operator is the chanel operator used to send or receive values through a concurreny channel
// 	<-server.timer.C
//
// 	elapsed := time.Since(start)
// 	fmt.Printf("%s elapsed for %s timeoutLength: %d\n", elapsed, server.name, server.timeoutLength)
// 	server.beginElection()
// }
