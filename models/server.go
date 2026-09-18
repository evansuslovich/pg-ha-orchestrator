package models

import (
	"fmt"
	"math/rand/v2"
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

type Server struct {
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
}

func NewServer(id int, name string, timeoutLength int) *Server {
	if name == "" {
		name = "Server"
	}

	//  [100, 150)
	timeoutLength = 100 + rand.N(50)

	return &Server{
		id:            id,
		name:          name,
		role:          Follower,
		timeoutLength: timeoutLength,
		state:         make(map[string]int),
		timer:         time.NewTimer(time.Millisecond),
	}
}

func (server *Server) View() {
	fmt.Printf("name: %s\ntimeout: %d ms\nrole: %s\n\n", server.name, server.timeoutLength, server.role)
}

// If a follower receives no communication over a period of time (electionTimeout) then it assumes there is no viable leader and begins an election
func (server *Server) beginElection() {
	server.currentTerm += 1
	server.role = Candidate
	server.votedFor = server.id
}

func (server *Server) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("%s starting \n", server.name)

	start := time.Now()
	server.timer.Reset(time.Duration(server.timeoutLength) * time.Millisecond)
	// <- operator is the chanel operator used to send or receive values through a concurreny channel
	<-server.timer.C
	elapsed := time.Since(start)
	fmt.Printf("%s elapsed for %s timeoutLength: %d\n", elapsed, server.name, server.timeoutLength)
}
