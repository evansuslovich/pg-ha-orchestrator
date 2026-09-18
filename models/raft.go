package models

import (
	"fmt"
	"sync"
)

type Raft struct {
	Count   int
	Servers []*Server
}

func NewRaft() *Raft {
	return &Raft{
		Count: 5,
		Servers: []*Server{
			NewServer(1, "Server A", 0),
			NewServer(2, "Server B", 0),
			NewServer(3, "Server C", 0),
			NewServer(4, "Server D", 0),
			NewServer(5, "Server E", 0),
		},
	}
}

func (raft *Raft) Elect() {

}

func (raft *Raft) View() {
	fmt.Printf("ServerCount: %d\n", raft.Count)

	fmt.Printf("\n")
	fmt.Printf("\n")

	for _, server := range raft.Servers {
		server.View()
	}
}

func (raft *Raft) Run() {

	var wg sync.WaitGroup

	for i := range raft.Servers {
		wg.Add(1)
		go raft.Servers[i].Run(&wg)
	}

	wg.Wait()
	fmt.Println("All finished")
}
