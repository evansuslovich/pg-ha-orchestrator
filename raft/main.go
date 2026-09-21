package raft

import (
	"errors"
	"log"
	"net/rpc"
	"slices"
	"strconv"
	"sync"
)

type Raft struct {
	Count int
	Nodes []*Node
}

type Args struct {
	Count int
}

type Response struct {
	Value string
}

func NumberToLetter(n int) string {
	if n < 1 || n > 26 {
		return "Invalid (Out of A-Z range)"
	}
	// 'A' is 65 in ASCII. If n=1: 65 + 1 - 1 = 65 ('A')
	return string(rune('A' + n - 1))
}

type RunArgs struct{}

type RunResponse struct{}

func (raft *Raft) Run(args *Args, response *RunResponse) error {
	for _, node := range raft.Nodes {
		go node.Run()
	}
	return nil
}

type buildResult struct {
	node *Node
	err  error
}
type BuildResponse struct {
	Count int
}

func (raft *Raft) Build(args *Args, response *BuildResponse) error {
	results := make(chan buildResult, args.Count)
	var startWg sync.WaitGroup

	addresses := make([]string, args.Count)
	for i := 0; i < args.Count; i++ {
		addresses[i] = ":" + strconv.Itoa(1234+i+1)
	}

	for i := 0; i < args.Count; i++ {
		startWg.Add(1)
		go func(id int) {
			defer startWg.Done()
			node, err := StartServer(&NodeArgs{Id: id, Name: NumberToLetter(id), Addresses: addresses, TimeoutLength: 5000})

			results <- buildResult{node: node, err: err}
		}(i + 1)
	}

	go func() {
		startWg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return errors.New("encountered error starting a node: " + res.err.Error())
		}

		raft.Count += 1
		raft.Nodes = append(raft.Nodes, res.node)
	}

	response.Count = raft.Count
	return nil
}

type ViewArgs struct {
	Id int
}

func (raft *Raft) View(args *ViewArgs, response *ViewResponse) error {
	// find corresponding Node
	index := slices.IndexFunc(raft.Nodes, func(n *Node) bool {
		return n.id == args.Id
	})

	if index == -1 {
		return errors.New("Node id: " + strconv.Itoa(args.Id) + " not found")
	}

	selected_node := raft.Nodes[index]

	client, err := rpc.Dial("tcp", selected_node.address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	if err := client.Call("Node.View", EmptyArgs{}, response); err != nil {
		log.Fatal("raft error:", err)
	}

	return nil
}

func (raft *Raft) Stop(args *ViewArgs, response *Response) error {
	index := slices.IndexFunc(raft.Nodes, func(n *Node) bool {
		return n.id == args.Id
	})
	if index == -1 {
		return errors.New("Node id: " + strconv.Itoa(args.Id) + " not found")
	}

	raft.Nodes[index].Stop()
	response.Value = "stopped node " + strconv.Itoa(args.Id)
	return nil
}
