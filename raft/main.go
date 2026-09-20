package raft

import (
	"errors"
	"log"
	"net/rpc"
	"slices"
	"strconv"
)

type Raft struct {
	Count int
	Nodes []*Node
}

type Args struct {
	Count int
}

type RaftResponse struct {
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

func (raft *Raft) Run(args *Args, response *RaftResponse) error {

	for i := 0; i < args.Count; i++ {
		nodeArgs := &NodeArgs{Id: i + 1, Name: NumberToLetter(i + 1), TimeoutLength: 50}
		node, err := StartServer(nodeArgs)
		if err != nil {
			return errors.New("encountered error starting node " + strconv.Itoa(i+1) + ": " + err.Error())
		}
		raft.Count += 1
		raft.Nodes = append(raft.Nodes, node)
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
