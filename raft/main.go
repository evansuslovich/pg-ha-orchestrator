package raft

import (
	"log"
	"net/rpc"
)

type Raft struct {
	Count int
	Nodes []*Node
}

type Args struct{}

type RaftResponse struct {
	Count int
}
type Response struct {
	Value string
}

func (raft *Raft) Run(args *Args, response *RaftResponse) error {
	nodeArgs := &NodeArgs{Id: 1, Name: "Node A", TimeoutLength: 50}
	node := StartServer(nodeArgs)

	raft.Count += 1
	raft.Nodes = append(raft.Nodes, node)

	nodeArgs = &NodeArgs{Id: 2, Name: "Node B", TimeoutLength: 50}
	node = StartServer(nodeArgs)

	raft.Count += 1
	raft.Nodes = append(raft.Nodes, node)

	response.Count = raft.Count
	return nil
}

func (raft *Raft) View(args *Args, response *Response) error {
	var viewResponse ViewResponse

	client, err := rpc.Dial("tcp", "localhost:1235")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	if err := client.Call("Node.View", EmptyArgs{}, &viewResponse); err != nil {
		log.Fatal("raft error:", err)
	}

	response.Value = viewResponse.Node
	return nil
}
