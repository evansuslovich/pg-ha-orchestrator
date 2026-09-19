package main

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/evansuslovich/pg-ha-orchestrator/raft"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := &raft.Args{}
	var response raft.RaftResponse
	if err := client.Call("Raft.Run", args, &response); err != nil {
		log.Fatal("raft error:", err)
	}
	fmt.Printf("Count of nodes: %d\n", response.Count)
}
