package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/evansuslovich/pg-ha-orchestrator/raft"
)

func main() {
	raft := new(raft.Raft)
	// Register: server registers an object, making it visible as a service with the name of the type of the object
	rpc.Register(raft)
	rpc.HandleHTTP()
	// https://pkg.go.dev/net#Listen
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	// https://pkg.go.dev/net/http#Serve
	// Serve accepts incoming HTTP connections on the listener l, creating a new service goroutine for each.
	// The service goroutines read requests and then call handler to reply to them
	log.Println("serving on :1234")
	log.Fatal(http.Serve(l, nil))
}
