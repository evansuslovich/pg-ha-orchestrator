package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"rpc/arith"
)

func main() {
	a := new(arith.Arith)
	// Register: server registers an object, making it visible as a service with the name of the type of the object
	rpc.Register(a)
	rpc.HandleHTTP()
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
