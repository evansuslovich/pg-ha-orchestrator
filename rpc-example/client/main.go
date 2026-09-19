package main

import (
	"fmt"
	"log"
	"net/rpc"
	"rpc/arith"
)

func main() {

	// A client wishing to use the service establihses a connection and then invokes NewClient on the connection. The convenience function Dial (DialHTTP) performs both steps for a raw network connetion (an HTTP connection)
	// Two methods Call and Go, that specify the service and method to call, a pointer containing the arguments, and a pointer to receive the result parameters

	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// - schematically like: func (t *T) MethodName(argType T1, replyType *T2) error
	// - T1 and T2 can be marshaled by encoding/gov
	// - argType: the arguments provided by the caller
	// - the result parameters to be returned to the caller
	args := &arith.Args{A: 7, B: 8}
	var reply int
	if err := client.Call("Arith.Multiply", args, &reply); err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d*%d=%d\n", args.A, args.B, reply)

	var quo arith.Quotient
	if err := client.Call("Arith.Divide", args, &quo); err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d/%d=%d rem %d\n", args.A, args.B, quo.Quo, quo.Rem)
}
