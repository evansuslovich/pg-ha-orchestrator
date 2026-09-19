package main

import (
	"log"
	"net/rpc"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// args := &arith.Args{A: 7, B: 8}
	// var reply int
	// if err := client.Call("Arith.Multiply", args, &reply); err != nil {
	// 	log.Fatal("arith error:", err)
	// }
	// fmt.Printf("Arith: %d*%d=%d\n", args.A, args.B, reply)
}
