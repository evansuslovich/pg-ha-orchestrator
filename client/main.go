package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"

	"github.com/evansuslovich/pg-ha-orchestrator/raft"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	for {
		fmt.Print("Enter a command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		inputs := strings.Fields(input)

		// Switch case command handling
		switch inputs[0] {
		case "select":
			if len(inputs) < 2 {
				fmt.Println("usage: select <id>")
				continue
			}

			id, err := strconv.Atoi(inputs[1])
			if id < 1 {
				fmt.Println("Non-positive id:", strconv.Itoa(id))
				continue
			}
			if err != nil {
				fmt.Println("invalid node id:", err)
				continue
			}

			args := &raft.ViewArgs{Id: id}
			var response raft.ViewResponse
			if err := client.Call("Raft.View", args, &response); err != nil {
				log.Fatal(err)
			}
			fmt.Printf(response.Node)

		case "run":
			args := &raft.RunArgs{}
			var response raft.RunResponse
			if err := client.Call("Raft.Run", args, &response); err != nil {
				log.Fatal(err)
			}
		case "stop":
			if len(inputs) < 2 {
				fmt.Println("usage: stop <id>")
				continue
			}

			id, err := strconv.Atoi(inputs[1])
			if id < 1 {
				fmt.Println("Non-positive id:", strconv.Itoa(id))
				continue
			}
			if err != nil {
				fmt.Println("invalid node id:", err)
				continue
			}

			args := &raft.ViewArgs{Id: id}
			var response raft.Response
			if err := client.Call("Raft.Stop", args, &response); err != nil {
				log.Fatal(err)
			}
			fmt.Println(response.Value)

		case "build":
			if len(inputs) < 2 {
				fmt.Println("usage: build <count>")
				continue
			}

			count_of_nodes, err := strconv.Atoi(inputs[1])
			if err != nil {
				fmt.Println("invalid node count:", err)
				continue
			}
			if count_of_nodes > 5 {
				log.Fatal("Cannot handle more than 5 nodes")
			}

			args := &raft.Args{Count: count_of_nodes}
			var response raft.BuildResponse
			if err := client.Call("Raft.Build", args, &response); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Count of nodes running: %d\n", response.Count)

		case "help":
			fmt.Println("build <count>     build <n> number nodes")
			fmt.Println("select <id>       view state of a node")
			fmt.Println("stop <id>         pause node")
			fmt.Println("run               run nodes")
			fmt.Println("exit              exit application")
		case "exit":
			fmt.Println("Exiting program. Goodbye!")
			return
		case ":q":
			fmt.Println("Exiting program. Goodbye!")
			return
		default:
			fmt.Println("Unknown command. Try again.")
		}
	}
}
