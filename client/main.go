package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"regexp"
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
		case "speed":
			if len(inputs) < 2 {
				fmt.Println("usage: speed {snail, slow, medium, fast}")
				continue
			}

			speed := inputs[1]
			re := regexp.MustCompile(`snail|slow|medium|fast`)
			if !re.MatchString(speed) {
				fmt.Println("usage: speed {snail, slow, medium, fast}")
				continue
			}

			args := &raft.SpeedArgs{Speed: speed}
			var response raft.SpeedResponse
			if err := client.Call("Raft.Speed", args, &response); err != nil {
				log.Fatal(err)
			}

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
			fmt.Println(response.Node)

		case "set", "add", "minus":
			if len(inputs) < 2 {
				fmt.Printf("usage: %s <value>\n", inputs[0])
				continue
			}

			_, err := strconv.Atoi(inputs[1])

			if err != nil {
				fmt.Println("invalid node value:", err)
				continue
			}

			command := entryOps[inputs[0]] + " " + inputs[1]
			args := &raft.EntriesArgs{Command: []string{command}}
			var response raft.EntriesResponse
			if err := client.Call("Raft.Entries", args, &response); err != nil {
				log.Fatal(err)
			}

		case "add_entries":
			if len(inputs) < 2 {
				fmt.Println("usage: add_entries <op> <value>, <op> <value>, ...   (op: set, add, minus)")
				continue
			}

			commands, err := parseEntries(strings.Join(inputs[1:], " "))
			if err != nil {
				fmt.Println(err)
				continue
			}

			args := &raft.EntriesArgs{Command: commands}
			var response raft.EntriesResponse
			if err := client.Call("Raft.Set", args, &response); err != nil {
				log.Fatal(err)
			}

		case "run":
			args := &raft.RunArgs{}
			var response raft.RunResponse
			if err := client.Call("Raft.Run", args, &response); err != nil {
				log.Fatal(err)
			}

		case "pause":
			if len(inputs) < 2 {
				fmt.Println("usage: pause <id>")
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
			if err := client.Call("Raft.Pause", args, &response); err != nil {
				log.Fatal(err)
			}
			fmt.Println(response.Value)

		case "freeze":
			var response raft.Response
			if err := client.Call("Raft.Freeze", &raft.EmptyArgs{}, &response); err != nil {
				log.Fatal(err)
			}
			fmt.Println(response.Value)

		case "resume":
			// no id: resume everything paused by freeze
			if len(inputs) < 2 {
				var response raft.Response
				if err := client.Call("Raft.Unfreeze", &raft.EmptyArgs{}, &response); err != nil {
					log.Fatal(err)
				}
				fmt.Println(response.Value)
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
			if err := client.Call("Raft.Resume", args, &response); err != nil {
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
			fmt.Println("pause <id>        pause node")
			fmt.Println("resume <id>       resume node")
			fmt.Println("freeze            pause all running nodes")
			fmt.Println("resume            resume nodes paused by freeze")
			fmt.Println("set <value>       set leader to value")
			fmt.Println("add <value>       add value to leader's data")
			fmt.Println("minus <value>     subtract value from leader's data")
			fmt.Println("add_entries ...   append several entries, e.g. add_entries set 10, add 30, minus 40")
			fmt.Println("speed <value>     set node speed speed {snail, slow, medium, fast}")
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

// maps client-facing operations to the commands applyCommitted understands
var entryOps = map[string]string{
	"set":      "SET",
	"add":      "ADD",
	"minus":    "SUBTRACT",
	"subtract": "SUBTRACT",
}

// parseEntries turns "set 10, add 30, minus 40" into ["SET 10", "ADD 30", "SUBTRACT 40"]
func parseEntries(input string) ([]string, error) {
	var commands []string
	for _, entry := range strings.Split(input, ",") {
		parts := strings.Fields(entry)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid entry %q: expected <op> <value>", strings.TrimSpace(entry))
		}

		op, ok := entryOps[strings.ToLower(parts[0])]
		if !ok {
			return nil, fmt.Errorf("unknown operation %q: expected set, add or minus", parts[0])
		}

		if _, err := strconv.Atoi(parts[1]); err != nil {
			return nil, fmt.Errorf("invalid value %q: %v", parts[1], err)
		}

		commands = append(commands, op+" "+parts[1])
	}
	return commands, nil
}
