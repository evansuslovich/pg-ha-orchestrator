package raft

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

type Raft struct {
	Count  int
	Nodes  []*Node
	frozen []*Node // nodes paused by Freeze, restored by Unfreeze
}

type BuildArgs struct {
	Count int
}

type Response struct {
	Value string
}

type RunArgs struct{}

type RunResponse struct{}

func (raft *Raft) Run(args *EmptyArgs, response *RunResponse) error {
	for _, node := range raft.Nodes {
		go node.Run()
	}
	return nil
}

type EmptyResponse struct{}

func (raft *Raft) Build(args *BuildArgs, response *EmptyResponse) error {
	raft.Count = args.Count
	addresses := make([]string, raft.Count)
	for i := 0; i < raft.Count; i++ {
		addresses[i] = ":" + strconv.Itoa(1234+i+1)
	}

	for i := 0; i < raft.Count; i++ {
		node, err := StartServer(&NodeArgs{Id: i + 1, Addresses: addresses, ElectionTimeout: 5000})

		if err != nil {
			return errors.New("Encountered error starting a node: " + err.Error())
		}

		raft.Nodes = append(raft.Nodes, node)
	}

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

func (raft *Raft) Pause(args *ViewArgs, response *Response) error {
	index := slices.IndexFunc(raft.Nodes, func(n *Node) bool {
		return n.id == args.Id
	})
	if index == -1 {
		return errors.New("Node id: " + strconv.Itoa(args.Id) + " not found")
	}

	raft.Nodes[index].Pause()
	response.Value = "paused node " + strconv.Itoa(args.Id)
	return nil
}

func (raft *Raft) Resume(args *ViewArgs, response *Response) error {
	index := slices.IndexFunc(raft.Nodes, func(n *Node) bool {
		return n.id == args.Id
	})
	if index == -1 {
		return errors.New("Node id: " + strconv.Itoa(args.Id) + " not found")
	}

	raft.Nodes[index].Resume()
	response.Value = "Resume node " + strconv.Itoa(args.Id)
	return nil
}

// Freeze pauses every running node so the cluster can be inspected.
// Nodes that were already paused (or never started) are left alone, so
// Unfreeze restores the cluster to exactly how it was.
func (raft *Raft) Freeze(args *EmptyArgs, response *Response) error {
	if raft.frozen != nil {
		response.Value = "cluster already frozen"
		return nil
	}

	raft.frozen = []*Node{}
	for _, node := range raft.Nodes {
		node.condition.mu.Lock()
		running := node.condition.state == Running
		node.condition.mu.Unlock()

		if running {
			node.Pause()
			raft.frozen = append(raft.frozen, node)
		}
	}

	response.Value = "froze " + strconv.Itoa(len(raft.frozen)) + " node(s)"
	return nil
}

// Live renders every node side by side: one row per field, one column per node.
func (raft *Raft) Live(args *EmptyArgs, response *ViewResponse) error {
	labels := []string{"id", "role", "condition", "currentTerm", "votedFor", "commitIndex", "lastApplied", "nextIndex", "data"}

	// rows x columns
	output := make([][]string, len(labels))
	for elementIndex := range output {
		output[elementIndex] = make([]string, len(raft.Nodes))
	}

	for nodeIndex, node := range raft.Nodes {
		node.condition.mu.Lock()
		state := node.condition.state
		node.condition.mu.Unlock()

		nodeElements := []any{node.id, node.role, state, node.currentTerm, node.votedFor, node.commitIndex, node.lastApplied, node.nextIndex, node.data}

		for elementIndex, element := range nodeElements {
			output[elementIndex][nodeIndex] = fmt.Sprint(element)
		}
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 3, ' ', 0)
	for elementIndex, row := range output {
		fmt.Fprintln(w, labels[elementIndex]+"\t"+strings.Join(row, "\t"))
	}
	w.Flush()

	response.Content = buf.String()
	return nil
}

// Unfreeze resumes only the nodes that Freeze paused.
func (raft *Raft) Unfreeze(args *EmptyArgs, response *Response) error {
	if raft.frozen == nil {
		response.Value = "cluster is not frozen"
		return nil
	}

	for _, node := range raft.frozen {
		node.Resume()
	}

	response.Value = "unfroze " + strconv.Itoa(len(raft.frozen)) + " node(s)"
	raft.frozen = nil
	return nil
}

type EntriesArgs struct {
	Command []string // empty for a heartbeat
}

type EntriesResponse struct{}

func (raft *Raft) Entries(args *EntriesArgs, response *EntriesResponse) error {
	index := slices.IndexFunc(raft.Nodes, func(n *Node) bool {
		return n.role == Leader && n.condition.state == Running
	})

	if index == -1 {
		return errors.New("Leader not found")
	}

	raft.Nodes[index].replicate(args)
	return nil
}

type SpeedArgs struct {
	Speed string
}

type SpeedResponse struct{}

func (raft *Raft) Speed(args *SpeedArgs, response *SpeedResponse) error {
	for _, node := range raft.Nodes {
		go node.SetSpeed(args.Speed)
	}
	return nil
}
