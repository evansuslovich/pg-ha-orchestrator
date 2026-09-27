package raft

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of previous log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type appendEntriesResult struct {
	replicateResult AppendEntriesResponse
	err             error
	follower_idx    int
}

func (node *Node) AppendEntries(leader *AppendEntriesArgs, response *AppendEntriesResponse) error {
	if node.condition.state == Paused {
		debugf("Node %d is paused", node.id)
		return nil
	}

	// stale leader AppendEntries
	if leader.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.Success = false
		debugf("[Stale Leader] Node %d term %d is older than Node %d term: %d", leader.LeaderId, leader.Term, node.id, node.currentTerm)
		return nil
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	// stale Node
	if leader.Term > node.currentTerm {
		debugf("[Stale Node] Node %d term: %d is older than Node %d term %d", node.id, node.currentTerm, leader.LeaderId, leader.Term)
		node.currentTerm = leader.Term
		node.role = Follower
		node.votedFor = 0 // represents null
		node.heartbeatTimer.Stop()
	}

	// if the current node is in a candidate position and we receive an AppendEntries from a new leader
	if node.role == Candidate {
		debugf("[Stale Node] Node %d is in a Candidate position", node.id)
		node.role = Follower
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose terms matches prevLogTerm
	if leader.PrevLogIndex > len(node.logs) ||
		(leader.PrevLogIndex > 0 && node.logs[leader.PrevLogIndex-1].Term != leader.PrevLogTerm) {
		response.Term = node.currentTerm
		response.Success = false
		return nil
	}

	// add entry to logs
	for entry_index, entry := range leader.Entries {
		// correct Terms
		slot := leader.PrevLogIndex + entry_index
		if slot < len(node.logs) {
			if node.logs[slot].Term == entry.Term {
				continue // already have it
			}
			// terms do not match
			node.logs = node.logs[:slot]
		}
		// once all terms are corrected, add entries
		debugf("Node %d adding logs: %v", node.id, leader.Entries[entry_index:])
		node.logs = append(node.logs, leader.Entries[entry_index:]...)
		break
	}
	response.Term = node.currentTerm
	response.Success = true

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leader.LeaderCommit > node.commitIndex {
		node.commitIndex = min(leader.LeaderCommit, leader.PrevLogIndex+len(leader.Entries))
		debugf("Node %d incrementing commitIndex: %d", node.id, node.commitIndex)
		node.applyCommitted()
	}

	node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
	return nil
}

func (node *Node) appendEntriesArgsFor(node_id int) *AppendEntriesArgs {
	prevLogIndex := node.nextIndex[node_id] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = node.logs[prevLogIndex-1].Term
	}
	entries := append([]LogEntry(nil), node.logs[prevLogIndex:]...)

	return &AppendEntriesArgs{
		Term:         node.currentTerm,
		LeaderId:     node.id,
		PrevLogIndex: prevLogIndex, // custom per follower
		PrevLogTerm:  prevLogTerm,  // custom per follower
		Entries:      entries,      // custom per follower
		LeaderCommit: node.commitIndex,
	}
}

func (node *Node) replicate(args *EntriesArgs) error {
	if node.role != Leader {
		debugf("[Stale Node] Node %d failed replicating: no longer a leader", node.id)
		return nil
	}

	if len(args.Command) == 0 {
		debugf("Node %d heartbeat", node.id)
	} else {
		debugf("Node %d is replicating commands: %q", node.id, args.Command)
		for _, command := range args.Command {
			node.logs = append(node.logs, LogEntry{Term: node.currentTerm, Command: command})
		}
	}

	results := make(chan appendEntriesResult, len(node.addresses))
	var startWg sync.WaitGroup

	followersAppendEntriesArgs := make([]*AppendEntriesArgs, len(node.addresses))
	for i := range followersAppendEntriesArgs {
		followersAppendEntriesArgs[i] = node.appendEntriesArgsFor(i)
	}

	for i := 0; i < len(node.addresses); i++ {
		startWg.Add(1)

		go func(id int) {
			defer startWg.Done()

			client, err := rpc.Dial("tcp", node.addresses[i])
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var response AppendEntriesResponse
			if err := client.Call("Node.AppendEntries", followersAppendEntriesArgs[i], &response); err != nil {
				log.Fatal("raft error:", err)
			}

			results <- appendEntriesResult{replicateResult: response, err: err, follower_idx: i}
		}(i + 1)
	}

	go func() {
		startWg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return errors.New("encountered an error in replicating: " + res.err.Error())
		}
		// get the initial arguments for the follower
		followersArgs := followersAppendEntriesArgs[res.follower_idx]

		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if res.replicateResult.Term > node.currentTerm {
			debugf("[Stale Leader] Node %d is converted to a 'Follower'", node.id)
			node.currentTerm = res.replicateResult.Term
			node.role = Follower
			node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
			// no longer leader: ignore remaining responses and don't commit anything
			return nil
		} else if res.replicateResult.Success {
			node.matchIndex[res.follower_idx] = followersArgs.PrevLogIndex + len(followersArgs.Entries)
			node.nextIndex[res.follower_idx] = node.matchIndex[res.follower_idx] + 1
		} else {
			// retry
			node.nextIndex[res.follower_idx] = max(1, node.nextIndex[res.follower_idx]-1)
		}

	}

	node.advanceCommitIndex()
	return nil
}

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (node *Node) applyCommitted() {
	for node.lastApplied < node.commitIndex {
		node.lastApplied += 1

		// log indices start at 1, node.logs starts at 0
		command, value, err := node.parseCommand(node.lastApplied - 1)
		if err != nil {
			debugf("Node %d failed to apply log %d: %v", node.id, node.lastApplied, err)
			continue
		}
		debugf("Node %d applying %s %d", node.id, command, value)
		if command == "SET" {
			node.data = value
		}
		if command == "ADD" {
			node.data += value
		}
		if command == "SUBTRACT" {
			node.data -= value
		}
	}
}

// index is the position in node.logs (0-based)
func (node *Node) parseCommand(index int) (string, int, error) {
	parts := strings.Fields(node.logs[index].Command)
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("invalid input format")
	}

	command := parts[0]

	value, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("failed to convert value to int: %w", err)
	}

	return command, value, nil
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
func (leader *Node) advanceCommitIndex() {
	// all peers + self
	population := len(leader.addresses) + 1
	majority := population/2 + 1

	for n := len(leader.logs); n > leader.commitIndex; n-- {
		// only count replicas for entries from the current term (§5.4.2);
		// entries below this have even older terms, so stop looking
		if leader.logs[n-1].Term != leader.currentTerm {
			break
		}

		replication_count := 1 // leader itself
		for _, match := range leader.matchIndex {
			if match >= n {
				replication_count += 1
			}
		}

		if replication_count >= majority {
			leader.commitIndex = n // commits every entry <= n as well
			leader.applyCommitted()
			debugf("Node %d committed up to index %d", leader.id, leader.commitIndex)
			break
		}
	}
}
