package raft

import (
	"errors"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteResponse struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (node *Node) RequestVote(candidate *RequestVoteArgs, response *RequestVoteResponse) error {
	if node.condition.state == Paused {
		debugf("Node %d is paused", node.id)
		return nil
	}

	// reject if candidate's term is old
	if candidate.Term < node.currentTerm {
		response.Term = node.currentTerm
		response.VoteGranted = false

		debugf("Node %d is rejecting Candidate %d due to stale terms", node.id, candidate.CandidateId)
		debugf("Node %d's term: %d > Candidate %d term: %d", node.id, node.currentTerm, candidate.CandidateId, candidate.Term)
		return nil
	}

	// update node and make a Follower
	if candidate.Term > node.currentTerm {
		node.currentTerm = candidate.Term
		node.role = Follower
		node.votedFor = 0 // represents null
		node.heartbeatTimer.Stop()
	} else {
		debugf("[Stale Candidate] Node %d term: %d < Candidate %d term %d", node.id, node.currentTerm, candidate.CandidateId, candidate.Term)
	}

	myLastLogIndex := len(node.logs)

	logOk := candidate.LastLogTerm > node.LastLogTerm() ||
		(candidate.LastLogTerm == node.LastLogTerm() && candidate.LastLogIndex >= myLastLogIndex)

	if !logOk {
		debugf("Candidate lastLogTerm or lastLogIndex is out of sync")
		response.Term = node.currentTerm
		response.VoteGranted = false
		return nil
	}

	response.Term = node.currentTerm
	canVote := node.votedFor == 0 || node.votedFor == candidate.CandidateId
	// server only votes for candidate if logs candidate are up to date
	if canVote {
		node.votedFor = candidate.CandidateId
		// reset election timer
		node.electionTimer.Reset(time.Duration(node.electionTimeout) * time.Millisecond)
		response.VoteGranted = true
		debugf("Node %d voted for candidate %d", node.id, candidate.CandidateId)
	}

	return nil
}

type requestVoteResult struct {
	electionResult RequestVoteResponse
	err            error
}

func (node *Node) newLeader() {
	debugf("Node %d won the election", node.id)
	node.role = Leader
	node.nextIndex = make([]int, len(node.addresses))
	node.matchIndex = make([]int, len(node.addresses))

	for i := range node.addresses {
		// at most the size of the node's logs
		node.nextIndex[i] = len(node.logs) + 1
		// at least
		node.matchIndex[i] = 0
	}

	node.replicate(&EntriesArgs{})
}

func (node *Node) startElection() error {
	debugf("Node %d is starting an election", node.id)
	node.currentTerm += 1
	node.role = Candidate
	node.votedFor = node.id

	// contact all nodes
	requestVoteArgs := &RequestVoteArgs{
		Term:         node.currentTerm,
		CandidateId:  node.id,
		LastLogIndex: len(node.logs),
		LastLogTerm:  node.LastLogTerm(),
	}

	results := make(chan requestVoteResult, len(node.addresses))
	var startWg sync.WaitGroup

	for i := 0; i < len(node.addresses); i++ {
		startWg.Add(1)

		go func(id int) {
			defer startWg.Done()

			client, err := rpc.Dial("tcp", node.addresses[i])
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var response RequestVoteResponse
			if err := client.Call("Node.RequestVote", requestVoteArgs, &response); err != nil {
				log.Fatal("raft error:", err)
			}

			results <- requestVoteResult{electionResult: response, err: err}
		}(i + 1)
	}

	go func() {
		startWg.Wait()
		close(results)
	}()

	vote_count := 1

	for res := range results {
		if res.err != nil {
			return errors.New("encountered an error in starting an election: " + res.err.Error())
		}

		if res.electionResult.VoteGranted {
			vote_count += 1
		}

		if node.currentTerm < res.electionResult.Term {
			node.currentTerm = res.electionResult.Term
		}
	}

	// all peers + self
	population := len(node.addresses) + 1
	majority := population/2 + 1
	if vote_count >= majority {
		node.newLeader()
	}

	return nil
}
