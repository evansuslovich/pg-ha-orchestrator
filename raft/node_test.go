package raft

import (
	"log"
	"net"
	"net/rpc"
	"testing"
	"time"
)

type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func enableDebugLogging(t *testing.T) {
	t.Helper()
	prevDebug, prevOutput := Debug, log.Writer()
	Debug = true
	log.SetOutput(testLogWriter{t})
	t.Cleanup(func() {
		Debug = prevDebug
		log.SetOutput(prevOutput)
	})
}

// startTestNode registers node on a real net/rpc server listening on an
// OS-assigned port (matching the pattern StartServer uses in production)
// and returns a client dialed to it. Both are cleaned up automatically.
func startTestNode(t *testing.T, node *Node) *rpc.Client {
	t.Helper()

	server := rpc.NewServer()
	if err := server.Register(node); err != nil {
		t.Fatalf("register: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0") // :0 lets the OS pick a free port
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	go server.Accept(l)

	client, err := rpc.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	return client
}

func newTestNode(id int) *Node {
	return &Node{
		id:               id,
		role:             Follower,
		currentTerm:      1,
		electionTimeout:  5000,
		heartbeatTimeout: 999,
		electionTimer:    time.NewTimer(time.Millisecond),
		heartbeatTimer:   time.NewTimer(time.Millisecond),
		condition:        Condition{state: Fresh, pauseCh: make(chan struct{})},
	}
}

func TestRequestVote_GrantsVoteForUpToDateCandidate(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	client := startTestNode(t, node)

	args := &RequestVoteArgs{Term: 2, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
	var response RequestVoteResponse
	if err := client.Call("Node.RequestVote", args, &response); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}

	if !response.VoteGranted {
		t.Errorf("expected vote granted, got false")
	}
	if node.votedFor != 2 {
		t.Errorf("expected votedFor = 2, got %d", node.votedFor)
	}
	if node.currentTerm != 2 {
		t.Errorf("expected currentTerm = 2, got %d", node.currentTerm)
	}
}

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.currentTerm = 5
	client := startTestNode(t, node)

	args := &RequestVoteArgs{Term: 2, CandidateId: 2}
	var response RequestVoteResponse
	if err := client.Call("Node.RequestVote", args, &response); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}

	if response.VoteGranted {
		t.Errorf("expected vote rejected for stale term, got granted")
	}
	if response.Term != 5 {
		t.Errorf("expected response.Term = 5, got %d", response.Term)
	}
}

func TestRequestVote_RejectsSecondCandidateSameTerm(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	client := startTestNode(t, node)

	first := &RequestVoteArgs{Term: 2, CandidateId: 2}
	var firstResponse RequestVoteResponse
	if err := client.Call("Node.RequestVote", first, &firstResponse); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}
	if !firstResponse.VoteGranted {
		t.Fatalf("expected first candidate to win the vote")
	}

	second := &RequestVoteArgs{Term: 2, CandidateId: 3}
	var secondResponse RequestVoteResponse
	if err := client.Call("Node.RequestVote", second, &secondResponse); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}
	if secondResponse.VoteGranted {
		t.Errorf("expected second candidate in the same term to be rejected, got granted")
	}
}

func TestAppendEntries_HigherTermConvertsToFollower(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.role = Leader
	client := startTestNode(t, node)

	args := &AppendEntriesArgs{Term: 5, LeaderId: 2}
	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if node.role != Follower {
		t.Errorf("expected role Follower after higher-term AppendEntries, got %v", node.role)
	}
	if node.currentTerm != 5 {
		t.Errorf("expected currentTerm = 5, got %d", node.currentTerm)
	}
}

func TestAppendEntries_RejectsStaleLeader(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.currentTerm = 5
	client := startTestNode(t, node)

	args := &AppendEntriesArgs{Term: 2, LeaderId: 2}
	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if response.Success {
		t.Errorf("expected stale leader's AppendEntries to be rejected")
	}
	if response.Term != 5 {
		t.Errorf("expected response.Term = 5, got %d", response.Term)
	}
}

func TestAppendEntries_PausedNodeIgnoresRPC(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.condition.state = Paused
	client := startTestNode(t, node)

	args := &AppendEntriesArgs{Term: 2, LeaderId: 2}
	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if node.currentTerm != 1 {
		t.Errorf("expected paused node's term to stay unchanged, got %d", node.currentTerm)
	}
}

func TestAppendEntries_NewEntry(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	client := startTestNode(t, node)

	// empty log, so there is no previous entry to match against
	entries := []LogEntry{
		{Command: "SET 10", Term: 1},
	}
	args := &AppendEntriesArgs{Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: 0}

	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if response.Term != 1 {
		t.Errorf("expected leaders' term to be unchanged, got %d", response.Term)
	}

	if !response.Success {
		t.Errorf("expected leader' AppendEntries to be successful")
	}

	if len(node.logs) != 1 {
		t.Errorf("expected node's logs increased, expected 1 got %d", len(node.logs))
	}
}

// This exists in the case:
// Log Index 1 2 3 4
// Node A    1 1 2  // wins election at 2 and then crashes
// Node B    1 1 3  // wins election at 3
func TestAppendEntries_ExistingEntryConflictsWithNewOne(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.currentTerm = 3
	// existing entries
	node.logs = []LogEntry{
		{Command: "SET 10", Term: 1},
		{Command: "SET 15", Term: 1},
		{Command: "SET 20", Term: 2},
	}
	client := startTestNode(t, node)

	// leader's log:  [SET 10 t1] [SET 15 t1] [SET 25 t3]
	// leader first assumes the follower has its entry at index 3 (term 3)
	args := &AppendEntriesArgs{Term: 3, LeaderId: 2, PrevLogIndex: 3, PrevLogTerm: 3, Entries: nil, LeaderCommit: 2}

	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if response.Term != 3 {
		t.Errorf("expected leaders' term to be unchanged, got %d", response.Term)
	}

	if response.Success {
		t.Errorf("expected rejection: entry at prev log index has a different term")
	}

	if len(node.logs) != 3 {
		t.Errorf("expected node's logs unchanged on rejection, expected 3 got %d", len(node.logs))
	}

	// leader backs up one index and resends the entry that follows it
	args = &AppendEntriesArgs{
		Term: 3, LeaderId: 2,
		PrevLogIndex: 2, PrevLogTerm: 1,
		Entries:      []LogEntry{{Command: "SET 25", Term: 3}},
		LeaderCommit: 2,
	}
	response = AppendEntriesResponse{}
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if response.Term != 3 {
		t.Errorf("expected leaders' term to be unchanged, got %d", response.Term)
	}

	if !response.Success {
		t.Errorf("expected success: entries match up to prev log index")
	}

	if len(node.logs) != 3 {
		t.Fatalf("expected conflicting entry replaced, expected 3 logs got %d", len(node.logs))
	}

	if want := (LogEntry{Command: "SET 25", Term: 3}); node.logs[2] != want {
		t.Errorf("expected conflicting entry replaced with %v, got %v", want, node.logs[2])
	}
}
