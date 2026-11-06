package raft

import (
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"testing"
	"time"
)

type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// Debug is set once here rather than per test: Run goroutines started by
// Resume can't be joined, so they may still read it after their test ends.
// Logs are discarded unless a test opts in with enableDebugLogging.
func TestMain(m *testing.M) {
	Debug = true
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

func enableDebugLogging(t *testing.T) {
	t.Helper()
	prevOutput := log.Writer()
	log.SetOutput(testLogWriter{t})
	t.Cleanup(func() { log.SetOutput(prevOutput) })
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
	// leader has NOT committed the entry, so the follower adds to its log
	args := &AppendEntriesArgs{Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: 0}

	var response AppendEntriesResponse
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("AppendEntries call failed: %v", err)
	}

	if !response.Success {
		t.Errorf("expected AppendEntries to be successful")
	}

	if len(node.logs) != 1 {
		t.Errorf("expected entry stored in log, got %d entries", len(node.logs))
	}

	if node.commitIndex != 0 || node.lastApplied != 0 || node.data != 0 {
		t.Errorf("uncommitted entry must not be applied: commitIndex=%d, lastApplied=%d data=%d", node.commitIndex, node.lastApplied, node.data)
	}

	// increment leader's matchIndex[node.id] = prevLogIndex (0) + len(entries) (1)
	// matchIndex[node.id] = 1
	// nextIndex[node.id] = 2
	// advanceCommitIndex
	// population = 2
	// majority (2)
	// n = 1
	// n > leader.commitIndex = 0
	//  leader.logs[0].Term == leader.currentTerm
	// replication_count := 1
	// for _, match range leader.matchIndex
	// if match >= n (1 >= 1)
	// replication_count += 1
	// replication_count (2) >= 2
	// leader.commitIndex = 1
	// leader.applyCommited()
	// node.lastApplied += 1
	// node.data = 10

	args = &AppendEntriesArgs{Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: nil, LeaderCommit: 1}
	if err := client.Call("Node.AppendEntries", args, &response); err != nil {
		t.Fatalf("Heartbeat call failed: %v", err)
	}

	// if leader.LeaderCommit (1) > node.commitIndex (0) {
	//  node.commitIndex = min(leader.LeaderCommit (1), leader.PrevLogIndex + len(leader.Entries) (1))
	// node.commitIndex = 1
	// node.applyCommited()
	// node.lastApplied
	// ... increments lastApplied to commitIndex
	// }

	if !response.Success {
		t.Errorf("expected AppendEntries to be successful")
	}

	if len(node.logs) != 1 {
		t.Errorf("expected entry stored in log, got %d entries", len(node.logs))
	}

	if node.commitIndex != 1 || node.lastApplied != 1 || node.data != 10 {
		t.Errorf("expected committed entry must applied: commitIndex=%d, lastApplied=%d data=%d", node.commitIndex, node.lastApplied, node.data)
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
	node.commitIndex = 2
	node.lastApplied = 2
	client := startTestNode(t, node)

	// leader.logs = []LogEntry{
	// 	{Command: "SET 10", Term: 1},
	// 	{Command: "SET 15", Term: 1},
	// 	{Command: "SET 20", Term: 3},
	// }
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
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
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

// Population 4, majority 3. Two peers grant the vote, but a third is already
// at a higher term. Seeing that term must turn the candidate back into a
// follower, even though it collected enough votes.
func TestStartElection_HigherTermResponseStepsDown(t *testing.T) {
	enableDebugLogging(t)

	peerA := newTestNode(2)
	peerB := newTestNode(3)
	peerC := newTestNode(4)
	peerC.currentTerm = 5

	candidate := newTestNode(1)
	candidate.addresses = []string{serveTestNode(t, peerA), serveTestNode(t, peerB), serveTestNode(t, peerC)}

	if err := candidate.startElection(); err != nil {
		t.Fatalf("startElection: %v", err)
	}

	if candidate.role != Follower {
		t.Errorf("expected role Follower after seeing higher term, got %v", candidate.role)
	}
	if candidate.currentTerm != 5 {
		t.Errorf("expected currentTerm = 5, got %d", candidate.currentTerm)
	}
}

func TestRequestVote_RejectsCandidateWithStaleLog(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)
	node.logs = []LogEntry{{Term: 2, Command: "SET 10"}}
	client := startTestNode(t, node)

	// longer log, but its last entry has an older term
	olderTerm := &RequestVoteArgs{Term: 3, CandidateId: 2, LastLogIndex: 5, LastLogTerm: 1}
	var response RequestVoteResponse
	if err := client.Call("Node.RequestVote", olderTerm, &response); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}
	if response.VoteGranted {
		t.Errorf("expected vote rejected for candidate with older last log term")
	}

	// same last log term, but a shorter log
	shorterLog := &RequestVoteArgs{Term: 3, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 2}
	response = RequestVoteResponse{}
	if err := client.Call("Node.RequestVote", shorterLog, &response); err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}
	if response.VoteGranted {
		t.Errorf("expected vote rejected for candidate with shorter log")
	}

	if node.votedFor != 0 {
		t.Errorf("expected no vote cast, got votedFor = %d", node.votedFor)
	}
}

func TestGetAddressInfo_SplitsSelfFromPeers(t *testing.T) {
	addresses := []string{":1235", ":1236", ":1237"}

	self, peers := getAddressInfo(&NodeArgs{Id: 2, Addresses: addresses})

	if self != ":1236" {
		t.Errorf("expected own address :1236, got %s", self)
	}
	if want := []string{":1235", ":1237"}; !reflect.DeepEqual(peers, want) {
		t.Errorf("expected peers %v, got %v", want, peers)
	}
}

// unreachableAddress returns a loopback address nothing is listening on.
func unreachableAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitForState(t *testing.T, node *Node, want NodeState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		state := node.condition.state
		node.mu.Unlock()
		if state == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("node %d never reached state %v", node.id, want)
}

// Population 3, majority 2: the candidate plus one reachable peer is enough.
func TestStartElection_WinsWithOnePeerUnreachable(t *testing.T) {
	enableDebugLogging(t)

	peer := newTestNode(2)
	candidate := newTestNode(1)
	candidate.addresses = []string{serveTestNode(t, peer), unreachableAddress(t)}

	if err := candidate.startElection(); err != nil {
		t.Fatalf("startElection: %v", err)
	}

	if candidate.role != Leader || candidate.currentTerm != 2 {
		t.Errorf("expected Leader at term 2, got %v at term %d", candidate.role, candidate.currentTerm)
	}
	if peer.votedFor != 1 {
		t.Errorf("expected peer to vote for 1, got %d", peer.votedFor)
	}
}

func TestPauseResume_StopsAndRestartsRun(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)

	done := make(chan struct{})
	go func() {
		node.Run()
		close(done)
	}()
	waitForState(t, node, Running)

	node.Pause()
	node.Pause() // already paused: must not close pauseCh twice

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Run did not return after Pause")
	}

	node.Resume()
	t.Cleanup(node.Pause)
	waitForState(t, node, Running)
}

// Pausing a node before it has ever run (e.g. build, pause 2, run) must
// keep it paused, so a later Resume can start it.
func TestRun_PausedBeforeStartStaysPaused(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)

	node.Pause()
	node.Run() // pauseCh is already closed, so this returns immediately

	state := node.condition.state
	if state != Paused {
		t.Errorf("expected state Paused, got %v", state)
	}
}

func TestSetSpeed_UpdatesTimeouts(t *testing.T) {
	enableDebugLogging(t)
	node := newTestNode(1)

	node.SetSpeed("fast")

	if node.condition.speed != Fast || node.heartbeatTimeout != 1499 {
		t.Errorf("expected speed Fast and heartbeatTimeout 1499, got %v and %d", node.condition.speed, node.heartbeatTimeout)
	}
	if node.electionTimeout < 1499 || node.electionTimeout >= 1499+1000 {
		t.Errorf("expected electionTimeout in [1499, 2499), got %d", node.electionTimeout)
	}

	node.SetSpeed("warp")

	if node.condition.speed != Fast || node.heartbeatTimeout != 1499 {
		t.Errorf("expected unknown speed to leave settings alone, got %v and %d", node.condition.speed, node.heartbeatTimeout)
	}
}
