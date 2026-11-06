package raft

import (
	"net"
	"net/rpc"
	"reflect"
	"testing"
)

// serveTestNode registers node on a net/rpc server listening on an
// OS-assigned port and records that port in node.address, so other nodes
// can dial it the same way replicate() does in production.
func serveTestNode(t *testing.T, node *Node) string {
	t.Helper()

	server := rpc.NewServer()
	if err := server.Register(node); err != nil {
		t.Fatalf("register: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	go server.Accept(l)

	node.address = l.Addr().String()
	return node.address
}

func TestReplicate_CommitsOnMajority(t *testing.T) {
	enableDebugLogging(t)

	followerA := newTestNode(2)
	followerB := newTestNode(3)
	addrA := serveTestNode(t, followerA)
	addrB := serveTestNode(t, followerB)

	leader := newTestNode(1)
	leader.addresses = []string{addrA, addrB}
	leader.role = Leader
	leader.nextIndex = []int{1, 1}
	leader.matchIndex = []int{0, 0}

	if err := leader.replicate(&EntriesArgs{Command: []string{"SET 10", "ADD 5"}}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	want := []LogEntry{{Term: 1, Command: "SET 10"}, {Term: 1, Command: "ADD 5"}}
	for _, follower := range []*Node{followerA, followerB} {
		if !reflect.DeepEqual(follower.logs, want) {
			t.Errorf("node %d: expected logs %v, got %v", follower.id, want, follower.logs)
		}
	}

	if !reflect.DeepEqual(leader.matchIndex, []int{2, 2}) {
		t.Errorf("expected matchIndex [2 2], got %v", leader.matchIndex)
	}
	if !reflect.DeepEqual(leader.nextIndex, []int{3, 3}) {
		t.Errorf("expected nextIndex [3 3], got %v", leader.nextIndex)
	}
	if leader.commitIndex != 2 || leader.data != 15 {
		t.Errorf("expected leader commitIndex 2 and data 15, got %d and %d", leader.commitIndex, leader.data)
	}

	// followers only learn about the new commitIndex on the next AppendEntries
	for _, follower := range []*Node{followerA, followerB} {
		if follower.commitIndex != 0 {
			t.Errorf("node %d: expected commitIndex 0 before heartbeat, got %d", follower.id, follower.commitIndex)
		}
	}

	if err := leader.replicate(&EntriesArgs{}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	for _, follower := range []*Node{followerA, followerB} {
		if follower.commitIndex != 2 || follower.data != 15 {
			t.Errorf("node %d: expected commitIndex 2 and data 15, got %d and %d", follower.id, follower.commitIndex, follower.data)
		}
	}
}

// A leader may not commit entries from earlier terms by counting replicas
// (§5.4.2); they only commit indirectly, once an entry from its own term does.
func TestAdvanceCommitIndex_OnlyCommitsCurrentTermEntries(t *testing.T) {
	enableDebugLogging(t)

	leader := newTestNode(1)
	leader.role = Leader
	leader.currentTerm = 3
	leader.addresses = []string{"peerA", "peerB"} // never dialed, only counted
	leader.logs = []LogEntry{{Term: 1, Command: "SET 10"}, {Term: 1, Command: "ADD 5"}}
	leader.matchIndex = []int{2, 2}

	leader.advanceCommitIndex()

	if leader.commitIndex != 0 || leader.data != 0 {
		t.Errorf("expected old-term entries to stay uncommitted, got commitIndex %d and data %d", leader.commitIndex, leader.data)
	}

	leader.logs = append(leader.logs, LogEntry{Term: 3, Command: "ADD 1"})
	leader.matchIndex = []int{3, 0}

	leader.advanceCommitIndex()

	if leader.commitIndex != 3 || leader.lastApplied != 3 || leader.data != 16 {
		t.Errorf("expected commitIndex 3, lastApplied 3 and data 16, got %d, %d and %d", leader.commitIndex, leader.lastApplied, leader.data)
	}
}

func TestReplicate_RejectionBacksUpNextIndex(t *testing.T) {
	enableDebugLogging(t)

	follower := newTestNode(2)
	addr := serveTestNode(t, follower)

	// leader wrongly assumes the follower already has index 1
	leader := newTestNode(1)
	leader.addresses = []string{addr}
	leader.role = Leader
	leader.logs = []LogEntry{{Term: 1, Command: "SET 10"}}
	leader.nextIndex = []int{2}
	leader.matchIndex = []int{0}

	if err := leader.replicate(&EntriesArgs{}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	if leader.nextIndex[0] != 1 || leader.matchIndex[0] != 0 {
		t.Errorf("expected nextIndex 1 and matchIndex 0 after rejection, got %d and %d", leader.nextIndex[0], leader.matchIndex[0])
	}
	if len(follower.logs) != 0 {
		t.Errorf("expected follower log untouched on rejection, got %v", follower.logs)
	}

	// retry from the backed-up index now succeeds
	if err := leader.replicate(&EntriesArgs{}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	if !reflect.DeepEqual(follower.logs, leader.logs) {
		t.Errorf("expected follower logs %v, got %v", leader.logs, follower.logs)
	}
	if leader.matchIndex[0] != 1 || leader.nextIndex[0] != 2 || leader.commitIndex != 1 {
		t.Errorf("expected matchIndex 1, nextIndex 2 and commitIndex 1, got %d, %d and %d", leader.matchIndex[0], leader.nextIndex[0], leader.commitIndex)
	}
}

func TestReplicate_HigherTermResponseStepsDown(t *testing.T) {
	enableDebugLogging(t)

	follower := newTestNode(2)
	follower.currentTerm = 5
	addr := serveTestNode(t, follower)

	leader := newTestNode(1)
	leader.addresses = []string{addr}
	leader.role = Leader
	leader.nextIndex = []int{1}
	leader.matchIndex = []int{0}

	if err := leader.replicate(&EntriesArgs{Command: []string{"SET 10"}}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	if leader.role != Follower || leader.currentTerm != 5 {
		t.Errorf("expected Follower at term 5, got %v at term %d", leader.role, leader.currentTerm)
	}
	if leader.commitIndex != 0 || leader.data != 0 {
		t.Errorf("expected nothing committed, got commitIndex %d and data %d", leader.commitIndex, leader.data)
	}
}

func TestReplicate_NonLeaderDoesNothing(t *testing.T) {
	enableDebugLogging(t)

	node := newTestNode(1) // Follower, with no addresses to dial

	if err := node.replicate(&EntriesArgs{Command: []string{"SET 10"}}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	if len(node.logs) != 0 {
		t.Errorf("expected non-leader to leave its log alone, got %v", node.logs)
	}
}

func TestApplyCommitted_SkipsMalformedCommands(t *testing.T) {
	enableDebugLogging(t)

	node := newTestNode(1)
	node.logs = []LogEntry{
		{Term: 1, Command: "SET 10"},
		{Term: 1, Command: "SUBTRACT 3"},
		{Term: 1, Command: "SET"},        // missing value
		{Term: 1, Command: "SET abc"},    // value not an int
		{Term: 1, Command: "MULTIPLY 2"}, // unknown command
		{Term: 1, Command: "ADD 1"},
	}
	node.commitIndex = 6

	node.applyCommitted()

	if node.lastApplied != 6 || node.data != 8 {
		t.Errorf("expected lastApplied 6 and data 8, got %d and %d", node.lastApplied, node.data)
	}
}

// Population 3, majority 2: the leader plus one reachable follower commits.
func TestReplicate_CommitsWithOneFollowerUnreachable(t *testing.T) {
	enableDebugLogging(t)

	follower := newTestNode(2)

	leader := newTestNode(1)
	leader.addresses = []string{serveTestNode(t, follower), unreachableAddress(t)}
	leader.role = Leader
	leader.nextIndex = []int{1, 1}
	leader.matchIndex = []int{0, 0}

	if err := leader.replicate(&EntriesArgs{Command: []string{"SET 10"}}); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	if !reflect.DeepEqual(leader.matchIndex, []int{1, 0}) || !reflect.DeepEqual(leader.nextIndex, []int{2, 1}) {
		t.Errorf("expected matchIndex [1 0] and nextIndex [2 1], got %v and %v", leader.matchIndex, leader.nextIndex)
	}
	if leader.commitIndex != 1 || leader.data != 10 {
		t.Errorf("expected commitIndex 1 and data 10, got %d and %d", leader.commitIndex, leader.data)
	}
}
