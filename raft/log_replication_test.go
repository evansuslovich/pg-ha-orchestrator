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
