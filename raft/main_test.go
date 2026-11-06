package raft

import (
	"strings"
	"testing"
)

func TestFreezeUnfreeze_OnlyTouchesRunningNodes(t *testing.T) {
	enableDebugLogging(t)

	running := newTestNode(1)
	go running.Run()
	waitForState(t, running, Running)
	t.Cleanup(running.Pause)

	paused := newTestNode(2)
	paused.condition.state = Paused
	fresh := newTestNode(3)

	raft := &Raft{Nodes: []*Node{running, paused, fresh}}

	var response Response
	raft.Freeze(&EmptyArgs{}, &response)
	if response.Value != "froze 1 node(s)" {
		t.Errorf("expected 'froze 1 node(s)', got %q", response.Value)
	}
	if running.condition.state != Paused || paused.condition.state != Paused || fresh.condition.state != Fresh {
		t.Errorf("expected states Paused, Paused, Fresh, got %v, %v, %v", running.condition.state, paused.condition.state, fresh.condition.state)
	}

	raft.Freeze(&EmptyArgs{}, &response)
	if response.Value != "cluster already frozen" {
		t.Errorf("expected second Freeze to be a no-op, got %q", response.Value)
	}

	raft.Unfreeze(&EmptyArgs{}, &response)
	if response.Value != "unfroze 1 node(s)" {
		t.Errorf("expected 'unfroze 1 node(s)', got %q", response.Value)
	}
	waitForState(t, running, Running)
	if paused.condition.state != Paused || fresh.condition.state != Fresh {
		t.Errorf("expected untouched nodes to stay Paused and Fresh, got %v and %v", paused.condition.state, fresh.condition.state)
	}

	raft.Unfreeze(&EmptyArgs{}, &response)
	if response.Value != "cluster is not frozen" {
		t.Errorf("expected second Unfreeze to be a no-op, got %q", response.Value)
	}
}

func TestEntries_RequiresRunningLeader(t *testing.T) {
	enableDebugLogging(t)

	pausedLeader := newTestNode(1)
	pausedLeader.role = Leader
	pausedLeader.condition.state = Paused
	follower := newTestNode(2)
	follower.condition.state = Running

	raft := &Raft{Nodes: []*Node{pausedLeader, follower}}

	err := raft.Entries(&EntriesArgs{Command: []string{"SET 10"}}, &EntriesResponse{})
	if err == nil || err.Error() != "Leader not found" {
		t.Errorf("expected 'Leader not found', got %v", err)
	}
}

func TestRaft_UnknownNodeIdReturnsError(t *testing.T) {
	raft := &Raft{Nodes: []*Node{newTestNode(1)}}
	args := &ViewArgs{Id: 99}

	if err := raft.View(args, &ViewResponse{}); err == nil {
		t.Errorf("View: expected error for unknown node id")
	}
	if err := raft.Pause(args, &Response{}); err == nil {
		t.Errorf("Pause: expected error for unknown node id")
	}
	if err := raft.Resume(args, &Response{}); err == nil {
		t.Errorf("Resume: expected error for unknown node id")
	}
}

func TestRaftView_ReturnsNodeViewOrError(t *testing.T) {
	node := newTestNode(1)
	serveTestNode(t, node)
	raft := &Raft{Nodes: []*Node{node}}

	var response ViewResponse
	if err := raft.View(&ViewArgs{Id: 1}, &response); err != nil {
		t.Fatalf("View: %v", err)
	}
	if !strings.Contains(response.Content, "===== Node 1") {
		t.Errorf("expected view of node 1, got %q", response.Content)
	}

	// an unreachable node returns an error instead of killing the process
	node.address = unreachableAddress(t)
	if err := raft.View(&ViewArgs{Id: 1}, &ViewResponse{}); err == nil {
		t.Errorf("expected error viewing an unreachable node")
	}
}

func TestLive_ShowsEveryNode(t *testing.T) {
	leader := newTestNode(1)
	leader.role = Leader
	raft := &Raft{Nodes: []*Node{leader, newTestNode(2)}}

	var response ViewResponse
	raft.Live(&EmptyArgs{}, &response)

	for _, want := range []string{"Leader", "Follower", "currentTerm"} {
		if !strings.Contains(response.Content, want) {
			t.Errorf("expected live view to contain %q, got:\n%s", want, response.Content)
		}
	}
}
