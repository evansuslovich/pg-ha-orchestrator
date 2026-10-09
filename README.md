# pg-ha-orchestrator

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go, built as the foundation for a high-availability PostgreSQL orchestrator.

Right now the project runs a small Raft cluster locally. An interactive client lets you build nodes, watch leader election happen, replicate commands through the leader, and pause or resume nodes to simulate failures. The Postgres side (containers, logical replication, cluster provisioning) is planned but not started yet. See [TODO.md](TODO.md).

## How it works

```
┌────────┐  net/rpc over HTTP   ┌──────────────┐   net/rpc over TCP   ┌─────────┐
│ client │ ───────────────────▶ │    server    │ ───────────────────▶ │ Node 1  │ :1235
│ (REPL) │      :1234           │ (raft.Raft)  │                      │ Node 2  │ :1236
└────────┘                      └──────────────┘                      │ Node 3  │ :1237
                                                                      └─────────┘
                                                                  full mesh: each node is
                                                                  both RPC server and client
```

- **client** (`client/`): an interactive prompt that sends commands to the server.
- **server** (`server/`): the control plane. It registers a `raft.Raft` instance on `:1234` and manages the lifecycle of the nodes (build, run, pause, resume, freeze). It sits outside the protocol and doesn't take part in it.
- **raft** (`raft/`): the nodes themselves. Each node listens on its own TCP port (`:1235`, `:1236`, …) and talks to its peers directly.
  - `server.go`: node state, the run loop (election and heartbeat timers), pause/resume, and speed control
  - `leader_election.go`: the `RequestVote` RPC and `startElection`
  - `log_replication.go`: the `AppendEntries` RPC, `replicate`, commit index advancement, and applying committed entries to the state machine

The replicated state machine is a single integer, `data`, which changes through `SET`, `ADD`, and `SUBTRACT` commands. Once an entry is committed on a majority, every node applies it.

## Getting started

Requirements: Go 1.27+

Start the server in one terminal. It runs with debug logging, so you can watch elections and replication happen:

```sh
make server
```

Start the client in a second terminal:

```sh
make client
```

### Example session

```
Enter a command: build 3          # start 3 nodes (max 5)
Count of nodes running: 3
Enter a command: run              # start election timers; a leader is elected after a few seconds
Enter a command: set 10           # replicate "SET 10" through the leader
Enter a command: add_entries add 5, minus 2
Enter a command: select 2         # inspect node 2: role, term, log, commitIndex, data, ...
Enter a command: pause 1          # simulate a node failure
Enter a command: resume 1
Enter a command: freeze           # pause every running node to inspect a snapshot of the cluster
Enter a command: resume           # resume only the nodes that freeze paused
```

### Client commands

| Command | Description |
| --- | --- |
| `build <count>` | Build `<count>` nodes (max 5) |
| `run` | Start all nodes |
| `select <id>` | View a node's full state |
| `pause <id>` | Pause a node |
| `resume <id>` | Resume a paused node |
| `freeze` | Pause all running nodes |
| `resume` | Resume the nodes paused by `freeze` |
| `set <value>` | Replicate `SET <value>` |
| `add <value>` | Replicate `ADD <value>` |
| `minus <value>` | Replicate `SUBTRACT <value>` |
| `add_entries <op> <value>, ...` | Replicate several entries in one batch, e.g. `add_entries set 10, add 30, minus 40` |
| `speed {snail,slow,medium,fast}` | Change election and heartbeat timeouts on all nodes |
| `help` | List commands |
| `exit` / `:q` | Quit |

## Development

```sh
make build   # go build ./...
make test    # go test ./raft/... -v
```

Run the server with `-debug` (or `-d`) to turn on debug logging. `make server` does this for you.

`rpc-example/` is a standalone module from early experiments with Go's `net/rpc`. It isn't part of the main build.

## Status

Implemented:
- Leader election (`RequestVote`), with randomized election timeouts
- Heartbeats and log replication (`AppendEntries`), including log conflict resolution
- Commit index advancement on majority replication, and applying committed entries to the state machine
- Pausing, resuming, and freezing nodes to simulate failures

Not yet implemented:
- Persistent state (all state is in memory)
- Membership changes
- Proper locking around node state
- Anything Postgres. See [TODO.md](TODO.md).

## The journey

This project is also a learning log. If you want to see how it came together, these files record the process:

- [NOTES.md](NOTES.md): my notes from reading *In Search of an Understandable Consensus Algorithm*. They cover why Raft was designed to be easier to understand than Paxos, how it splits consensus into leader election, log replication, and safety, the state each server keeps, the `AppendEntries` and `RequestVote` RPCs, and the rules for followers, candidates, and leaders. They finish with a timeline sketch of how heartbeats hold off elections.
- [ISSUES.md](ISSUES.md): problems I ran into while building this and how I worked through them. Examples: why calling `rpc.HandleHTTP` and `http.Serve` on every node broke Go's global default RPC server, why the client hung until `http.Serve` moved into a goroutine, using `chan struct{}` to signal timers to stop, deciding whether nodes or the orchestrator should know peer addresses, getting the heartbeat timer to keep the election timer from firing, and the move from a bare signal channel to a mutex-guarded `Condition` once nodes needed running, paused, and fresh states.
- [ARCHITECTURE.md](ARCHITECTURE.md): the original MVP sketch. It describes what the client should be able to do, the server's role as orchestrator, and nodes as full-mesh RPC peers.
- [TODO.md](TODO.md): what's done and what's next. It covers Raft features, Docker and Postgres replication, the TUI, a possible GUI for visualization, and planned refactors.
- [DOCS.md](DOCS.md): the resources I learned from. These include the Raft paper, visualizations, John Ousterhout's talk, and the PostgreSQL docs on high availability, log shipping, the WAL, and logical replication.
