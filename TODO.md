# TODO:

## Raft:
- [ ] Strong leader
- [ ] Leader election
- [ ] Membership changes

## Docker
- [ ] Creating containers for PG versions
- [ ] Logical Replication among versions
- [ ] Automate the provisioning of new clusters 

## TUI
- [ ] Take inputs from user to:
 - [ x ] build system with N Nodes,
 - [ x ] start all nodes
 - [ x ] view node's state (i.e select * from nodes where pk = id)
 - [ x ] pause node while running
 - [ ] weird condition: resume node while running
 - [ x ] help command

## GUI
- [ ] It'd be neat to get a GUI for the sake of visualization 

## Architecture
- [ x ] Sketch out architecture for MVP

## RPC + Golang + Context
- [ x ] Get an idea of RPC + HTTP server

## Elections
- [ x ] Contact node-node via RPC to request votes
    - [ x ] Parallelize RPC calls
    - [ ] If AppendEntries RPC received from new leader: convert to follower

## Heartbeat
- [ x ] Figure out a heartbeat timeout
- [ x ] Once a leader is elected, send out heartbeats
  - [ x ] Parallelize RPC calls

## AppendEntries
- [ ] Create API call from client to SET val

## Refactor
- [ ] Determing naming convetion from the "server" and the "client" call
   - [ ] replicate - AppendEntires
   - [ ] startElection - RequestVote
- [ ] Reusable function for parallizing a RPC call
   - [ ] clean up replicate() and startElection()

## Safety
- [ ] Add sync mutex to `role`, `currentFollower` ...
