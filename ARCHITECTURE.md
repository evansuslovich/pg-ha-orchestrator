
# MVP

## What can the Client do?
 - can build Nodes
 - can request state
 - can view a Node
 - can pause a Node
 - can resume a Node
 - can adjust the state
     - (i.e SET X = 3)
## What does the Server do?
 - initialize `raft/main.go` the orchestrator

## Main
 - The orchestrator of the project
   - sits between the Client and the Nodes

## Node
 - Both an RPC server and client (full mesh) via request-response
   - uses TCP to communicate via rpc.Dial


