
# MVP

## What can the Client do?
 - the client can build Nodes
 - the client can view the content of a Node
 - the client can request state 
 - the client can adjust the state 
     - (i.e SET X = 3)
## What does the Server do?
 - initialize `raft/main.go` the orchestrator

## Main
 - The orchestrator of the project
   - sits between the Client and the Nodes

## Node
 - Both an RPC server and client (full mesh) via request-response
   - uses TCP to communicate via rpc.Dial


