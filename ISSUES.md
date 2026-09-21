# Issues (along the way that differ from learnings or notes)

## HandleHTTP and http.Serve versus rpc.NewServer()
 - Currently the architecture of my project follows:
 - Client asks the Server to call `Raft.Run` which spins up Nodes
   - Client dials on tcp port 1234 and passes in `args` and `response` 
   - Server creats a Raft object, registers the server and HandleHTTP
     - `Register()` publishes the receiver's methods in the DefaultServer
       - `DefaultServer` is an instace of `Server` 
     - `HandleHTTP()` registers an HTTP handler for TPC messages to `DefaultServer`

 - What was I doing wrong? 
   - I was following this pattern in `Raft.Run` on each Node: calling `rpc.HandleHTTP` and `http.serve`
   - This broke the global-singleton design

## Running http.Serve(...) in Raft.Run 
 - On initialization of a new node, I ran http.Serve(...) resulting in the client to hang
 - I'll have to put it inside a goroutine 


## How to stop node.timer.c?
 - using `chan struct{}`: a channel used exclusively for signaling and synchronization between goroutines, rather than for trasferring data.
 - benefits of `chan struct{}`
   - zero memory footprint: o bytes
   - clear code intent: tells the reader that data isn't passed, rather coordinating

## LeaderElection RPC
 - I'm curious if the Node should be aware of the other Nodes and the `main.go` function handles it
 - main.go is more of a control plane than a participant in the protocol itself
 - adding addresses (`addresses []string`)of other nodes as a field in Node's state
   - address: string representing TCP address

## Heartbeat Timer
 - Similiar to electionTimer, heartbeatTimer runs on a shorter time length
 - Only start the timer when a node is a `Leader`
 - The issue I faced was with the heartbeatTimer, since heartbeatTimeout < electionTimeout
   - upon each iteration of heartbeatTimer, `Reset` is called on `node.electionTimer`
