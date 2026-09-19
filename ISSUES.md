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
