# Notes on In Search of an Understandable Consensus Algorithm

## Consensus
 - allow a collection of machines to work as a coherent group that can survive the failures of some of its members. Key idea in building large-scale software systems.

- Step 1: Client sends information
- Step 2: Consensus module handles Log
- Step 3: Log updates statemachine  
- Step 4: Updated state machine  returns a response to client

## What's wrong with Paxos? 
 - Paxos is difficult to understand
 - Single-decree Paxos is dense and subtle: it's divided into two complicated unintuitive stages
 - No widely agreed upon algorithm for mutli-Paxos
 - [wiki for paxos algo](https://en.wikipedia.org/wiki/Paxos_(computer_science))

## Understandability: 
 - problem decomposition: wherever possible, divide problems into seperate pieces that could be solved (i.e seperating leader election, log. repl., safety, and membership changes)
 - simplify state space


## Raft conensus algorithm:
- the consensus algorithm manages a replicated log containing state machine commands from clients. The state machine process identifical sequences of commands from the logs, so they produce the same outputs
- raft: first select a leader that will handle replication logs
- leader accepts log entries from clients, replicates them on the other servers, and tells servers when its safe to apply log entries to their state machines.
- if a leader fails or disconnects, another leader will be elected

- With the leader approach there are three relatively independent subproblems:
 - Leader election: when existing leader fails select a new one
 - Log replication: the leader must accept log entries from clients
 - Safety: at Nth index, log value is the same on all servers



## State:
- currentTerm: latest term server
- votedFor: candidateId that received vote in current term
- log[]: log entries; each entry contains command for state machine

### Volatile state on all servers:
- commitIndex: index of highest log entry known to be commited
- lastApplied: index of highest log entry applied to state machine

### Volatile state on leaders:
- nextIndex[] for each server, index of the next log entry to send to that server
- matchIndex[] for each server, index of highest log entry known to be replicated on server

### AppendEntries RPC (remote procedure calls)
- term: leader's term
- leaderId: so followers can redirect clients
- prevLogIndex: index of log entry immediately preceeding new ones
- prevLogTerm: term of prevLogIndex entry
- entries[]: log entries to store
- leaderCommit: leader's commitIndex

### Results: 
- term: currentTerm for leader to update itself
- success: true if follower contained entry matching prevLogIndex and prevLogTerm

### Receeiver implementation:
 - ... 

## RequestVote RPC
Invoked by candidates to gather votes
- term: candidate's term
- candidateId: candidate requesting vote
- lastLogIndex: index of candidate's last log entry
- lastLogTerm: term of candidate's last long entry

### Receeiver implementation:
 - ... 


## Rules for Servers:
### All servers
  - if commitIndex > lastApplied: increment lastApplied, apply `log[lastApplied]` to state machine
  - if RPC request or response contains term T > currentTerm: currentTerm = T, convert to followers

### Followers 
  - Respond to RPCs from candidates and leaders
  - If election timeout elapses without receiving AppendEntries RPC from current leader or grantving vote to canddiate: convert to candidate

### Candidates
 - On conversion to candidate, start election:
   - Increment currentTerm
   - Vote for self
   - Reset election timer
   - Send RequestVote RPCs to all other servers
 - if votes received from majority of servers: become leader
 - if AppendEntries RPC received from new leader: convert to follower
 - If election timeout elapses: start a new election

### Leaders
 - Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts


Client --> Leader --> Follower A
                |---> Follower B
                |---> Follower C 
Upon Election:
Leader's election_timeout = 100ms
Leader's heartbeat_timeout = 50ms

time: 0ms
Leader sends out empty AppendEntries to Followers and repeats every heartbeat_timeout
Leader -- AppendEntries --> Follower A
                      |---> Follower B
                      |---> Follower C 
time: 50ms
Leader sends out empty AppendEntries to Followers after heartbeat_timeout elapsed 
Leader -- AppendEntries --> Follower A
                      |---> Follower B
                      |---> Follower C 
time: 110ms
election_timeout (110ms - 50ms) elapsed and a follower will now turn into a candidate for an election


 - If command received from client: append entry to local log, respond after entry applied to state machine
 - If last log index >= newIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
   - If successful: update nextIndex and matchIndex for follower
   - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

 - If there exists an N such that N > commitIndex, a majority of `matchIndex[i] >= N`, and `log[N].term == currentTerm`: set commitIndex = N


## Raft Basics
- Raft servers communicate using remote procedure calls (RPCs), and the basic consensus algorithm requires only two types of RPCs.
  - RequestVote RPCs are initiated by canddiates during elections.  
  - AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of a heartbeat.  
- There's a third RPC for transferring snapshots between servers
- Servers retry RPCs if they do not receive a response in a timely manner, and they issue RPCs in parallel for best performance 

## Leader Election
 - A candidate continues until:
   - (a) it wins the election
     - receives votes from a majority of the servers
     - once candidate wins, sends heartbeat message to all of the other servers
   - (b) another server establishes itself as a leader 
     - if candidate receives AppendEntries RPC from other server
       - proposed_leader.currentTerm >= candidate.currentTerm ? candidate is now follower : rejects RPC and continues in candidate state
   - (c) a period of time goes by with no winner
     - there's a chance that several followers become candidates at the same time. Each candidate will time out and start a new election

## Log replication:
 - Once a leader is elected:
   - new requests are appended to its log as a new entry
   - Issues AppendEntries RPCs in parallel to each of theo ther servers
   - Once entry is safely replicated the leader applies the entry to its state machine
     - If followers crash or run slowly, the leader retries indefinetly
 - The leader handles inconsistencies by forcing the followers' logs to duplicate its own. This means that conflicting entries in follower logs will be overwritten with entries from the leader's log.

## Safety
 - A follower might be unavailable while the leader commits several log entries, then it could be elected leader and overwrite these entries with new ones 
 -  (The Leader Completeness Property from Figure 3): if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms
 - Raft approaches the inconsistency issue with: all committed entries from a previous term are present on each new leader from the moment of its election
   - LEADERS never overwrite existing entries in their logs (like MASTER-SLAVE)
 - Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs

## Timing and availability
 - broadcastTime << electionTimeout << MTBF
 - `broadcastTime`: (.5ms to 20ms)average time it takes a server to send RPCs in parallel to every server in the cluser and receive their responses
 - `electionTimeout`: (10ms to 500ms)if a follower receives no communication over a period of time
 - `MTBF`: (several months or more) average time between failures for a single server


# Notes on net/rpc
 - Package rpc provides access to the exported methods of an object across a network or other I/O connection
 - Only methods that satisfy these criteria will be made available for remote access:
   - the method' type is exported
   - the method is exported
   - the method has two arguments, both exported (or builtin) types
   - the method's second argument is a pointer
   - the method has return type error
 - schematically like: func (t *T) MethodName(argType T1, replyType *T2) error
   - T1 and T2 can be marshaled by encoding/gov
   - argType: the arguments provided by the caller
   - the result parameters to be returned to the caller
 - See `/rpc-example` for a basic implementation 



## Notes on Voting Algorithm:
 - Candidate in a parallel requests votes from all other nodes

 - Claude assisted pseudocode:
   - 1. If candidate's term < node's currentTerm then reject
   - 2. Catch up to a newer term: unconditional and play catch up
        - node's currentTerm = candidate's term
        - update node to Follower
        - updaet votedFor to null
   - 3. Is the candidate up to date log wise?
       - candidate's last log index >= node's last log index
       - validate that the value at index match
         - handle on fresh start when logs are empty
         - candidate.lastLogTerm == node[lastLogTermIndex]
   - 4. Has the node voted for anyone else in the term?



## Log Replication Diagram:

Node A (leader)
Node B (follower)
Node C (follower)


You're on Instagram in Los Angeles, CA
    --> Create Account (UI)
        --> Meta Server (in a region near you)
            --> Checks to see if the username exists, passwords, does your account already exist? -->
                -->  Bad?
                    --> Return 400 "Sorry mate"
                -->  Good?
                    --> Contact the DB (in our case the leader)
                        --> Log {user: "Chlodiggity", email: chlodiggity@email.com,  password: 123432123asbdsajdnsadsadjasdn}
                            --> Node B (DB in NY) and Node C (DB in Asia)
                                --> Hey can you insert into your DB?
                                    --> If majority insert then the request is commited in the leader
                                        --> RETURN 200, YAY

## Figure 7:
 - When a leader at the top comes to power it is possible that any of the scenarios (a-f) could occur in the follower logs
   - missing entries
   - extra uncommited entries
   - both (missing and extra uncommited entries)
    - [1 1 1 4 4 5 5 6 6 6] leader
    - [1 1 1 2 2 2 3 3 3 3 3] (node we're talking about) one of the followers
     - if node became leader --> added several entries to its log
       --> then crashed before committing any of them
         --> restarted quickly --> added several entries to its log
           --> then crashed before committing any of them


## Notes on AppendEntries:
 - 1. Client --> Leader
   - Leader appends the command to its log as a new entry, then issues AppendEntries RPC (in parallel)
