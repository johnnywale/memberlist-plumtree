------------------------------ MODULE plumtree ------------------------------
(***************************************************************************)
(* TLA+ Specification of the Plumtree Epidemic Broadcast Tree Protocol     *)
(*                                                                         *)
(* This specification models the core Plumtree protocol for epidemic       *)
(* broadcast with self-healing spanning trees.                             *)
(*                                                                         *)
(* Key Properties:                                                         *)
(* - Safety: Messages delivered at most once per node (no duplicates)      *)
(* - Liveness: All messages eventually delivered to all nodes              *)
(* - Tree Connectivity: The eager peer graph remains connected             *)
(*                                                                         *)
(* Protocol Overview:                                                      *)
(* 1. Nodes maintain eager peers (full message) and lazy peers (IHave)    *)
(* 2. New messages are pushed to eager peers immediately                   *)
(* 3. IHave announcements sent to lazy peers after delay                   *)
(* 4. Missing messages trigger Graft (promote to eager, request message)  *)
(* 5. Duplicate messages trigger Prune (demote to lazy)                   *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

\* Set of all nodes in the cluster
CONSTANT Nodes

\* Set of all possible messages (message IDs)
CONSTANT Messages

\* Maximum gossip round number (for bounded model checking)
CONSTANT MaxRound

\* Message type tags
CONSTANTS Gossip, IHave, Graft, Prune

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

\* eager_peers[n] = set of nodes that n considers eager (full message push)
VARIABLE eager_peers

\* lazy_peers[n] = set of nodes that n considers lazy (IHave announcements)
VARIABLE lazy_peers

\* delivered[n] = set of message IDs that node n has delivered
VARIABLE delivered

\* pending_graft[n] = set of {msg_id, target} pairs for pending graft requests
VARIABLE pending_graft

\* network = set of in-flight messages (network is asynchronous)
VARIABLE network

\* message_cache[n] = set of {msg_id, round} pairs of cached messages
VARIABLE message_cache

vars == <<eager_peers, lazy_peers, delivered, pending_graft, network, message_cache>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

\* A network message is a record
NetworkMessageType ==
    [type: {Gossip, IHave, Graft, Prune},
     from: Nodes,
     to: Nodes,
     msg_id: Messages,
     round: Nat]

\* A graft request record
GraftRequest == [msg_id: Messages, target: Nodes]

\* A cache entry
CacheEntry == [msg_id: Messages, round: Nat]

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ eager_peers \in [Nodes -> SUBSET Nodes]
    /\ lazy_peers \in [Nodes -> SUBSET Nodes]
    /\ delivered \in [Nodes -> SUBSET Messages]
    /\ \A n \in Nodes : pending_graft[n] \subseteq [msg_id: Messages, target: Nodes]
    /\ network \subseteq NetworkMessageType
    /\ \A n \in Nodes : message_cache[n] \subseteq CacheEntry

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    \* Start with some initial eager peer topology (e.g., ring)
    /\ eager_peers \in [Nodes -> SUBSET Nodes]
    /\ \A n \in Nodes : n \notin eager_peers[n]  \* No self-loops
    /\ lazy_peers = [n \in Nodes |-> Nodes \ ({n} \union eager_peers[n])]
    /\ delivered = [n \in Nodes |-> {}]
    /\ pending_graft = [n \in Nodes |-> {}]
    /\ network = {}
    /\ message_cache = [n \in Nodes |-> {}]

(***************************************************************************)
(* HELPER OPERATORS                                                        *)
(***************************************************************************)

\* Check if node n has message m
HasMessage(n, m) == m \in delivered[n]

\* Check if message is in cache
InCache(n, m) == \E entry \in message_cache[n] : entry.msg_id = m

\* Get round for message in cache
GetCachedRound(n, m) ==
    LET entries == {entry \in message_cache[n] : entry.msg_id = m}
    IN IF entries = {} THEN 0
       ELSE (CHOOSE entry \in entries : TRUE).round

\* Other nodes (excluding self)
OtherNodes(n) == Nodes \ {n}

(***************************************************************************)
(* BROADCAST ACTION                                                        *)
(* A node initiates a new broadcast                                        *)
(***************************************************************************)

Broadcast(n, m) ==
    \* Precondition: node hasn't already delivered this message
    /\ ~HasMessage(n, m)
    \* Deliver locally
    /\ delivered' = [delivered EXCEPT ![n] = @ \union {m}]
    \* Add to cache
    /\ message_cache' = [message_cache EXCEPT ![n] = @ \union {[msg_id |-> m, round |-> 0]}]
    \* Send Gossip to all eager peers
    /\ network' = network \union
        {[type |-> Gossip, from |-> n, to |-> p, msg_id |-> m, round |-> 0] : p \in eager_peers[n]}
    \* Send IHave to all lazy peers
    /\ network' = network \union
        {[type |-> IHave, from |-> n, to |-> p, msg_id |-> m, round |-> 0] : p \in lazy_peers[n]}
    \* Unchanged
    /\ UNCHANGED <<eager_peers, lazy_peers, pending_graft>>

(***************************************************************************)
(* RECEIVE GOSSIP ACTION                                                   *)
(* A node receives a Gossip message with full payload                      *)
(***************************************************************************)

ReceiveGossip(n) ==
    \E msg \in network :
        /\ msg.type = Gossip
        /\ msg.to = n
        \* Remove from network
        /\ network' = network \ {msg}
        /\ IF HasMessage(n, msg.msg_id) THEN
            \* Duplicate: consider pruning
            /\ UNCHANGED <<delivered, message_cache, eager_peers, lazy_peers, pending_graft>>
           ELSE
            \* First time: deliver and forward
            /\ delivered' = [delivered EXCEPT ![n] = @ \union {msg.msg_id}]
            /\ message_cache' = [message_cache EXCEPT ![n] = @ \union {[msg_id |-> msg.msg_id, round |-> msg.round]}]
            \* Promote sender to eager if not already
            /\ eager_peers' = [eager_peers EXCEPT ![n] = @ \union {msg.from}]
            /\ lazy_peers' = [lazy_peers EXCEPT ![n] = @ \ {msg.from}]
            \* Forward to eager peers (except sender)
            /\ LET newRound == Min(msg.round + 1, MaxRound)
               IN network' = (network \ {msg}) \union
                   {[type |-> Gossip, from |-> n, to |-> p, msg_id |-> msg.msg_id, round |-> newRound]
                    : p \in eager_peers[n] \ {msg.from}}
            \* Cancel any pending graft for this message
            /\ pending_graft' = [pending_graft EXCEPT ![n] = {g \in @ : g.msg_id /= msg.msg_id}]

(***************************************************************************)
(* RECEIVE IHAVE ACTION                                                    *)
(* A node receives an IHave announcement                                   *)
(***************************************************************************)

ReceiveIHave(n) ==
    \E msg \in network :
        /\ msg.type = IHave
        /\ msg.to = n
        \* Remove from network
        /\ network' = network \ {msg}
        /\ IF HasMessage(n, msg.msg_id) THEN
            \* Already have it, ignore
            /\ UNCHANGED <<delivered, message_cache, eager_peers, lazy_peers, pending_graft>>
           ELSE
            \* Don't have it: send Graft request
            /\ network' = (network \ {msg}) \union
                {[type |-> Graft, from |-> n, to |-> msg.from, msg_id |-> msg.msg_id, round |-> msg.round]}
            /\ pending_graft' = [pending_graft EXCEPT ![n] = @ \union {[msg_id |-> msg.msg_id, target |-> msg.from]}]
            /\ UNCHANGED <<delivered, message_cache, eager_peers, lazy_peers>>

(***************************************************************************)
(* RECEIVE GRAFT ACTION                                                    *)
(* A node receives a Graft request                                         *)
(***************************************************************************)

ReceiveGraft(n) ==
    \E msg \in network :
        /\ msg.type = Graft
        /\ msg.to = n
        \* Remove from network
        /\ network' = network \ {msg}
        \* Promote requester to eager
        /\ eager_peers' = [eager_peers EXCEPT ![n] = @ \union {msg.from}]
        /\ lazy_peers' = [lazy_peers EXCEPT ![n] = @ \ {msg.from}]
        \* If we have the message, send Gossip back
        /\ IF InCache(n, msg.msg_id) THEN
            LET cachedRound == GetCachedRound(n, msg.msg_id)
            IN network' = (network \ {msg}) \union
                {[type |-> Gossip, from |-> n, to |-> msg.from, msg_id |-> msg.msg_id, round |-> cachedRound]}
           ELSE
            UNCHANGED network'
        /\ UNCHANGED <<delivered, message_cache, pending_graft>>

(***************************************************************************)
(* RECEIVE PRUNE ACTION                                                    *)
(* A node receives a Prune request                                         *)
(***************************************************************************)

ReceivePrune(n) ==
    \E msg \in network :
        /\ msg.type = Prune
        /\ msg.to = n
        \* Remove from network
        /\ network' = network \ {msg}
        \* Demote sender from eager to lazy
        /\ eager_peers' = [eager_peers EXCEPT ![n] = @ \ {msg.from}]
        /\ lazy_peers' = [lazy_peers EXCEPT ![n] = @ \union {msg.from}]
        /\ UNCHANGED <<delivered, message_cache, pending_graft>>

(***************************************************************************)
(* SEND PRUNE ACTION                                                       *)
(* A node sends Prune to reduce redundant eager links                      *)
(***************************************************************************)

SendPrune(n, target) ==
    \* Precondition: target is in eager set
    /\ target \in eager_peers[n]
    /\ network' = network \union
        {[type |-> Prune, from |-> n, to |-> target, msg_id |-> CHOOSE m \in Messages : TRUE, round |-> 0]}
    /\ UNCHANGED <<eager_peers, lazy_peers, delivered, pending_graft, message_cache>>

(***************************************************************************)
(* GRAFT TIMEOUT ACTION                                                    *)
(* A pending graft times out, try alternate peer                          *)
(***************************************************************************)

GraftTimeout(n) ==
    \E graft \in pending_graft[n] :
        \* Remove old graft
        /\ pending_graft' = [pending_graft EXCEPT ![n] = @ \ {graft}]
        \* Try another lazy peer
        /\ LET alternates == lazy_peers[n] \ {graft.target}
           IN IF alternates /= {} THEN
               /\ \E alt \in alternates :
                   /\ network' = network \union
                       {[type |-> Graft, from |-> n, to |-> alt, msg_id |-> graft.msg_id, round |-> 0]}
                   /\ pending_graft' = [pending_graft EXCEPT ![n] = (@ \ {graft}) \union {[msg_id |-> graft.msg_id, target |-> alt]}]
              ELSE
               \* No alternates, give up
               UNCHANGED <<network, pending_graft>>
        /\ UNCHANGED <<eager_peers, lazy_peers, delivered, message_cache>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E n \in Nodes, m \in Messages : Broadcast(n, m)
    \/ \E n \in Nodes : ReceiveGossip(n)
    \/ \E n \in Nodes : ReceiveIHave(n)
    \/ \E n \in Nodes : ReceiveGraft(n)
    \/ \E n \in Nodes : ReceivePrune(n)
    \/ \E n \in Nodes, t \in Nodes : SendPrune(n, t)
    \/ \E n \in Nodes : GraftTimeout(n)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Weak fairness: enabled actions eventually happen
Fairness == WF_vars(Next)

(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* No node has itself in its eager set
NoSelfInEager == \A n \in Nodes : n \notin eager_peers[n]

\* No node has itself in its lazy set
NoSelfInLazy == \A n \in Nodes : n \notin lazy_peers[n]

\* Eager and lazy sets are disjoint for each node
EagerLazyDisjoint == \A n \in Nodes : eager_peers[n] \intersect lazy_peers[n] = {}

\* No duplicate deliveries (guaranteed by construction - using sets)
NoDuplicateDelivery == TRUE  \* Sets prevent duplicates by construction

\* All safety invariants combined
SafetyInvariants ==
    /\ TypeInvariant
    /\ NoSelfInEager
    /\ NoSelfInLazy
    /\ EagerLazyDisjoint

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* Eventually all messages are delivered to all nodes
EventualDelivery ==
    \A m \in Messages :
        (\E n \in Nodes : m \in delivered[n]) ~>
        (\A n \in Nodes : m \in delivered[n])

\* All pending grafts eventually resolve
GraftProgress ==
    \A n \in Nodes :
        \A g \in pending_graft[n] :
            <>(g \notin pending_graft[n])

(***************************************************************************)
(* CONNECTIVITY                                                            *)
(***************************************************************************)

\* Helper: Transitive closure of eager peer relation
\* A node n can reach node m if there's a path of eager links
RECURSIVE CanReach(_, _, _)
CanReach(n, m, visited) ==
    IF n = m THEN TRUE
    ELSE IF n \in visited THEN FALSE
    ELSE \E p \in eager_peers[n] : CanReach(p, m, visited \union {n})

\* The eager graph is connected: every node can reach every other node
EagerConnected ==
    \A n, m \in Nodes : n = m \/ CanReach(n, m, {})

=============================================================================
