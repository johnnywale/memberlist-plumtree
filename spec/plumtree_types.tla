---------------------------- MODULE plumtree_types ----------------------------
(***************************************************************************)
(* Type definitions and helper operators for the Plumtree specification.   *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* Message Types                                                           *)
(***************************************************************************)

(* Message type tags *)
CONSTANTS Gossip, IHave, Graft, Prune

MessageTypes == {Gossip, IHave, Graft, Prune}

(***************************************************************************)
(* Network Message Structure                                               *)
(*                                                                         *)
(* A network message is a record with:                                     *)
(*   - type: The message type (Gossip, IHave, Graft, Prune)               *)
(*   - from: The sender node                                               *)
(*   - to: The recipient node                                              *)
(*   - msg_id: The message ID being referenced                            *)
(*   - round: The gossip round number (for Gossip and IHave)              *)
(***************************************************************************)

NetworkMessage(type, from, to, msg_id, round) ==
    [type |-> type, from |-> from, to |-> to, msg_id |-> msg_id, round |-> round]

(***************************************************************************)
(* Graft Request Status                                                    *)
(***************************************************************************)

CONSTANTS Pending, Succeeded, Failed

GraftStatus == {Pending, Succeeded, Failed}

(***************************************************************************)
(* Helper Operators                                                        *)
(***************************************************************************)

(* Check if a set is non-empty *)
NonEmpty(S) == S /= {}

(* Get the other nodes excluding self *)
OtherNodes(n, Nodes) == Nodes \ {n}

(* Random element from a set (non-deterministic choice) *)
CHOOSE_FROM(S) == CHOOSE x \in S : TRUE

(* Minimum of two natural numbers *)
Min(a, b) == IF a < b THEN a ELSE b

(* Maximum of two natural numbers *)
Max(a, b) == IF a > b THEN a ELSE b

(* Count elements in a set satisfying a predicate *)
CountWhere(S, P(_)) == Cardinality({x \in S : P(x)})

(***************************************************************************)
(* Set Operations                                                          *)
(***************************************************************************)

(* Symmetric difference of two sets *)
SymDiff(A, B) == (A \ B) \union (B \ A)

(* Check if two sets are disjoint *)
Disjoint(A, B) == A \intersect B = {}

(***************************************************************************)
(* Sequence Operations                                                     *)
(***************************************************************************)

(* Check if element is in sequence *)
InSeq(x, seq) == \E i \in DOMAIN seq : seq[i] = x

(* Convert sequence to set *)
SeqToSet(seq) == {seq[i] : i \in DOMAIN seq}

=============================================================================
