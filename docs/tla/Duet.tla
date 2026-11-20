------------------------------- MODULE Duet -------------------------------
(***************************************************************************)
(* The part of duet that decides who may write and what counts as          *)
(* written, at the level of single block reads and writes on the shared    *)
(* disk.  It follows pkg/lease (ballot, survey, retreat, fence) and the    *)
(* reader in pkg/node.                                                     *)
(*                                                                         *)
(* STATUS: written, not yet run through TLC.  See README.md here.          *)
(*                                                                         *)
(* What is modelled: the epoch table, one Disk Paxos block per node, the   *)
(* log of each epoch as a count of records, an owner that writes a record  *)
(* and then confirms it, challengers that may start a ballot at any time,  *)
(* crashes at any step, and a reader that follows the log.                 *)
(*                                                                         *)
(* What is left out, on purpose: clocks and leases.  In the design they    *)
(* only decide when a challenger may start, so here a challenger may       *)
(* start whenever it likes, which covers every schedule a broken clock     *)
(* could produce.  Also left out: torn blocks, log trimming, the layout    *)
(* of epochs within one log region (rule 7), and checkpoints.              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes,      \* node ids, natural numbers 1..N
          Readers,    \* ids of log followers
          MaxEpoch,   \* epochs 1..MaxEpoch
          MaxRec,     \* records per epoch
          MaxRound,   \* ballots a node may start per run of the model
          SlotLast,   \* TRUE: look for a later epoch's slot after the ballot
                      \* blocks (the code since the fix); FALSE: before them
          TableCheck  \* TRUE: a ballot consults the epoch table after its
                      \* phase 1 survey (the code since the fix)

N == Cardinality(Nodes)
None == [owner |-> 0, cut |-> 0]
Empty == [inst |-> 0, mbal |-> 0, bal |-> 0, inp |-> None]

VARIABLES
  slot,     \* epoch table: slot[e] is None or [owner, cut], cut being how
            \* many records of epoch e-1 belong to the log
  pax,      \* pax[n]: node n's ballot block
  len,      \* len[e]: records of epoch e on disk
  acked,    \* acked[e]: records of epoch e acknowledged to a client
  pc, ep, own, best, todo, val, wpos, rounds,   \* per node
  rpc, rep, rsnap, rtodo, applied                \* per reader

vars == <<slot, pax, len, acked, pc, ep, own, best, todo, val, wpos, rounds,
          rpc, rep, rsnap, rtodo, applied>>

Epochs == 1..MaxEpoch
SlotAt(e) == IF e \in Epochs THEN slot[e] ELSE None
Latest == IF \E e \in Epochs : slot[e] # None
            THEN CHOOSE e \in Epochs : slot[e] # None /\ SlotAt(e+1) = None
            ELSE 0

Init ==
  /\ slot = [e \in Epochs |-> IF e = 1 THEN [owner |-> CHOOSE n \in Nodes : TRUE, cut |-> 0] ELSE None]
  /\ pax = [n \in Nodes |-> Empty]
  /\ len = [e \in Epochs |-> 0]
  /\ acked = [e \in Epochs |-> 0]
  /\ pc = [n \in Nodes |-> "idle"]
  /\ ep = [n \in Nodes |-> 0]
  /\ own = [n \in Nodes |-> Empty]
  /\ best = [n \in Nodes |-> Empty]
  /\ todo = [n \in Nodes |-> {}]
  /\ val = [n \in Nodes |-> None]
  /\ wpos = [n \in Nodes |-> 0]
  /\ rounds = [n \in Nodes |-> 0]
  /\ rpc = [r \in Readers |-> "read"]
  /\ rep = [r \in Readers |-> 1]
  /\ rsnap = [r \in Readers |-> 0]
  /\ rtodo = [r \in Readers |-> {}]
  /\ applied = [r \in Readers |-> [e \in Epochs |-> 0]]

nodeVars == <<pc, ep, own, best, todo, val, wpos, rounds>>
readerVars == <<rpc, rep, rsnap, rtodo, applied>>

----------------------------------------------------------------------------
(* Challenger: lease.ballot                                                *)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

\* Phase 1 write.  Reading the table and the ballot numbers in one step is a
\* simplification; a ballot number only has to be new and unique.
StartBallot(n) ==
  /\ pc[n] = "idle" /\ rounds[n] < MaxRound
  /\ LET inst == Latest + 1
         mine == IF pax[n].inst = inst THEN pax[n] ELSE [Empty EXCEPT !.inst = inst]
         top  == Max({mine.mbal} \cup {pax[j].mbal : j \in {k \in Nodes : pax[k].inst = inst}})
         b    == [mine EXCEPT !.mbal = ((top \div N) + 1) * N + n]
     IN /\ inst \in Epochs
        /\ pax' = [pax EXCEPT ![n] = b]
        /\ own' = [own EXCEPT ![n] = b]
        /\ best' = [best EXCEPT ![n] = b]
        /\ ep' = [ep EXCEPT ![n] = inst]
  /\ pc' = [pc EXCEPT ![n] = "p1"]
  /\ todo' = [todo EXCEPT ![n] = Nodes \ {n}]
  /\ rounds' = [rounds EXCEPT ![n] = @ + 1]
  /\ UNCHANGED <<slot, len, acked, val, wpos, readerVars>>

\* retreat: wipe the block, but only if nothing was ever accepted under it.
Retreat(n) == IF own[n].bal = 0 THEN [pax EXCEPT ![n] = Empty] ELSE pax

\* survey, one block per step.
Survey(n, j) ==
  /\ pc[n] \in {"p1", "p2"} /\ j \in todo[n]
  /\ LET b == pax[j]
         fail == b.inst > ep[n] \/ (b.inst = ep[n] /\ b.mbal > own[n].mbal)
     IN IF fail
          THEN /\ pc' = [pc EXCEPT ![n] = "idle"]
               /\ pax' = IF pc[n] = "p1" THEN Retreat(n) ELSE pax
               /\ UNCHANGED <<best, todo>>
          ELSE /\ best' = [best EXCEPT ![n] = IF b.inst = ep[n] /\ b.bal > @.bal THEN b ELSE @]
               /\ todo' = [todo EXCEPT ![n] = @ \ {j}]
               /\ UNCHANGED <<pc, pax>>
  /\ UNCHANGED <<slot, len, acked, ep, own, val, wpos, rounds, readerVars>>

\* After a clean phase 1 survey the challenger may still withdraw (the old
\* owner's lease block moved, or reading the log failed).
Withdraw(n) ==
  /\ pc[n] = "p1" /\ todo[n] = {}
  /\ pax' = Retreat(n)
  /\ pc' = [pc EXCEPT ![n] = "idle"]
  /\ UNCHANGED <<slot, len, acked, ep, own, best, todo, val, wpos, rounds, readerVars>>

\* Phase 2 write.  The cutoff is read from the log here, after the phase 1
\* block is on disk.
Accept(n) ==
  /\ pc[n] = "p1" /\ todo[n] = {}
  /\ ~TableCheck \/ slot[ep[n]] = None
  /\ LET v == IF best[n].bal = 0
                THEN [owner |-> n, cut |-> IF ep[n] = 1 THEN 0 ELSE len[ep[n] - 1]]
                ELSE best[n].inp
         b == [own[n] EXCEPT !.bal = own[n].mbal, !.inp = v]
     IN /\ val' = [val EXCEPT ![n] = v]
        /\ own' = [own EXCEPT ![n] = b]
        /\ pax' = [pax EXCEPT ![n] = b]
  /\ pc' = [pc EXCEPT ![n] = "p2"]
  /\ todo' = [todo EXCEPT ![n] = Nodes \ {n}]
  /\ UNCHANGED <<slot, len, acked, ep, best, wpos, rounds, readerVars>>

\* The instance turns out to be decided already: the table says so, though
\* no ballot block did.  Withdraw and go by the table.
Decided(n) ==
  /\ TableCheck /\ pc[n] = "p1" /\ todo[n] = {} /\ slot[ep[n]] # None
  /\ pax' = Retreat(n)
  /\ pc' = [pc EXCEPT ![n] = IF slot[ep[n]].owner = n THEN "own" ELSE "idle"]
  /\ UNCHANGED <<slot, len, acked, ep, own, best, todo, val, wpos, rounds, readerVars>>

\* Chosen: record it in the table if nobody has, and become owner if the
\* table names this node.  Before the fix a node went by its own value even
\* when the table already held another.
WriteSlot(n) ==
  /\ pc[n] = "p2" /\ todo[n] = {}
  /\ LET final == IF slot[ep[n]] = None \/ ~TableCheck THEN val[n] ELSE slot[ep[n]]
     IN /\ slot' = IF slot[ep[n]] = None THEN [slot EXCEPT ![ep[n]] = val[n]] ELSE slot
        /\ val' = [val EXCEPT ![n] = final]
        /\ pc' = [pc EXCEPT ![n] = IF final.owner = n THEN "own" ELSE "idle"]
  /\ UNCHANGED <<pax, len, acked, ep, own, best, todo, wpos, rounds, readerVars>>

\* A node finds the table naming it as owner of the latest epoch and takes
\* it up: its ballot was finished by another node, or its lease lapsed and
\* it resumes.  (The code also requires a clean renewal first; leaving
\* that out only adds behaviours.)
TakeUp(n) ==
  /\ pc[n] = "idle" /\ Latest > 0 /\ slot[Latest].owner = n
  /\ pc' = [pc EXCEPT ![n] = "own"]
  /\ ep' = [ep EXCEPT ![n] = Latest]
  /\ UNCHANGED <<slot, pax, len, acked, own, best, todo, val, wpos, rounds, readerVars>>

----------------------------------------------------------------------------
(* Owner: write one batch, flush, then lease.Confirm (fence)               *)

Write(n) ==
  /\ pc[n] = "own" /\ len[ep[n]] < MaxRec
  /\ len' = [len EXCEPT ![ep[n]] = @ + 1]
  /\ wpos' = [wpos EXCEPT ![n] = len[ep[n]] + 1]
  /\ pc' = [pc EXCEPT ![n] = IF SlotLast THEN "cf_pax" ELSE "cf_slot"]
  /\ todo' = [todo EXCEPT ![n] = Nodes \ {n}]
  /\ UNCHANGED <<slot, pax, acked, ep, own, best, val, rounds, readerVars>>

Ack(n) == [acked EXCEPT ![ep[n]] = IF wpos[n] > @ THEN wpos[n] ELSE @]

CfSlot(n) ==
  /\ pc[n] = "cf_slot"
  /\ IF SlotAt(ep[n] + 1) # None
       THEN pc' = [pc EXCEPT ![n] = "idle"] /\ UNCHANGED acked
       ELSE IF SlotLast
              THEN pc' = [pc EXCEPT ![n] = "own"] /\ acked' = Ack(n)
              ELSE pc' = [pc EXCEPT ![n] = "cf_pax"] /\ UNCHANGED acked
  /\ UNCHANGED <<slot, pax, len, ep, own, best, todo, val, wpos, rounds, readerVars>>

CfPax(n, j) ==
  /\ pc[n] = "cf_pax" /\ j \in todo[n]
  /\ IF pax[j].inst > ep[n] /\ pax[j].mbal > 0
       THEN pc' = [pc EXCEPT ![n] = "idle"] /\ UNCHANGED <<todo, acked>>
       ELSE /\ todo' = [todo EXCEPT ![n] = @ \ {j}]
            /\ IF todo[n] = {j}
                 THEN IF SlotLast
                        THEN pc' = [pc EXCEPT ![n] = "cf_slot"] /\ UNCHANGED acked
                        ELSE pc' = [pc EXCEPT ![n] = "own"] /\ acked' = Ack(n)
                 ELSE UNCHANGED <<pc, acked>>
  /\ UNCHANGED <<slot, pax, len, ep, own, best, val, wpos, rounds, readerVars>>

\* The process dies at any point.  What it wrote stays on the disk.
Crash(n) ==
  /\ pc[n] # "idle"
  /\ pc' = [pc EXCEPT ![n] = "idle"]
  /\ UNCHANGED <<slot, pax, len, acked, ep, own, best, todo, val, wpos, rounds, readerVars>>

----------------------------------------------------------------------------
(* Reader: read records, then look for any sign of a later epoch           *)

RRead(r) ==
  /\ rpc[r] = "read" /\ slot[rep[r]] # None
  /\ rsnap' = [rsnap EXCEPT ![r] = len[rep[r]]]
  /\ rpc' = [rpc EXCEPT ![r] = IF SlotLast THEN "pax" ELSE "slot"]
  /\ rtodo' = [rtodo EXCEPT ![r] = Nodes]
  /\ UNCHANGED <<slot, pax, len, acked, nodeVars, rep, applied>>

RApply(r, k) == [applied EXCEPT ![r][rep[r]] = IF k > @ THEN k ELSE @]
Min(a, b) == IF a < b THEN a ELSE b

RSlot(r) ==
  /\ rpc[r] = "slot"
  /\ LET nx == SlotAt(rep[r] + 1)
     IN IF nx # None
          THEN /\ applied' = RApply(r, Min(rsnap[r], nx.cut))
               /\ rep' = [rep EXCEPT ![r] = @ + 1]
               /\ rpc' = [rpc EXCEPT ![r] = "read"]
          ELSE IF SlotLast
                 THEN applied' = RApply(r, rsnap[r]) /\ rpc' = [rpc EXCEPT ![r] = "read"] /\ UNCHANGED rep
                 ELSE rpc' = [rpc EXCEPT ![r] = "pax"] /\ UNCHANGED <<applied, rep>>
  /\ UNCHANGED <<slot, pax, len, acked, nodeVars, rsnap, rtodo>>

RPax(r, j) ==
  /\ rpc[r] = "pax" /\ j \in rtodo[r]
  /\ IF pax[j].inst > rep[r] /\ pax[j].mbal > 0
       THEN rpc' = [rpc EXCEPT ![r] = "read"] /\ UNCHANGED <<rtodo, applied>>  \* wait, read again
       ELSE /\ rtodo' = [rtodo EXCEPT ![r] = @ \ {j}]
            /\ IF rtodo[r] = {j}
                 THEN IF SlotLast
                        THEN rpc' = [rpc EXCEPT ![r] = "slot"] /\ UNCHANGED applied
                        ELSE rpc' = [rpc EXCEPT ![r] = "read"] /\ applied' = RApply(r, rsnap[r])
                 ELSE UNCHANGED <<rpc, applied>>
  /\ UNCHANGED <<slot, pax, len, acked, nodeVars, rep, rsnap>>

----------------------------------------------------------------------------
Next ==
  \/ \E n \in Nodes : StartBallot(n) \/ Withdraw(n) \/ Accept(n) \/ Decided(n) \/ WriteSlot(n)
                      \/ TakeUp(n) \/ Write(n) \/ CfSlot(n) \/ Crash(n)
  \/ \E n \in Nodes, j \in Nodes : Survey(n, j) \/ CfPax(n, j)
  \/ \E r \in Readers : RRead(r) \/ RSlot(r)
  \/ \E r \in Readers, j \in Nodes : RPax(r, j)

Spec == Init /\ [][Next]_vars

\* An acknowledged write is never cut off by a later epoch.
AckedSurvive == \A e \in Epochs : SlotAt(e + 1) # None => acked[e] <= slot[e + 1].cut

\* A reader never applies a record that a later epoch cuts off.
ReadersAgree == \A r \in Readers, e \in Epochs : SlotAt(e + 1) # None => applied[r][e] <= slot[e + 1].cut

\* A node acts as owner of an epoch only if the table names it.
OneOwner == \A n \in Nodes :
  pc[n] \in {"own", "cf_slot", "cf_pax"} => slot[ep[n]] # None /\ slot[ep[n]].owner = n
=============================================================================
