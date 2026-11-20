# Design

## Goal and constraints

Run the Kubernetes datastore on control plane VMs that share one
vSAN-backed disk, with no arbiter beyond what the platform already runs.

- Minimum two nodes. More nodes are warm readers and takeover candidates.
- The only tiebreaker is vSAN's own quorum (two hosts plus its witness).
- No BMC or power fencing, no external database, no object store.
- Kubernetes sees ordinary etcd.
- Scale target is the published GKE and EKS envelope, not just edge
  clusters. See [Scaling path](#scaling-path) for what that requires and
  what is unproven.

Guarantees that must hold under any failure, including partitions, VM
stuns and process pauses:

1. At most one node gets writes acknowledged in any epoch.
2. An acknowledged write is never lost and never reordered.
3. No two nodes ever apply different records at the same index.

Availability inherits vSAN's. If vSAN loses quorum for the disk, the
datastore is down until vSAN recovers.

## Disk layout

```
block 0              superblock: node count, epoch slots, log size, TTL, max batch
blocks 1 .. N        lease block per node
blocks N+1 .. 2N     Disk Paxos block per node
next E blocks        epoch table: slot e-1 describes epoch e
next L bytes         log
last 2N slots        checkpoints: two per node (optional)
```

Metadata blocks are 4096 bytes with a CRC. A node writes only its own lease
and Paxos blocks. Anyone may write an epoch table slot, but only with the
value the ballot chose, so the write is idempotent.

An epoch table entry says who owns the epoch, the index and offset of its
first record, and the cutoff: the offset where the previous epoch's valid
records end.

Log records are `{epoch, index, length, crc, payload}`. Each batch is one
write that starts on a 4096-byte boundary and is padded to a whole number
of blocks, so every write is block aligned (direct I/O requires it) and no
two writers ever touch the same block. A reader accepts a
record only if it has the epoch and index expected at that position.

## Protocol

**Rule 1, single writer.** Only the owner of the latest epoch appends.

**Rule 2, epochs are chosen by Disk Paxos.** To take over, a node runs one
ballot on the per-node Paxos blocks for instance `e+1`. The value chosen is
the epoch table entry. Racing nodes get one winner.

**Rule 3, wait before challenging.** A node starts a ballot only when the
owner released the lease, or the owner's lease block has stayed unchanged
for `TTL * (1 + drift)` on the challenger's clock, measured from after the
read that first showed that value.

**Rule 4, write then read, on both sides.**
- The owner renews by writing its lease block and then reading every other
  node's Paxos block and the next epoch slot. The renewal counts only if it
  saw no ballot for a later epoch. The lease is then valid for
  `TTL * (1 - drift)` from the moment before the renewal write was issued.
- The challenger writes its Paxos block and then reads the owner's lease
  block again. If it changed, the challenger withdraws.

Because each side writes before it reads, at least one sees the other.

**Rule 5, cutoff after exclusion.** The challenger scans the old epoch's
tail to find the cutoff only after its Paxos block is on disk and the
re-read in rule 4 passed. From that point the old owner can acknowledge
nothing new.

**Rule 6, confirm after write.** After a batch is flushed, the owner reads
the other nodes' ballot blocks and the next epoch slot. It acknowledges the
batch only if there is no sign of a later epoch. This read uses no clock.

**Rule 7, gap between epochs.** A new epoch starts on the block boundary
one maximum batch past the cutoff. A writer has at most one batch in flight, and it lands within
one batch of the cutoff, so a deposed writer's last write cannot touch the
new epoch.

**Rule 8, readers look twice.** A reader reads records first, then checks
for any sign of a later epoch (a slot, or a ballot block). If there is one,
it applies nothing until the slot appears and then applies only up to the
cutoff. A record beyond a cutoff can only reach the disk after the ballot
that fixes the cutoff began, so a reader that saw the record also sees the
ballot.

**Rule 9, never overwrite.** An owner that resumes after its lease lapsed
first applies every valid record of its own epoch already on disk, and
continues after them. Other nodes may have applied them.

### Why an acknowledged write survives

Take an owner A and a challenger B that completes a ballot. B wrote its
ballot block, and only afterwards scanned the log for the cutoff (rule 5).
A wrote its batch, and only afterwards read the ballot blocks (rule 6).
Either A's read came before B's ballot block was written, in which case A's
batch was on disk before B's scan and is inside the cutoff; or A's read saw
the ballot and A did not acknowledge. No clock appears in this argument.

### What the lease is for

Leases and clocks decide when a challenger may start a ballot (rule 3) and
let an owner stop early without a disk read. They affect how often
ownership changes needlessly. They are not what makes an acknowledgement
safe.

This was not the original design. Rule 6 first checked the lease by the
local clock. Lab run 3 on Google Cloud suspended the owner's VM and resumed
it: the guest's clock had not moved, so by its own clock the lease was
still good, and it accepted a write after the other node had taken over.
The simulation had only modelled pauses during which the paused node's
clock kept running. It now models clocks that stop.

### Assumptions

| # | Assumption | Status |
| --- | --- | --- |
| A1 | Reads and writes on the shared disk are linearizable across nodes | Unverified on vSAN multi-writer disks |
| A2 | A 4096-byte write is atomic, or a torn write fails its CRC | Standard for block devices; unverified here |
| A3 | A flush returning means the data survives what vSAN is configured to tolerate | Unverified for multi-writer disks |
| A4 | Two nodes' clocks differ in rate by less than `drift` over one lease period | Needed only to avoid needless ownership changes. Safety does not depend on it, which matters because a suspended VM's clock stops. |
| A5 | A writer has at most one batch in flight | True of the code |
| A6 | One process per node id at a time | Operational; not enforced |
| A7 | A crashed process's in-flight write is at most one batch | True of the code; kernel behaviour after process death to be checked |

A1 to A3 are the reason milestone 0 comes first.

## Checkpoints

A checkpoint is a copy of the state machine (etcd's database file) as of
one log index, plus the log position that follows it. A starting node
loads the newest intact checkpoint and replays only the log after it.

- **Every node writes its own.** Each node has two checkpoint slots and
  writes only to those. No agreement is needed, because a checkpoint adds
  no information: nodes apply only acknowledged records, and the same log
  gives the same state. A checkpoint by a node that has since been deposed
  is still a correct state for its index. A follower can do the work, so
  the owner does not have to.
- **A crash cannot lose the previous one.** A node overwrites the older of
  its two slots: it clears that slot's header, writes the state, then
  writes the header with the state's checksum.
- **Readers trust nothing.** A reader takes the highest index among all
  headers, copies the state, and then checks the checksum and that the
  header has not changed meanwhile. If either fails, it tries the next
  newest, and with none left it replays the log from the start.
- **The snapshot is taken between applies** on the node's single loop, so
  the state and the log position describe the same moment. The copy to
  disk runs in the background from a database snapshot.

A state larger than a checkpoint slot cannot be checkpointed; the node
logs the failure and carries on. On Kubernetes-sized state the copy will
compete with the log for disk throughput, and that has not been measured.

## Trimming (design; not implemented)

The log has to be reused, or it fills. The plan is a circular log:
positions stay as ever-growing logical offsets in epoch records, cursors
and checkpoints, and map to the disk modulo the log size. Reuse creates
two hazards that the linear log does not have.

**Hazard 1: a slow reader is overtaken.** The writer overwrites records a
lagging or restarting node has not read yet.

- A node publishes a *head* in its own lease block: a logical offset with
  the promise that a durable checkpoint exists at or beyond it. Heads only
  grow. The effective head is the largest one in any lease block.
- The writer may overwrite a position only after publishing a head beyond
  it, in that order.
- A reader reads records first and the heads second. If the head has
  passed the point it read from, it throws the records away and restarts
  from a checkpoint at or beyond the head. This is the same read-then-look
  order that already protects readers from a takeover.
- Stale data from the previous lap can never be mistaken for current
  data: a record is accepted only with the exact epoch and index expected
  at its position, and indexes never repeat.

**Hazard 2: a deposed writer's late write lands on live data.** Today a
deposed owner can still issue one write, and it is harmless because it
lands in a gap after its epoch's cutoff that nobody ever reads or reuses.
In a circular log that gap is reused one lap later. A node that was
suspended just before the write, for longer than a lap takes, would wake
up and destroy acknowledged records. No timeout can rule this out, for
the reason found in lab run 3: a suspended node's clock stops.

- The rule: the gap after epoch E's cutoff may not be reused until the
  owner of E is known to have stopped writing for E. The evidence is in
  that node's own lease block: it shows a newer incarnation (the process
  restarted), a newer epoch, or a mark the node writes when it learns it
  was deposed. A node is one loop, so once it has written that mark its
  earlier write has returned.
- The writer therefore stops at `min(head, cutoff of the oldest epoch
  whose owner has not retired) + log size`. If it cannot advance, writes
  fail with "log full", as today.

**A deposed owner that never comes back.** If its host is destroyed,
nothing on the disk can prove it will not write again, and the log cannot
wrap past its gap. Something outside the disk has to say the node is
gone. The decision:

1. **The node's own mark covers every case where it runs again.** vSphere
   HA restarting the VM, a reboot, a VKS node replaced in place, a
   suspended VM resuming: each ends with the node writing its lease block
   under a new incarnation or with the deposed mark.
2. **For a node that never runs again, vCenter is the authority, and it
   is asked off the failover path.** A small controller confirms through
   vCenter that the VM is powered off or deleted and then writes the
   retire mark for that node (`duet retire`). In VKS the natural hook is
   Cluster API machine deletion, which completes only once vCenter has
   removed the VM. This is the check Pacemaker's `fence_vmware_rest`
   makes, with one difference that matters: **takeover never waits for
   it.** Only reclaiming log space does. In a two-site failure vCenter may
   be down or on the wrong side; the surviving node still takes over in
   seconds and keeps writing into the space the log has left.
3. **The log is sized for that wait.** Free space must cover the longest
   time vCenter may be unreachable. At 1,000 writes a second of 1 KiB
   that is about 86 GiB a day.

Why this and not the alternatives:

- **What the cloud designs do** is fence at the storage layer: the
  journal behind EKS, MemoryDB and Aurora refuses an append from a writer
  with a stale epoch. That is the right answer and vSAN does not offer
  it: a multi-writer disk accepts any attached VM's write, and SCSI-3
  reservations on vSAN are supported for Windows failover clustering
  only. This is recorded in [VCF-GAPS.md](VCF-GAPS.md); if the platform
  gains it, step 2 disappears.
- **Spanner's answer is bounded clocks.** Lab run 3 showed a VM's clock
  cannot carry that weight: it stops while the VM is suspended.
- **vSAN already fences the common two-site case.** A site cut off from
  both the other site and the witness loses access to its objects and its
  VMs are powered off, so a deposed owner there cannot write, and when it
  returns it does so as a restarted VM, which is case 1.
- **Suspending a VM with a multi-writer disk is unsupported** in vSphere,
  which narrows the long-pause hazard on VCF. It does not remove it (a
  host can stall), so the rule stays.
- **It scales with partitions.** Retirement is per node, not per log: one
  vCenter answer retires that node in every partition's log.

**Epoch table.** It stays finite. Reusing slots has the same late-writer
hazard for ballot participants and is deferred. One slot is used per
ownership change, so the limit is the number of failovers over the life
of the disk: 65,536 slots cost 256 MiB.

**Open points before coding.**

- A reader that must restart from a checkpoint while the only checkpoint
  at or beyond the head is being replaced has to wait for the new one.
- The head must be re-read by a restarting node from all lease blocks
  before it writes its own, or heads could go backwards.
- This changes rules that the simulation and the mutation tests cover.
  It gets implemented in the simulation first, with wrap-around in the
  random schedules, before the binary uses it.

## Decisions made

| Decision | Value | Reason |
| --- | --- | --- |
| Recovery target | 15 s from owner failure to first acknowledged write on the survivor, 99th percentile | Well under a VM restart by vSphere HA; achievable with a short lease. To be revised once accounts state a number. |
| Lease length | 5 s, renewed every 1 s, `drift` 0.1 | Takeover wait of 5.5 s leaves room for detection, the ballot and replay inside the target. To be re-derived from measured vSAN flush latency. |
| Milestone 1 gate | If stock etcd under Pacemaker on a shared disk meets the same 15 s target and can be supported, stop and publish that | Agreed with the project owner, conditional on support |
| Build order | Protocol in simulation first, then lab stand-ins, vSAN last | No vSAN lab is available yet |
| Language, licence | Go, Apache-2.0 | etcd's API, storage packages and test tools are Go |

## Scaling path

GKE reached 130,000 nodes by putting the etcd API on Spanner. EKS reached
100,000 by keeping etcd, removing Raft, and adding three things. duet has
the same shape as EKS, so the EKS list is the plan:

| EKS change | duet equivalent | State |
| --- | --- | --- |
| Consensus moved to a journal | The shared disk is the journal | Implemented in simulation |
| Replicas no longer bound by quorum | Any number of tailing readers | Implemented, tested with three |
| In-memory database | State machine in memory, rebuilt from checkpoint plus log | Implemented on etcd's database file in a state directory; put it on tmpfs to hold it in memory |
| Key-space partitioning | One duet instance per resource group, each with its own disk and owner, wired through kube-apiserver's per-resource etcd overrides | Design only |
| (implicit) batching | Group commit: one flush per batch | Implemented |

Benchmark targets, taken from what the two providers published:

- 13,000 lease updates per second and over one million objects (GKE at 130,000 nodes).
- 20 GB in one partition; 32 GB and over ten million objects across partitions (EKS).

What stands between this repo and those numbers:

- **Flush latency sets the ceiling.** Write throughput per partition is
  flushes per second times records per flush. Nobody has measured a vSAN
  multi-writer disk for this workload.
- **One writer per partition.** Partitions spread writers across nodes, but
  a single hot resource type is still one writer. Spanner does not have
  this limit; Journal-based EKS does.
- **The log is not trimmed and the epoch table is finite.** Checkpoints
  exist; trimming and epoch slot reuse are still required before any
  long run.
- **Most of the scaling work at 100,000 nodes is outside the datastore**:
  consistent reads from the API server's cache, streaming lists, scheduler
  throughput. Those are upstream Kubernetes features and need a large test
  rig to exercise.
- **Limits on how many VMs can share one disk** are unverified.

None of this can be shown from a laptop. The repo can show the protocol is
correct and measure per-partition throughput on real storage; a claim of
GKE or EKS scale needs a load test at that scale.

## Milestones

| # | Milestone | Exit criteria |
| --- | --- | --- |
| 0 | Platform spike on a two-host vSAN cluster: A1 to A3, flush latency, reservation commands from Linux | Results table; go or no-go per ownership backend |
| 1 | Baselines on the same disk: vSphere HA restart, Pacemaker with stock etcd | Recovery numbers; gate decision |
| 2 | Ownership and log | **Done in simulation.** TLA+ model still to write. |
| 3 | State machine and etcd API; checkpoints and trimming | kube-apiserver runs; upstream storage tests pass. **State: API, state machine and checkpoints done; most upstream storage tests pass and kube-apiserver v1.37.2 runs through an owner kill (`test/kube`); no trimming; followers do not serve.** |
| 4 | Correctness harness on real processes and disks | Linearizability checker passes under faults |
| 5 | Kubernetes demo on the lab and on vSAN | Conformance; recovery benchmark against milestone 1 |
| 6 | Scale: partitions, in-memory state, load tests against the targets above | Measured throughput per partition; gap to targets stated |

Until a vSAN lab exists, milestones 0 and 1 run on the Google Cloud
stand-in described in [LAB.md](LAB.md), and their results are labelled as
such.
