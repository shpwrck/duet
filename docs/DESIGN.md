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
remainder            log
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
| In-memory database | State machine in memory, rebuilt from checkpoint plus log | Not started |
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
- **The log is not trimmed and the epoch table is finite.** Checkpoints,
  trimming and epoch slot reuse are required before any long run.
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
| 3 | State machine and etcd API; checkpoints and trimming | kube-apiserver runs; upstream storage tests pass |
| 4 | Correctness harness on real processes and disks | Linearizability checker passes under faults |
| 5 | Kubernetes demo on the lab and on vSAN | Conformance; recovery benchmark against milestone 1 |
| 6 | Scale: partitions, in-memory state, load tests against the targets above | Measured throughput per partition; gap to targets stated |

Until a vSAN lab exists, milestones 0 and 1 run on the Google Cloud
stand-in described in [LAB.md](LAB.md), and their results are labelled as
such.
