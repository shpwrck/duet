# Status

What is built, how each piece was tested, and what the tests do and do not
show. For results on real machines see [LAB-RUNS.md](LAB-RUNS.md) and
[../test/kube/README.md](../test/kube/README.md).

## Components

| Piece | State |
| --- | --- |
| Disk layout, metadata blocks (`pkg/disk`) | Implemented |
| Record log with group commit (`pkg/wal`) | Implemented; addressed in laps so space can be reused |
| Disk Paxos ownership and leases (`pkg/lease`) | Implemented |
| Writer, tailer, takeover (`pkg/node`) | Implemented |
| Fault simulation and tests | Implemented, see below |
| `duet` process: direct I/O, real clock, renewal loop, group commit (`cmd/duet`) | Implemented, with a temporary HTTP interface for testing |
| Process-level fault test (`test/proc`) | Implemented: owner frozen and killed under load |
| State machine: etcd's own storage, transaction and lease code driven by the log (`pkg/sm`) | Implemented |
| etcd v3 gRPC API: KV, Watch, Lease, Maintenance Status (`pkg/api`, `duet run -etcd`) | Implemented for what kube-apiserver uses; passes the part of Kubernetes' own storage test suite listed in [test/kube](../test/kube/README.md); a real kube-apiserver (v1.37.2) ran against it through an owner kill with no acknowledged write lost. Nodes that do not own the log forward to the one that does; they do not serve reads themselves |
| Checkpoints (`pkg/ckpt`) | Implemented: any node writes them. A node with nothing on its own disk loads the newest and replays the rest |
| Restart from the node's own state file (`pkg/sm`) | Implemented: the applied index is written in every database commit, as etcd does it; tested at 324 crash points and by killing both nodes at once under load |
| State size limit | Implemented: new data is refused at nine tenths of a checkpoint slot, with etcd's own error. No defragment call yet |
| Log reuse (`pkg/trim`, `pkg/node`, `duet retire`) | Implemented; tested in simulation with mutation tests and as processes over seven laps of a small log |
| Epoch table reuse | Not planned; one slot per ownership change, sized at format time |
| SCSI-3 reservation backend | Not started |
| TLA+ model | Not started |
| TLS, authentication, backup and restore, defragment, member add and remove | Not started |
| Google Cloud lab (`deploy/lab-gcp`) | Run eight times; results, including the comparison with three-member etcd, in [docs/LAB-RUNS.md](LAB-RUNS.md) |
| Anything on vSAN | Not started; needs a lab |

## Tests

```
go test -short ./...             # simulation only, a few seconds
go test ./...                    # adds real processes, about 3 minutes
BIN=/dir/with/duet+etcdctl test/soak.sh 19   # random faults for 9 minutes
DUET_SEEDS=6000 go test ./pkg/node -run TestRandomSchedules -v   # soak
```

The tests run several nodes against one in-memory disk and interleave them
at the level of single block reads and writes. They cover a writer paused
mid-write past its lease, renewals racing ballots at every interleaving,
ballots racing each other, crashes at every step of a ballot, handover,
restart, a torn tail, and randomised schedules with pauses, slow I/O, clock
skew and crashes.

What that does and does not show:

- Each of ten deliberate bugs in the safety rules (for example, dropping the
  check after write) makes at least one test fail. The full suite catches all
  ten; the randomised test alone catches eight of ten at 4,000 seeds.
- A 6,000-schedule run passed with about 69,000 acknowledged writes and
  55,000 ownership changes.
- As real processes on one machine (three nodes, 1 s lease, direct I/O on a
  local disk), a frozen owner and a killed owner were each replaced in under
  two seconds, with every acknowledged write present on all nodes afterwards.
  Throughput there was about 7,000 writes per second from 8 clients. That
  is a local SSD and one kernel; it says nothing about shared network disks.
- The simulated disk is ideal: linearizable, atomic block writes, durable on
  return. Whether a vSAN multi-writer disk behaves that way is unverified
  and is the first thing to test on real hardware.
- Passing tests are evidence, not proof. A TLA+ model and independent
  testing are planned before any production claim.

## Through the etcd API

These run the real binary and the stock etcd client, on one machine unless
the lab is named.

| Question | Test | Result |
| --- | --- | --- |
| Does every read see the latest write, across owner changes? | `TestLinearizable`: recorded history checked with porcupine | About 21,000 operations across a kill, a planned stop and a kill; linearizable in every run so far |
| Is an acknowledged write ever lost? | Every lab and soak scenario verifies each acknowledged key afterwards | None lost in 8 lab runs or in a 9 minute soak of 583,385 writes with a random fault every 20 s |
| Does it survive both nodes dying at once? | `TestBothNodesKilledContinueFromLocalState` | Four double kills under load; state identical to a node built from the shared disk alone |
| Can the nodes be cut off from each other but not from the disk? | `TestNodesCannotReachEachOther` | Clients move to the owner; no second owner appears |
| Does the log wrap safely? | `TestLogReuse`, `test/soak.sh` | More than seven laps with owner kills, a frozen standby and a dead owner |
| What happens when the state outgrows its space? | `TestQuotaKeepsStateCheckpointable` | New data refused; deletes, reads and checkpoints continue |
| How does it compare with etcd on reads, lists and watches? | `duet etcdbench` | Level on reads and lists ([numbers](../test/kube/README.md#reads-lists-and-watches)) |
| How does it compare with etcd under faults, on real zones? | Lab run 8 | Far better when a zone is lost (2.5 s against 46 s), level or slightly behind for single leader faults, behind on a full cold start at the time of that run ([numbers](LAB-RUNS.md)) |

What these do not show: anything on vSAN; state of several gigabytes; a
soak of hours; a control plane larger than 100 simulated nodes and 3,000
pods; more than two duet nodes in a lab; behaviour when the shared disk
itself is slow or returns errors, other than in simulation.
