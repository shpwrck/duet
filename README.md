# duet

An etcd-compatible datastore for Kubernetes control planes that share one
disk instead of running a consensus protocol between themselves.

The target platform is VMware Cloud Foundation: control plane VMs on a vSAN
cluster, sharing one vSAN-backed virtual disk. vSAN, with the witness it
already has, replicates the disk and breaks ties when hosts are partitioned.
duet adds no arbiter of its own. Two nodes is the minimum; the same protocol
runs with more.

The idea is the one Amazon EKS uses at 100,000 nodes: take Raft out, keep a
single writer, and push durability and ordering into a replicated log. Here
the log is a shared disk.

**Status: early.** The ownership protocol and the log are tested in
simulation, as real processes on one machine, and on two Google Cloud VMs
sharing a replicated disk. The stock etcd client works against it across an
owner failure, and Kubernetes' storage layer passes most of its own test
suite against it. kube-apiserver itself has not been run against it, the
log is never trimmed, and nothing has run on vSAN. See
[what exists](#what-exists-today) before drawing conclusions.

## How it works

- **One writer per epoch.** The owner appends checksummed records to the
  log on the shared disk, many records per flush.
- **Ownership is decided on the disk.** A Disk Paxos ballot over per-node
  blocks picks the owner of each epoch. A timed lease tells the others when
  they may start a ballot. Nothing is decided over the network, so a network
  partition between nodes cannot create two owners.
- **Everyone else tails.** Other nodes read the log as it grows and stay
  warm, so takeover replays only the unread tail.
- **A deposed writer is harmless.** After its data reaches the disk it reads
  the disk again, and acknowledges only if no other node has started to take
  over. That check uses no clock. A late write lands in a gap that no
  reader ever looks at.

[docs/DESIGN.md](docs/DESIGN.md) has the protocol, its assumptions and the
safety argument.

## What exists today

| Piece | State |
| --- | --- |
| Disk layout, metadata blocks (`pkg/disk`) | Implemented |
| Record log with group commit (`pkg/wal`) | Implemented, no trimming yet |
| Disk Paxos ownership and leases (`pkg/lease`) | Implemented |
| Writer, tailer, takeover (`pkg/node`) | Implemented |
| Fault simulation and tests | Implemented, see below |
| `duet` process: direct I/O, real clock, renewal loop, group commit (`cmd/duet`) | Implemented, with a temporary HTTP interface for testing |
| Process-level fault test (`test/proc`) | Implemented: owner frozen and killed under load |
| State machine: etcd's own storage, transaction and lease code driven by the log (`pkg/sm`) | Implemented |
| etcd v3 gRPC API: KV, Watch, Lease, Maintenance Status (`pkg/api`, `duet run -etcd`) | Implemented for what kube-apiserver uses; passes the part of Kubernetes' own storage test suite listed in [test/kube](test/kube/README.md); kube-apiserver itself has not been run. Only the owner serves |
| Checkpoints (`pkg/ckpt`) | Implemented: any node writes them, a restart loads the newest and replays the rest |
| Log trimming, epoch table reuse | Not started; the log fills up and stops |
| SCSI-3 reservation backend | Not started |
| TLA+ model | Not started |
| Google Cloud lab (`deploy/lab-gcp`) | Run; results in [docs/LAB-RUNS.md](docs/LAB-RUNS.md). Tests the log, not yet the etcd API |
| Anything on vSAN | Not started; needs a lab |

## Tests

```
go test -short ./...             # simulation only, a few seconds
go test ./...                    # adds real processes, about 30 seconds
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

## Documents

- [docs/DESIGN.md](docs/DESIGN.md): protocol, assumptions, scaling path, milestones
- [docs/PRIOR-ART.md](docs/PRIOR-ART.md): Kine, GKE on Spanner, EKS on Journal and others, and what each lacks here
- [docs/VCF-GAPS.md](docs/VCF-GAPS.md): what is unverified or missing in VCF for this to ship
- [docs/LAB.md](docs/LAB.md): the three labs, including the Google Cloud stand-in for vSAN

## License

Apache-2.0. See [LICENSE](LICENSE).
