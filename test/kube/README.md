# Kubernetes storage conformance

Runs the storage test suite that Kubernetes uses for its own etcd3
backend (`k8s.io/apiserver/pkg/storage/testing`) against `duet run -etcd`,
through the same `etcd3` store code kube-apiserver uses.

It is a separate Go module so the main module does not depend on
Kubernetes.

    cd test/kube && go test ./...

This is the storage layer of kube-apiserver, not kube-apiserver itself.

## What runs

44 entries of the suite: create, get, delete with preconditions and
suggestions, guaranteed update and its conflict paths, objects with a
TTL, the main list test (with plain and streamed ranges), paging,
continuation, continuation across a compaction, consistent list,
resource-version matching, namespace scoping, transformation failures,
stats, compaction, and watches (from zero, from a revision, delayed
delivery, delete events, errors, cluster and namespace scoped, cancel,
timeout).

## What does not run

- The corrupt-object tests (`RunTestGetListWithErrorAggregation` and
  relatives, `RunTestWatchWithUnsafeDelete`), which need a hook to make
  decoding fail.
- The checks that count etcd calls per list page
  (`RunTestListContinuation` runs, but with call counting switched off).
- Watch bookmark tests, which Kubernetes runs against its watch cache, not
  against etcd.
- Anything during an ownership change. All of this runs against a stable
  owner; `test/proc` covers takeover with the plain etcd client.

## kube-apiserver itself, with etcd as the control

`apiserver.sh` runs a real kube-apiserver against two duet nodes, or with
`BACKEND=etcd` against three etcd members as the control. It kills the
datastore's leader under a `kubectl create` loop with a watch open, then
restarts it and stops the new leader gracefully.

Runs on 20 Nov 2026: Kubernetes v1.37.2, etcd v3.7.4 with default
timings, duet with a 2 s disk lease. One 2-vCPU machine, local disk, an
image file as duet's shared disk. Two runs each, both shown.

| | duet, 2 nodes | etcd, 3 members |
| --- | --- | --- |
| Leader killed: creates acknowledged, of 400 | 399, 399 | 399, 399 |
| Acknowledged creates missing afterwards | 0, 0 | 0, 0 |
| Acknowledged creates the watch did not see | 0, 0 | 0, 0 |
| First write after the kill | 2.6 s, 2.5 s | 3.1 s, 3.1 s |
| Planned stop: creates acknowledged, of 150 | 149, 149 | 150, 150 |
| Longest gap in writes during planned stop | 0.3 s, 0.3 s | 0.1 s, 0.1 s |

`bench_test.go` puts the same write load on either through the stock etcd
client (guarded transactions, 1 KiB values, 15 s):

| | duet, 2 nodes | etcd, 3 members |
| --- | --- | --- |
| 8 clients | 2,134 writes/s, p50 3.2 ms, p99 12.8 ms | 1,414 writes/s, p50 5.2 ms, p99 14.2 ms |
| 64 clients | 4,135 writes/s, p50 12.7 ms, p99 50.0 ms | 3,298 writes/s, p50 17.8 ms, p99 49.4 ms |

Read these with care. Everything shared two CPU cores and one disk, the
load generator included, so the numbers say more about this machine than
about either system; etcd ran three processes to duet's two. The
comparison that matters is on separate machines across two zones, which
has not been run. duet loses one create on a planned stop where etcd
loses none.

Two things were fixed to get here. At first only the owner listened, and
kube-apiserver took 7.8 s to recover from the kill and 18 s from a planned
stop that duet itself completed in 0.2 s, because its etcd client was
backing off from an endpoint that had been refusing connections. Then
followers forwarded but failed fast while the owner was unreachable, and
about 45 of 400 creates failed where etcd failed one. Now every node
listens, and a node that does not own the log holds a call until the
owner is reachable and then forwards it.

## Two sites, a kube-apiserver in each

`twosites.sh` runs two kube-apiservers, each talking only to the datastore
member beside it, and sends writes through both. Then site A is lost
outright: its datastore member and its API server (for etcd, the site
holding two of the three members). Later it returns.

One run each on 20 Nov 2026, same machine and versions as above:

| | duet, 2 nodes | etcd, 3 members |
| --- | --- | --- |
| Both sites up: creates acknowledged, of 60 | 60, half through each API server | 60, half through each |
| Site A lost: creates through site B, of 150 | 149; first one 2.6 s after the loss | 36, all from before the loss; none after |
| Site B's API server during the outage | serving | cannot read: no majority |
| Site A back: earlier acknowledged creates missing | 0 of 244 | 0 of 131 |
| Site A back: new creates, of 60 | 60, half through each | 60, half through each |

This is the two-zone result from the cloud lab, reproduced with real API
servers: with etcd's majority in the lost site, the surviving site's API
server is up but useless until the site returns. Neither system lost an
acknowledged write.

## A whole control plane, with simulated nodes

`controlplane.sh` adds kube-controller-manager and kube-scheduler, with
[kwok](https://kwok.sigs.k8s.io) standing in for the nodes. It rolls out a
Deployment, then a second one during which the datastore's leader is
killed, and waits for every pod to be Running. `lib.sh` holds the setup
the scripts share.

One run each on 20 Nov 2026, same machine and versions as above, 100
simulated nodes and 3,000 pods per Deployment:

| | duet, 2 nodes | etcd, 3 members |
| --- | --- | --- |
| 3,000 pods Running, no fault | 29 s | 35 s |
| 3,000 more, leader killed 8 s in | 348 s | 296 s |
| Pods Running at the end, of 6,000 | 6,000 | 6,000 |

Both converge, and both take about ten times longer after a leader kill.
Since etcd does too, the slowdown is on the Kubernetes side on this
machine and has not been looked into. One run each is not enough to rank
the two.

Not exercised: real kubelets or workloads, more than 6,000 pods, or any
of this on separate machines.

## Reads, lists and watches

`duet etcdbench` loads objects and then measures the calls a
kube-apiserver makes most. It talks to any etcd v3 endpoint, so the same
command measures both systems.

    duet etcdbench -endpoints 127.0.0.1:23790,127.0.0.1:23791 -keys 20000 -duration 10s

One machine, 20,000 objects of 4 KiB, duet with two nodes on one disk image
and etcd v3.7.4 with three members. All members share one local disk here,
which costs etcd three synchronous writes to it per write against duet's
one, so the write rows flatter duet. The read rows do not depend on that.

| Call | duet, 2 nodes | etcd, 3 members |
|---|---|---|
| Read one object, must be current, 32 clients | 8,018/s, p99 11.5 ms | 8,065/s, p99 11.7 ms |
| Read one object, may be stale, 32 clients | 10,872/s, p99 9.7 ms | 12,665/s, p99 8.9 ms |
| List all 20,000 in pages of 500, 4 clients | p50 0.80 s, p99 0.92 s | p50 0.81 s, p99 0.94 s |
| 90% reads, 10% writes, 32 clients | 6,551/s, p99 13.7 ms | 4,064/s, p99 21.1 ms |
| Writes with 50 watchers on the prefix, 8 clients | 753/s, p99 29.2 ms | 561/s, p99 39.5 ms |
| Write sent to watch event received, 50 watchers | p50 10.1 ms, p99 28.8 ms | p50 14.3 ms, p99 42.1 ms |

Reads and lists are the same within noise, which is expected: both serve
them from the same etcd storage code. A read that must be current costs
duet one confirmation that it still owns the log and costs etcd one round
to a majority. Half of duet's clients reach the standby first and are
forwarded, and that shows in the stale-read row.

## A larger state

The same machine with 100,000 objects of 4 KiB and then two more load
phases, so the etcd state file grows from 240 MiB to 740 MiB. A checkpoint
is taken every 40,000 records.

| Measurement | Result |
|---|---|
| Checkpoint of 240 MiB | 1.4 s |
| Checkpoint of 490 MiB | 2.6 s |
| Checkpoint of 690 MiB | 3.0 s |
| Longest stretch with no acknowledged write while checkpoints are taken, 8 clients | 57 ms |
| Standby restarted with an empty state directory, until it has the 740 MiB state back from the shared disk | 4.5 s |
| Owner killed under load with that state, longest stretch with no acknowledged write | 2.4 s |
| Acknowledged writes lost | 0 of 58,716 |

Checkpoint time grows with the state, at roughly 200 to 250 MiB a second
on this disk, and it does not stop writes. The time to replace a killed
owner is the same as with an empty store, because the standby already has
the state applied and only the lease has to run out. A standby that lost
everything is rebuilt from the shared disk with no copy from the other
node.

This is still a small state by the measure of a large cluster (etcd's
default limit is 2 GiB and large clusters raise it to 8 GiB). At 8 GiB a
checkpoint at this disk speed would take 30 to 40 s and each node needs
two slots of that size on the shared disk. That has not been run.
