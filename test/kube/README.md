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

Not exercised: controllers, scheduler, kubelets, any real workload, more
than a few hundred objects, or any of this on separate machines.
