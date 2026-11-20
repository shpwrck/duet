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

## kube-apiserver itself

`apiserver.sh` runs a real kube-apiserver against two duet nodes. Run by
hand on 20 Nov 2026 with Kubernetes v1.37.2 (2 s disk lease, one machine,
image file as the shared disk):

- kube-apiserver became ready in 3 s on an empty store and created its
  bootstrap namespaces.
- With kubectl: namespaces, 50 configmaps, a patch, a delete, a paged
  list, a Deployment object, a CustomResourceDefinition and an object of
  that kind all behaved normally.
- **Owner killed under load** (two runs): 353 and 360 of 400 creates
  acknowledged; none missing afterwards; an open `kubectl get --watch`
  saw every acknowledged one. The API accepted writes again 2.3 to 2.6 s
  after the kill.
- **Planned handover** (SIGTERM to the owner): 149 and 150 of 150
  acknowledged, none missing, longest gap between writes 0.1 s.

An earlier version let only the owner listen. kube-apiserver then took
7.8 s to recover from the kill and 18 s from a planned handover that duet
itself completed in 0.2 s, because its etcd client was backing off from
an endpoint that had been refusing connections. Now every node listens
and a node that does not own the log forwards to the one that does.

Not exercised: controllers, scheduler, kubelets, any real workload, more
than a few hundred objects, or any of this on separate machines.
