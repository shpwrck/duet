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
