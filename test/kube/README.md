# Kubernetes storage conformance

Runs the storage test suite that Kubernetes uses for its own etcd3
backend (`k8s.io/apiserver/pkg/storage/testing`) against `duet run -etcd`,
through the same `etcd3` store code kube-apiserver uses.

It is a separate Go module so the main module does not depend on
Kubernetes.

    cd test/kube && go test ./...

This is the storage layer of kube-apiserver, not kube-apiserver itself.

## What runs

36 entries of the suite (145 subtests): create, get, delete with
preconditions and suggestions, guaranteed update and its conflict paths,
objects with a TTL, lists (paging, continuation, continuation across a
compaction, consistent list, namespace scoping), compaction, and watches
(from zero, from a revision, delayed delivery, delete events, cluster and
namespace scoped, cancel, timeout).

## What does not run

- Tests that count etcd calls or swap the value transformer mid-test
  (`RunTestList`, `RunTestStats`, `RunTestGuaranteedUpdate`, the
  transformation-failure and corrupt-object tests). They need hooks into
  the client that this harness does not build. `RunTestList` is the
  largest gap.
- Watch bookmark tests, which Kubernetes runs against its watch cache, not
  against etcd.
- Anything during an ownership change. All of this runs against a stable
  owner; `test/proc` covers takeover with the plain etcd client.
