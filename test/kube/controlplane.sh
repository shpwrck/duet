#!/usr/bin/env bash
# A control plane under load, on duet or on the etcd control: real
# kube-apiserver, kube-controller-manager and kube-scheduler, with kwok
# standing in for the nodes. Creates NODES simulated nodes and a Deployment
# of PODS pods, measures how long until every pod is Running, kills the
# datastore's leader half-way through a second Deployment, and checks that
# everything still converges.
#
#   BACKEND=duet|etcd  NODES=100  PODS=2000  KILL_AFTER=5
#
# Needs kube-apiserver, kubectl, kube-controller-manager, kube-scheduler
# and kwok in $KUBE_BIN (plus etcd and etcdctl for the control), and kwok's
# stage definitions in $KUBE_BIN/kwok-stages: from the kwok repository,
# kustomize/stage/node/fast, node/heartbeat-with-lease and pod/fast.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
NODES=${NODES:-100}
PODS=${PODS:-2000}
K=("$KUBE_BIN/kubectl" --kubeconfig "$KUBECONFIG_FILE" --request-timeout=10s)

"$KUBE_BIN/kube-controller-manager" --kubeconfig "$KUBECONFIG_FILE" \
  --service-account-private-key-file="$W/sa.key" --root-ca-file="$W/certs/apiserver.crt" \
  --leader-elect=false --kube-api-qps=300 --kube-api-burst=600 --concurrent-replicaset-syncs=20 \
  --use-service-account-credentials=false --controllers='*,-nodeipam' \
  >"$W/kcm.log" 2>&1 &
cat >"$W/sched.yaml" <<SC
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection: {kubeconfig: "$KUBECONFIG_FILE", qps: 300, burst: 600}
leaderElection: {leaderElect: false}
SC
"$KUBE_BIN/kube-scheduler" --config "$W/sched.yaml" -v="${SCHED_V:-1}" >"$W/sched.log" 2>&1 &
STAGES=()
for f in "$KUBE_BIN"/kwok-stages/*.yaml; do STAGES+=(--config "$f"); done
"$KUBE_BIN/kwok" --kubeconfig "$KUBECONFIG_FILE" --manage-all-nodes=true --node-lease-duration-seconds=40 "${STAGES[@]}" \
  >"$W/kwok.log" 2>&1 &

for i in $(seq "$NODES"); do
  cat <<NODE
---
apiVersion: v1
kind: Node
metadata: {name: kwok-$i, labels: {type: kwok}}
status:
  allocatable: {cpu: "64", memory: 256Gi, pods: "500"}
  capacity: {cpu: "64", memory: 256Gi, pods: "500"}
NODE
done | "${K[@]}" apply -f - >/dev/null
deploy() { # name replicas
  cat <<DEP | "${K[@]}" apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata: {name: $1, namespace: default}
spec:
  replicas: $2
  selector: {matchLabels: {app: $1}}
  template:
    metadata: {labels: {app: $1}}
    spec: {containers: [{name: c, image: fake}]}
DEP
}
running() { "${K[@]}" get pods -l app="$1" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l; }
converge() { # name replicas timeout-seconds; prints seconds taken
  local start=$SECONDS
  while [ "$(running "$1")" -lt "$2" ]; do
    if [ $((SECONDS - start)) -gt "$3" ]; then echo "timeout"; return 1; fi
    sleep 1
  done
  echo $((SECONDS - start))
}
sleep 2
for p in kube-controller-manager kube-scheduler kwok; do
  pgrep -f "$KUBE_BIN/$p .*$W" >/dev/null || { echo "$p did not stay up; see $W" >&2; exit 1; }
done
for _ in $(seq 60); do [ "$("${K[@]}" get nodes --no-headers 2>/dev/null | grep -c ' Ready')" -ge "$NODES" ] && break; sleep 1; done
echo "$BACKEND: $("${K[@]}" get nodes --no-headers | grep -c ' Ready') of $NODES simulated nodes Ready"

deploy a "$PODS"
echo "$BACKEND: $PODS pods Running after $(converge a "$PODS" 600)s (no fault)"

deploy b "$PODS"
sleep "${KILL_AFTER:-5}"; L=$(leader); kill -9 "$(cat "$W/n$L.pid")"
echo "$BACKEND: killed leader (member $L) with $(running b) of $PODS pods of the second Deployment Running"
echo "$BACKEND: second $PODS pods Running after $(converge b "$PODS" 600)s (leader killed ${KILL_AFTER:-5} s in)"
node "$L"; sleep 5
total=$("${K[@]}" get pods --no-headers --request-timeout=60s | grep -c Running)
dup=$("${K[@]}" get pods -o name --request-timeout=60s | sort | uniq -d | wc -l)
echo "$BACKEND: $total pods Running in total (want $((2 * PODS))), duplicate names $dup"
[ "$total" = $((2 * PODS)) ]
