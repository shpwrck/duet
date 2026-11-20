#!/usr/bin/env bash
# Runs a real kube-apiserver against a datastore, kills the datastore's
# leader under a write loop with a watch open, then does a planned
# handover, and reports what was lost and how long writes stopped.
#
#   BACKEND=duet  (default) two duet nodes on one disk image, 2 s lease
#   BACKEND=etcd  the control: three etcd members, default timings
#
# Needs kube-apiserver and kubectl in $KUBE_BIN, and for the control etcd
# and etcdctl as well (none are downloaded here):
#   https://dl.k8s.io/release/<version>/bin/linux/amd64/{kube-apiserver,kubectl}
#   https://github.com/etcd-io/etcd/releases
# Results of the last runs are in test/kube/README.md.
set -euo pipefail
KUBE_BIN=${KUBE_BIN:?directory holding kube-apiserver and kubectl}
BACKEND=${BACKEND:-duet}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
W=$(mktemp -d)
trap 'pkill -9 -f "$W/" 2>/dev/null || true; sleep 0.2; pkill -9 -f "$W/" 2>/dev/null || true' EXIT

if [ "$BACKEND" = duet ]; then
  SERVERS=http://127.0.0.1:23790,http://127.0.0.1:23791
  (cd "$ROOT" && go build -o "$W/duet" ./cmd/duet)
  truncate -s 2G "$W/disk.img"
  D=-direct=true
  fmt() { "$W/duet" format -device "$W/disk.img" -nodes 2 -ttl 2s -epoch-slots 256 -max-batch 2097152 -checkpoint-size 268435456 $D; }
  fmt || { D=-direct=false; fmt; }
  node() {
    mkdir -p "$W/s$1"
    "$W/duet" run -device "$W/disk.img" -node "$1" -listen 127.0.0.1:710"$1" -etcd 127.0.0.1:2379"$1" \
      -etcd-peers 127.0.0.1:23790,127.0.0.1:23791 -state-dir "$W/s$1" -watch-progress-interval 5s $D >>"$W/n$1.log" 2>&1 &
    echo $! >"$W/n$1.pid"
  }
  leader() { for i in 0 1; do curl -s --max-time 1 127.0.0.1:710$i/status | grep -q '"owner":true' && { echo $i; return; }; done; echo none; }
  node 0; sleep 3; node 1; sleep 1
else
  SERVERS=http://127.0.0.1:23790,http://127.0.0.1:23791,http://127.0.0.1:23792
  CLUSTER=n0=http://127.0.0.1:23800,n1=http://127.0.0.1:23801,n2=http://127.0.0.1:23802
  node() {
    "$KUBE_BIN/etcd" --name n"$1" --data-dir "$W/s$1" --listen-client-urls http://127.0.0.1:2379"$1" \
      --advertise-client-urls http://127.0.0.1:2379"$1" --listen-peer-urls http://127.0.0.1:2380"$1" \
      --initial-advertise-peer-urls http://127.0.0.1:2380"$1" --initial-cluster "$CLUSTER" \
      --initial-cluster-state new --watch-progress-notify-interval 5s >>"$W/n$1.log" 2>&1 &
    echo $! >"$W/n$1.pid"
  }
  leader() {
    "$KUBE_BIN/etcdctl" --endpoints="$SERVERS" --command-timeout=1s endpoint status -w json 2>/dev/null |
      python3 -c 'import sys,json
for e in json.load(sys.stdin):
    s=e["Status"]
    if s["leader"]==s["header"]["member_id"]: print(e["Endpoint"][-1]); break
else: print("none")' 2>/dev/null || echo none
  }
  node 0; node 1; node 2; sleep 4
fi

openssl genrsa -out "$W/sa.key" 2048 2>/dev/null
openssl rsa -in "$W/sa.key" -pubout -out "$W/sa.pub" 2>/dev/null
echo "testtoken,admin,admin,system:masters" >"$W/tokens.csv"
"$KUBE_BIN/kube-apiserver" --etcd-servers="$SERVERS" \
  --service-account-issuer=https://kubernetes.default.svc --service-account-key-file="$W/sa.pub" \
  --service-account-signing-key-file="$W/sa.key" --cert-dir="$W/certs" --secure-port=6443 \
  --bind-address=127.0.0.1 --authorization-mode=AlwaysAllow --token-auth-file="$W/tokens.csv" \
  --service-cluster-ip-range=10.96.0.0/16 >"$W/apiserver.log" 2>&1 &
K=("$KUBE_BIN/kubectl" --server=https://127.0.0.1:6443 --token=testtoken --insecure-skip-tls-verify --request-timeout=3s)
for _ in $(seq 60); do "${K[@]}" get --raw /readyz >/dev/null 2>&1 && break; sleep 1; done
"${K[@]}" get --raw /readyz >/dev/null
"${K[@]}" create ns fo >/dev/null
"${K[@]}" -n fo get cm --watch --request-timeout=0 -o name >"$W/watch.out" 2>/dev/null &

load() { # name prefix, count, output file
  for i in $(seq "$2"); do
    if "${K[@]}" -n fo create cm "$1$i" --from-literal=v="$i" >/dev/null 2>&1; then echo "$1$i $(date +%s.%N)" >>"$3"; fi
  done
}
report() { # label, acked file, total, event time
  "${K[@]}" -n fo get cm -o name | sed 's#configmap/##' | sort >"$W/present"
  sed 's#configmap/##' "$W/watch.out" | sort -u >"$W/watched"
  cut -d' ' -f1 "$2" | sort >"$W/names"
  local missing unseen gap first
  missing=$(comm -23 "$W/names" "$W/present" | wc -l)
  unseen=$(comm -23 "$W/names" "$W/watched" | wc -l)
  gap=$(awk 'NR>1 && $2-p>g{g=$2-p} {p=$2} END{printf "%.1f", g}' "$2")
  first=$(awk -v k="$4" '$2>k{printf "%.1f", $2-k; exit}' "$2")
  echo "$BACKEND $1: acknowledged $(wc -l <"$2") of $3, missing $missing, not seen by watch $unseen, first write ${first}s after, longest gap ${gap}s"
  [ "$missing" = 0 ] && [ "$unseen" = 0 ]
}

load f 400 "$W/acked1" & LOAD=$!
sleep 5; L=$(leader); kill -9 "$(cat "$W/n$L.pid")"; T=$(date +%s.%N)
wait $LOAD; sleep 3
report "leader killed" "$W/acked1" 400 "$T"

node "$L"; sleep 6
load g 150 "$W/acked2" & LOAD=$!
sleep 4; L=$(leader); kill -TERM "$(cat "$W/n$L.pid")"; T=$(date +%s.%N)
wait $LOAD; sleep 3
report "planned stop of leader" "$W/acked2" 150 "$T"
