#!/usr/bin/env bash
# Runs a real kube-apiserver against two duet nodes on one disk image,
# kills the owning node under a write loop with a watch open, and checks
# that no acknowledged create is missing and the watch saw them all.
#
# Needs kube-apiserver and kubectl in $KUBE_BIN (not downloaded here):
#   https://dl.k8s.io/release/<version>/bin/linux/amd64/{kube-apiserver,kubectl}
# Last run by hand with v1.37.2; results in test/kube/README.md.
set -euo pipefail
KUBE_BIN=${KUBE_BIN:?directory holding kube-apiserver and kubectl}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
W=$(mktemp -d)
trap 'pkill -9 -f "$W/" 2>/dev/null || true' EXIT
(cd "$ROOT" && go build -o "$W/duet" ./cmd/duet)
truncate -s 2G "$W/disk.img"
D=-direct=true
fmt() { "$W/duet" format -device "$W/disk.img" -nodes 2 -ttl 2s -epoch-slots 256 -max-batch 2097152 -checkpoint-size 268435456 $D; }
fmt || { D=-direct=false; fmt; }
node() {
  mkdir -p "$W/s$1"
  "$W/duet" run -device "$W/disk.img" -node "$1" -listen 127.0.0.1:710"$1" -etcd 127.0.0.1:2379"$1" -etcd-peers 127.0.0.1:23790,127.0.0.1:23791 \
    -state-dir "$W/s$1" -watch-progress-interval 5s $D >>"$W/duet$1.log" 2>&1 &
  echo $! >"$W/duet$1.pid"
}
node 0; sleep 3; node 1
openssl genrsa -out "$W/sa.key" 2048 2>/dev/null
openssl rsa -in "$W/sa.key" -pubout -out "$W/sa.pub" 2>/dev/null
echo "testtoken,admin,admin,system:masters" >"$W/tokens.csv"
"$KUBE_BIN/kube-apiserver" --etcd-servers=http://127.0.0.1:23790,http://127.0.0.1:23791 \
  --service-account-issuer=https://kubernetes.default.svc --service-account-key-file="$W/sa.pub" \
  --service-account-signing-key-file="$W/sa.key" --cert-dir="$W/certs" --secure-port=6443 \
  --bind-address=127.0.0.1 --authorization-mode=AlwaysAllow --token-auth-file="$W/tokens.csv" \
  --service-cluster-ip-range=10.96.0.0/16 >"$W/apiserver.log" 2>&1 &
echo $! >"$W/apiserver.pid"
K=("$KUBE_BIN/kubectl" --server=https://127.0.0.1:6443 --token=testtoken --insecure-skip-tls-verify --request-timeout=3s)
for _ in $(seq 60); do "${K[@]}" get --raw /readyz >/dev/null 2>&1 && break; sleep 1; done
"${K[@]}" get --raw /readyz >/dev/null
"${K[@]}" create ns fo >/dev/null
"${K[@]}" -n fo get cm --watch --request-timeout=0 -o name >"$W/watch.out" 2>/dev/null &
echo $! >"$W/watch.pid"
(for i in $(seq 400); do
  if "${K[@]}" -n fo create cm "f$i" --from-literal=v="$i" >/dev/null 2>&1; then echo "f$i $(date +%s.%N)" >>"$W/acked"; fi
done) &
LOAD=$!
sleep 5; kill -9 "$(cat "$W/duet0.pid")"; KILL=$(date +%s.%N)
wait $LOAD; sleep 3
"${K[@]}" -n fo get cm -o name | sed 's#configmap/##' | sort >"$W/present"
sed 's#configmap/##' "$W/watch.out" | sort -u >"$W/watched"
cut -d' ' -f1 "$W/acked" | sort >"$W/names"
missing=$(comm -23 "$W/names" "$W/present" | wc -l)
unseen=$(comm -23 "$W/names" "$W/watched" | wc -l)
first=$(awk -v k="$KILL" '$2>k{printf "%.1f", $2-k; exit}' "$W/acked")
echo "acknowledged creates: $(wc -l <"$W/acked") of 400"
echo "acknowledged but missing: $missing"
echo "acknowledged but not seen by the watch: $unseen"
echo "first acknowledged create after the kill: ${first}s"

# Planned handover: bring the killed node back, then stop the owner
# gracefully under the same kind of load.
node 0; sleep 5
(for i in $(seq 150); do
  if "${K[@]}" -n fo create cm "g$i" --from-literal=v="$i" >/dev/null 2>&1; then echo "g$i $(date +%s.%N)" >>"$W/acked2"; fi
done) &
LOAD=$!
sleep 4; kill -TERM "$(cat "$W/duet1.pid")"
wait $LOAD
"${K[@]}" -n fo get cm -o name | sed 's#configmap/##' | sort >"$W/present"
cut -d' ' -f1 "$W/acked2" | sort >"$W/names2"
missing2=$(comm -23 "$W/names2" "$W/present" | wc -l)
gap=$(awk 'NR>1 && $2-p>g{g=$2-p} {p=$2} END{printf "%.1f", g}' "$W/acked2")
echo "planned handover: acknowledged $(wc -l <"$W/acked2") of 150, missing $missing2, longest gap ${gap}s"
[ "$missing" = 0 ] && [ "$unseen" = 0 ] && [ "$missing2" = 0 ]
