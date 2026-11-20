#!/usr/bin/env bash
# Two sites, each with its own kube-apiserver talking only to the datastore
# member beside it, as a stacked control plane does. Writes go through both
# API servers. Then site A is lost outright (its datastore member and its
# API server; for etcd, the site holding two of the three members), and
# later returns.
#
#   BACKEND=duet|etcd
#
# Needs the same binaries in $KUBE_BIN as apiserver.sh.
set -euo pipefail
FIRST_ETCD=http://127.0.0.1:23790
source "$(dirname "$0")/lib.sh"
apiserver_b() {
  "$KUBE_BIN/kube-apiserver" --etcd-servers=http://127.0.0.1:23791 \
    --service-account-issuer=https://kubernetes.default.svc --service-account-key-file="$W/sa.pub" \
    --service-account-signing-key-file="$W/sa.key" --cert-dir="$W/certs-b" --secure-port=6444 \
    --bind-address=127.0.0.1 --authorization-mode=AlwaysAllow --token-auth-file="$W/tokens.csv" \
    --service-cluster-ip-range=10.96.0.0/16 >>"$W/apiserver-b.log" 2>&1 &
}
apiserver_a() {
  "$KUBE_BIN/kube-apiserver" --etcd-servers="$FIRST_ETCD" \
    --service-account-issuer=https://kubernetes.default.svc --service-account-key-file="$W/sa.pub" \
    --service-account-signing-key-file="$W/sa.key" --cert-dir="$W/certs" --secure-port=6443 \
    --bind-address=127.0.0.1 --authorization-mode=AlwaysAllow --token-auth-file="$W/tokens.csv" \
    --service-cluster-ip-range=10.96.0.0/16 >>"$W/apiserver.log" 2>&1 &
}
KA=("$KUBE_BIN/kubectl" --server=https://127.0.0.1:6443 --token=testtoken --insecure-skip-tls-verify --request-timeout=3s)
KB=("$KUBE_BIN/kubectl" --server=https://127.0.0.1:6444 --token=testtoken --insecure-skip-tls-verify --request-timeout=3s)
ready() { for _ in $(seq 60); do "$@" get --raw /readyz >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
apiserver_b
ready "${KB[@]}"
"${KA[@]}" create ns two >/dev/null
load() { # prefix count file: alternate between the two API servers
  for i in $(seq "$2"); do
    if [ $((i % 2)) = 0 ]; then k=("${KA[@]}"); via=A; else k=("${KB[@]}"); via=B; fi
    if "${k[@]}" -n two create cm "$1$i" --from-literal=v="$i" >/dev/null 2>&1; then echo "$1$i $(date +%s.%N) $via" >>"$3"; fi
  done
}
watch_b() { # (re)start a watch through API server B; a watch does not survive losing the datastore
  pkill -f "$W/watch.marker" 2>/dev/null || true
  : >"$W/watch.out"
  "${KB[@]}" -n two get cm --watch --request-timeout=0 -o name --cache-dir="$W/watch.marker" >"$W/watch.out" 2>/dev/null &
}
check() { # label file total event-time
  sleep 2
  local acked a b first
  acked=$(wc -l <"$2"); a=$(grep -c ' A$' "$2" || true); b=$(grep -c ' B$' "$2" || true)
  first=""
  [ "$4" != 0 ] && first=", first write $(awk -v k="$4" '$2>k{printf "%.1f s", $2-k; f=1; exit} END{if(!f) printf "never"}' "$2") after"
  if ! "${KB[@]}" -n two get cm -o name >"$W/list" 2>/dev/null; then
    echo "$BACKEND $1: acknowledged $acked of $3 ($a through A, $b through B)$first; API server B cannot read: the datastore has no majority"
    return 1
  fi
  sed 's#configmap/##' "$W/list" | sort >"$W/present"
  sed 's#configmap/##' "$W/watch.out" | sort -u >"$W/watched"
  cut -d' ' -f1 "$2" | sort >"$W/names"
  local missing unseen
  missing=$(comm -23 "$W/names" "$W/present" | wc -l)
  unseen=$(comm -23 "$W/names" "$W/watched" | wc -l)
  echo "$BACKEND $1: acknowledged $acked of $3 ($a through A, $b through B), missing $missing, not seen by watch $unseen$first"
  [ "$missing" = 0 ] && [ "$unseen" = 0 ]
}

watch_b
: >"$W/acked0"; load n 60 "$W/acked0"
check "both sites up" "$W/acked0" 60 0

# Site A is lost: its datastore member(s) and its API server.
load f 300 "$W/acked1" & LOAD=$!
sleep 4
if [ "$BACKEND" = duet ]; then down="0"; else down="0 2"; fi
for i in $down; do kill -9 "$(cat "$W/n$i.pid")"; done
pkill -9 -f "secure-port=6443" || true
T=$(date +%s.%N)
wait $LOAD
check "site A lost" "$W/acked1" 300 "$T" || true

# Site A returns.
for i in $down; do node "$i"; done
apiserver_a
if ready "${KA[@]}"; then
  # Everything acknowledged before and during the outage must still be there.
  "${KB[@]}" -n two get cm -o name | sed 's#configmap/##' | sort >"$W/present"
  cat "$W/acked0" "$W/acked1" | cut -d' ' -f1 | sort >"$W/names"
  echo "$BACKEND site A back: $(comm -23 "$W/names" "$W/present" | wc -l) of $(wc -l <"$W/names") earlier acknowledged creates missing"
  watch_b; sleep 1
  T=$(date +%s.%N); : >"$W/acked2"; load r 60 "$W/acked2"
  check "site A back, new writes" "$W/acked2" 60 "$T"
else
  echo "$BACKEND site A back: its API server did not become ready"
  exit 1
fi
