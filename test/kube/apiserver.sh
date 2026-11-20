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
source "$(dirname "$0")/lib.sh"
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
