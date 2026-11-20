#!/usr/bin/env bash
# Runs the lab scenarios against an applied Terraform configuration and
# writes results.json plus a Markdown summary. Expects ./duet (linux/amd64),
# gcloud authenticated, jq, and TF_VAR_project set.
#
# Two parts. The first drives duet's log directly and injects faults,
# including hypervisor suspend and reset. The second puts the same load
# through the etcd API on duet (two nodes) and on the control, a
# three-member etcd cluster, and injects the same faults into each.
# Expects ./etcd and ./etcdctl as well for the control.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${TF_VAR_project:?set TF_VAR_project}"
TTL="${DUET_TTL:-5s}"
DEV=/dev/disk/by-id/google-duet-shared
SSH=(--project "$PROJECT" --tunnel-through-iap --quiet)

tf="$(terraform output -json)"
NAME=("$(jq -r '.nodes.value[0].name' <<<"$tf")" "$(jq -r '.nodes.value[1].name' <<<"$tf")")
ZONE=("$(jq -r '.nodes.value[0].zone' <<<"$tf")" "$(jq -r '.nodes.value[1].zone' <<<"$tf")")
IP=("$(jq -r '.nodes.value[0].ip' <<<"$tf")" "$(jq -r '.nodes.value[1].ip' <<<"$tf")")
CLIENT="$(jq -r '.client.value.name' <<<"$tf")"
CZONE="$(jq -r '.client.value.zone' <<<"$tf")"
EP="http://${IP[0]}:7000,http://${IP[1]}:7000"
# Index 2 is the third VM, used only as an etcd member.
NAME[2]="$(jq -r '.third.value.name' <<<"$tf")"
ZONE[2]="$(jq -r '.third.value.zone' <<<"$tf")"
IP[2]="$(jq -r '.third.value.ip' <<<"$tf")"

rsh() { # rsh NAME ZONE COMMAND...
  local name=$1 zone=$2
  shift 2
  gcloud compute ssh "$name" --zone "$zone" "${SSH[@]}" --command "$*"
}
node() { rsh "${NAME[$1]}" "${ZONE[$1]}" "${@:2}"; }
client() { rsh "$CLIENT" "$CZONE" "$@"; }

wait_ssh() { # new instances take a while to accept SSH
  for _ in $(seq 1 40); do
    if rsh "$1" "$2" true >/dev/null 2>&1; then return 0; fi
    sleep 10
  done
  echo "no SSH to $1 after 400s" >&2
  return 1
}

install_duet() {
  gcloud compute scp ./duet "$1:/tmp/duet" --zone "$2" "${SSH[@]}"
  rsh "$1" "$2" "sudo install -m 0755 /tmp/duet /usr/local/bin/duet"
}

start_node() {
  node "$1" "sudo systemctl reset-failed duet-node 2>/dev/null; sudo systemd-run --unit duet-node --collect \
    /usr/local/bin/duet run -device $DEV -node $1 -listen 0.0.0.0:7000"
}

owner() { # prints 0 or 1, the index of the current owner
  for _ in $(seq 1 60); do
    for i in 0 1; do
      if client "curl -s --max-time 2 http://${IP[$i]}:7000/status" 2>/dev/null | jq -e '.owner' >/dev/null 2>&1; then
        echo "$i"
        return 0
      fi
    done
    sleep 2
  done
  echo "no owner found" >&2
  return 1
}

# Whatever happens, keep each node's own log; it is the only record of why a
# node misbehaved once the VMs are gone.
collect_logs() {
  for i in 0 1 2; do
    node "$i" "sudo journalctl -u duet-node -u etcd-member --no-pager -o short-precise | tail -n 600" > "node-$i.log" 2>&1 || true
  done
}
trap collect_logs EXIT

results='[]'
scenario() { # scenario NAME SECONDS WORKERS FAULT_FUNCTION
  local name=$1 secs=$2 workers=$3 fault=$4 first
  echo "=== scenario: $name"
  first="$(owner)"
  client "setsid nohup duet load -endpoints $EP -duration ${secs}s -workers $workers \
    -acks /tmp/acks-$name.txt > /tmp/load-$name.json 2> /tmp/load-$name.err < /dev/null &"
  sleep 15
  "$fault"
  sleep "$((secs + 5))"
  local summary verify ok=true
  summary="$(client "cat /tmp/load-$name.json" 2>/dev/null || true)"
  jq -e . >/dev/null 2>&1 <<<"$summary" || summary='{}' 
  verify="$(client "duet verify -endpoints $EP -acks /tmp/acks-$name.txt -wait 180s" 2>&1)" || ok=false
  echo "$summary"
  echo "$verify"
  results="$(jq --arg n "$name" --argjson s "$summary" --arg v "$verify" --argjson ok "$ok" \
--arg o "$first" \
    '. + [{scenario: $n, owner_at_start: $o, verified: $ok, verify: $v, load: $s}]' <<<"$results")"
}

fault_none() { :; }
fault_freeze() { # owner frozen for four lease periods, then resumed
  local o
  o="$(owner)"
  node "$o" "sudo systemctl kill -s STOP duet-node"
  sleep 20
  node "$o" "sudo systemctl kill -s CONT duet-node"
}
fault_kill() { # owner process killed, restarted ten seconds later
  local o
  o="$(owner)"
  node "$o" "sudo systemctl kill -s KILL duet-node" || true
  sleep 10
  start_node "$o"
}
fault_handover() { # planned stop: the owner releases the lease
  local o
  o="$(owner)"
  node "$o" "sudo systemctl stop duet-node"
  sleep 5
  start_node "$o"
}
fault_vm_suspend() { # the owner's whole VM is suspended by the hypervisor, then resumed
  local o
  o="$(owner)"
  if ! gcloud compute instances suspend "${NAME[$o]}" --zone "${ZONE[$o]}" --project "$PROJECT" --quiet; then
    echo "suspend not available for this instance; scenario ran with no fault" >&2
    return 0
  fi
  sleep 25
  gcloud compute instances resume "${NAME[$o]}" --zone "${ZONE[$o]}" --project "$PROJECT" --quiet
}
fault_vm_reset() { # the owner's VM is hard reset with no shutdown, as when a host is lost
  local o
  o="$(owner)"
  gcloud compute instances reset "${NAME[$o]}" --zone "${ZONE[$o]}" --project "$PROJECT" --quiet
  sleep 30
  wait_ssh "${NAME[$o]}" "${ZONE[$o]}"
  start_node "$o"
}

for i in 0 1; do wait_ssh "${NAME[$i]}" "${ZONE[$i]}"; done
wait_ssh "$CLIENT" "$CZONE"
for i in 0 1; do install_duet "${NAME[$i]}" "${ZONE[$i]}"; done
install_duet "$CLIENT" "$CZONE"

node 0 "sudo duet format -device $DEV -nodes 2 -ttl $TTL -epoch-slots 1024 -max-batch 1048576"
start_node 0
start_node 1
owner >/dev/null

scenario baseline 60 8 fault_none
scenario throughput 60 128 fault_none
scenario handover 60 8 fault_handover
scenario freeze 90 8 fault_freeze
scenario kill 90 8 fault_kill
scenario vm_suspend 120 8 fault_vm_suspend
scenario vm_reset 180 8 fault_vm_reset

# ---- Part two: the etcd API, on duet and on the etcd control ----

node 0 "sudo systemctl stop duet-node" || true
node 1 "sudo systemctl stop duet-node" || true
wait_ssh "${NAME[2]}" "${ZONE[2]}"
for i in 0 1 2; do
  gcloud compute scp ./etcd ./etcdctl "${NAME[$i]}:/tmp/" --zone "${ZONE[$i]}" "${SSH[@]}"
  node "$i" "sudo install -m 0755 /tmp/etcd /tmp/etcdctl /usr/local/bin/"
done
gcloud compute scp ./etcdctl "$CLIENT:/tmp/etcdctl" --zone "$CZONE" "${SSH[@]}"
client "sudo install -m 0755 /tmp/etcdctl /usr/local/bin/etcdctl"

BACKEND=duet
members() { if [ "$BACKEND" = duet ]; then echo 0 1; else echo 0 1 2; fi; }
unit() { if [ "$BACKEND" = duet ]; then echo duet-node; else echo etcd-member; fi; }
api_endpoints() { local out=""; for i in $(members); do out="$out${out:+,}${IP[$i]}:2379"; done; echo "$out"; }
start_member() {
  if [ "$BACKEND" = duet ]; then
    node "$1" "sudo systemctl reset-failed duet-node 2>/dev/null; sudo rm -rf /dev/shm/duet; sudo mkdir -p /dev/shm/duet; sudo systemd-run --unit duet-node --collect \
      /usr/local/bin/duet run -device $DEV -node $1 -listen 0.0.0.0:7000 -etcd 0.0.0.0:2379 \
      -etcd-peers ${IP[0]}:2379,${IP[1]}:2379 -state-dir /dev/shm/duet -watch-progress-interval 5s"
  else
    node "$1" "sudo systemctl reset-failed etcd-member 2>/dev/null; sudo systemd-run --unit etcd-member --collect \
      /usr/local/bin/etcd --name m$1 --data-dir /var/lib/etcd-lab \
      --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://${IP[$1]}:2379 \
      --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://${IP[$1]}:2380 \
      --initial-cluster m0=http://${IP[0]}:2380,m1=http://${IP[1]}:2380,m2=http://${IP[2]}:2380 \
      --initial-cluster-state new --quota-backend-bytes 8589934592 --watch-progress-notify-interval 5s"
  fi
}
leader() { # index of the member that currently leads
  if [ "$BACKEND" = duet ]; then owner; return; fi
  local ep
  for _ in $(seq 1 30); do
    ep="$(client "etcdctl --endpoints=$(api_endpoints) --command-timeout=2s endpoint status -w json" 2>/dev/null |
      jq -r '.[] | select(.Status.leader == .Status.header.member_id) | .Endpoint' 2>/dev/null | head -n1)"
    for i in 0 1 2; do
      if [ "$ep" = "${IP[$i]}:2379" ]; then echo "$i"; return 0; fi
    done
    sleep 2
  done
  echo "no etcd leader found" >&2
  return 1
}

api_results='[]'
api_scenario() { # api_scenario NAME SECONDS WORKERS FAULT_FUNCTION
  local name=$1 secs=$2 workers=$3 fault=$4 tag="$BACKEND-$1" first
  echo "=== api scenario: $BACKEND $name"
  first="$(leader)"
  client "setsid nohup duet etcdload -endpoints $(api_endpoints) -duration ${secs}s -workers $workers \
    -acks /tmp/acks-$tag.txt > /tmp/load-$tag.json 2> /tmp/load-$tag.err < /dev/null &"
  sleep 15
  "$fault"
  sleep "$((secs + 5))"
  local summary verify ok=true
  summary="$(client "cat /tmp/load-$tag.json" 2>/dev/null || true)"
  jq -e . >/dev/null 2>&1 <<<"$summary" || summary='{}'
  verify="$(client "duet etcdverify -endpoints $(api_endpoints) -acks /tmp/acks-$tag.txt -wait 180s 2>/dev/null")" || ok=false
  echo "$summary"
  echo "$verify"
  api_results="$(jq --arg b "$BACKEND" --arg n "$name" --argjson s "$summary" --arg v "$verify" --argjson ok "$ok" --arg o "$first" \
    '. + [{backend: $b, scenario: $n, leader_at_start: $o, verified: $ok, verify: $v, load: $s}]' <<<"$api_results")"
}

api_kill() { # leader process killed, restarted ten seconds later
  local l
  l="$(leader)"
  node "$l" "sudo systemctl kill -s KILL $(unit)" || true
  sleep 10
  start_member "$l"
}
api_stop() { # planned stop of the leader
  local l
  l="$(leader)"
  node "$l" "sudo systemctl stop $(unit)"
  sleep 5
  start_member "$l"
}
api_zone_down() { # every member in one zone killed for 40 s: the worst zone for each system
  local down
  if [ "$BACKEND" = duet ]; then
    down="$(leader)" # the zone holding the owner
  else
    down="0 2" # the zone holding two of the three members
  fi
  for i in $down; do node "$i" "sudo systemctl kill -s KILL $(unit)" || true; done
  sleep 40
  for i in $down; do start_member "$i"; done
}

for BACKEND in duet etcd; do
  if [ "$BACKEND" = duet ]; then
    node 0 "sudo duet format -device $DEV -nodes 2 -ttl $TTL -epoch-slots 1024 -max-batch 1048576 -checkpoint-size 2147483648"
  fi
  for i in $(members); do start_member "$i"; done
  leader >/dev/null
  api_scenario baseline 60 8 fault_none
  api_scenario throughput 60 128 fault_none
  api_scenario stop 60 8 api_stop
  api_scenario kill 90 8 api_kill
  api_scenario zone_down 120 8 api_zone_down
  for i in $(members); do node "$i" "sudo systemctl stop $(unit)" || true; done
done

jq --arg ttl "$TTL" --argjson api "$api_results" '{lease: $ttl, storage: "GCP Hyperdisk Balanced High Availability, multi-writer, two zones", scenarios: ., api: $api}' \
  <<<"$results" > results.json

{
  echo "## duet lab on Google Cloud"
  echo
  echo "Lease $TTL. Two nodes in two zones on one regional multi-writer disk."
  echo
  echo "| Scenario | Owner at start | Verified | Acknowledged | Writes/s | p50 ms | p99 ms | Longest gap ms |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- |"
  jq -r '.scenarios[] | "| \(.scenario) | \(.owner_at_start) | \(.verified) | \(.load.acknowledged // "?") | \((.load.writes_per_second // 0) | floor) | \(.load.latency_ms_p50 // "?") | \(.load.latency_ms_p99 // "?") | \(.load.longest_gap_ms // "?") |"' results.json
  echo
  echo "## etcd API: duet on two nodes against etcd on three"
  echo
  echo "Same load through the stock etcd client. etcd has two members in one zone and one in the other; the load generator is in the zone with one. zone_down kills every member in the worst zone for each system for 40 s."
  echo
  echo "| Backend | Scenario | Leader at start | Verified | Acknowledged | Errors | Writes/s | p50 ms | p99 ms | Longest gap ms |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  jq -r '.api[] | "| \(.backend) | \(.scenario) | \(.leader_at_start) | \(.verified) | \(.load.acknowledged // "?") | \(.load.errors // "?") | \((.load.writes_per_second // 0) | floor) | \(.load.latency_ms_p50 // "?") | \(.load.latency_ms_p99 // "?") | \(.load.longest_gap_ms // "?") |"' results.json
} | tee summary.md

jq -e 'all(.scenarios[]; .verified) and all(.api[]; .verified)' results.json >/dev/null
