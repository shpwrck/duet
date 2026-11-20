#!/usr/bin/env bash
# Runs the lab scenarios against an applied Terraform configuration and
# writes results.json plus a Markdown summary. Expects ./duet (linux/amd64),
# gcloud authenticated, jq, and TF_VAR_project set.
#
# NOT YET RUN on Google Cloud. Expect fixes on the first real run.
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

results='[]'
scenario() { # scenario NAME SECONDS WORKERS FAULT_FUNCTION
  local name=$1 secs=$2 workers=$3 fault=$4
  echo "=== scenario: $name"
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
    '. + [{scenario: $n, verified: $ok, verify: $v, load: $s}]' <<<"$results")"
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

jq --arg ttl "$TTL" '{lease: $ttl, storage: "GCP Hyperdisk Balanced High Availability, multi-writer, two zones", scenarios: .}' \
  <<<"$results" > results.json

{
  echo "## duet lab on Google Cloud"
  echo
  echo "Lease $TTL. Two nodes in two zones on one regional multi-writer disk."
  echo
  echo "| Scenario | Verified | Acknowledged | Writes/s | p50 ms | p99 ms | Longest gap ms |"
  echo "| --- | --- | --- | --- | --- | --- | --- |"
  jq -r '.scenarios[] | "| \(.scenario) | \(.verified) | \(.load.acknowledged // "?") | \((.load.writes_per_second // 0) | floor) | \(.load.latency_ms_p50 // "?") | \(.load.latency_ms_p99 // "?") | \(.load.longest_gap_ms // "?") |"' results.json
} | tee summary.md

jq -e 'all(.scenarios[]; .verified)' results.json >/dev/null
