#!/bin/bash
# usage: BIN=dir soak.sh rounds
# Two duet nodes on one small disk image. Every round puts 20 s of writes
# through the etcd API, injects one fault picked at random, checks that
# every acknowledged write is readable, then empties and compacts the
# store so the state stays bounded while the log wraps many times.
W=$(mktemp -d); trap 'pkill -9 -f "$W/" 2>/dev/null; [ -n "$KEEP" ] && echo "kept $W" || rm -rf "$W"' EXIT
cd "${BIN:?directory holding duet and etcdctl}"; EP=127.0.0.1:23790,127.0.0.1:23791
truncate -s 1300M $W/disk.img
./duet format -device $W/disk.img -nodes 2 -ttl 2s -epoch-slots 4096 -max-batch 1048576 -checkpoint-size 268435456 -direct=true >/dev/null || exit 1
n() { mkdir -p $W/s$1; ./duet run -device $W/disk.img -node $1 -listen 127.0.0.1:710$1 -etcd 127.0.0.1:2379$1 -etcd-peers $EP -state-dir $W/s$1 -checkpoint-every 5000 -direct=true >>$W/n$1.log 2>&1 & echo $! > $W/p$1; }
st() { curl -s --max-time 1 127.0.0.1:710$1/status; }
own() { for i in 0 1; do st $i | grep -q '"owner":true' && { echo $i; return; }; done; echo x; }
n 0; sleep 3; n 1; sleep 1
bad=0; worst=0; total=0
for r in $(seq $1); do
  ./duet etcdload -endpoints $EP -duration 20s -workers 4 -value-size 2048 -acks $W/a$r.txt 2>/dev/null > $W/l$r.out &
  L=$!
  sleep $((4 + RANDOM % 6)); o=$(own); f=$((RANDOM % 5))
  [ $o = x ] && { echo "round $r: no owner"; bad=$((bad+1)); o=0; }
  s=$((1-o))
  case $f in
    0) k="kill owner";    kill -9 $(cat $W/p$o); sleep 3; n $o;;
    1) k="stop owner";    kill -TERM $(cat $W/p$o); sleep 3; n $o;;
    2) k="kill standby";  kill -9 $(cat $W/p$s); sleep 3; n $s;;
    3) k="freeze owner 6s"; kill -STOP $(cat $W/p$o); sleep 6; kill -CONT $(cat $W/p$o);;
    4) k="kill standby, lose its state"; kill -9 $(cat $W/p$s); rm -rf $W/s$s; sleep 2; n $s;;
  esac
  wait $L
  for i in 0 1; do kill -0 $(cat $W/p$i) 2>/dev/null || { n $i; k="$k (node $i had exited, restarted)"; }; done
  g=$(grep -ao '"longest_gap_ms":[0-9]*' $W/l$r.out | cut -d: -f2); a=$(grep -ao '"acknowledged":[0-9]*' $W/l$r.out | cut -d: -f2)
  v=$(./duet etcdverify -endpoints $EP -acks $W/a$r.txt 2>&1 | tail -1)
  total=$((total+a)); [ "${g:-0}" -gt $worst ] && worst=$g
  case "$v" in OK*) ;; *) bad=$((bad+1)); echo "round $r FAILED verify after $k: $v";; esac
  echo "round $r: $k, $a acked, gap ${g} ms"
  rm -f $W/a$r.txt
  rev=$(./etcdctl --endpoints=$EP del "" --from-key -w json 2>/dev/null | grep -ao '"revision":[0-9]*' | cut -d: -f2)
  ./etcdctl --endpoints=$EP compact $rev >/dev/null 2>&1
done
echo "SUMMARY rounds=$1 acked=$total worst_gap_ms=$worst failures=$bad nospace=$(grep -al "space exceeded" $W/l*.out 2>/dev/null | wc -l) laps=$(st 0 | grep -ao '"laps":[0-9.]*' | cut -d: -f2) state=$(du -sh $W/s0 | cut -f1)"
grep -ahi "panic\|lapped\|fatal" $W/n*.log | sort | uniq -c | cut -c1-200 | head -5
