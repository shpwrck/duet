# Sourced by the scripts here. Starts a datastore (BACKEND=duet: two duet
# nodes on one disk image, 2 s lease; BACKEND=etcd: three etcd members,
# default timings) and a real kube-apiserver on top of it. Provides
# node N (start member N), leader (index of the leading member), K (kubectl
# command array), W (scratch directory) and KUBECONFIG_FILE.
KUBE_BIN=${KUBE_BIN:?directory holding kube-apiserver and kubectl}
BACKEND=${BACKEND:-duet}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
W=$(mktemp -d)
trap '[ -n "${KEEP:-}" ] && exit; pkill -9 -f "$W/" 2>/dev/null || true; sleep 0.2; pkill -9 -f "$W/" 2>/dev/null || true' EXIT

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
cat >"$W/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters: [{name: lab, cluster: {server: "https://127.0.0.1:6443", insecure-skip-tls-verify: true}}]
users: [{name: admin, user: {token: testtoken}}]
contexts: [{name: lab, context: {cluster: lab, user: admin}}]
current-context: lab
KC
KUBECONFIG_FILE="$W/kubeconfig"
