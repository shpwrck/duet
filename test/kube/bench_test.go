package kube

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestBench measures write throughput and latency through the stock etcd
// client against whatever BENCH_ENDPOINTS points at, so the same load can
// be put on duet and on an etcd cluster. Skipped unless that is set.
//
//	BENCH_ENDPOINTS=127.0.0.1:2379,... BENCH_CLIENTS=64 go test -run TestBench -v
func TestBench(t *testing.T) {
	eps := os.Getenv("BENCH_ENDPOINTS")
	if eps == "" {
		t.Skip("BENCH_ENDPOINTS not set")
	}
	clients := 64
	fmt.Sscan(os.Getenv("BENCH_CLIENTS"), &clients)
	const duration = 15 * time.Second
	value := strings.Repeat("x", 1024)
	var mu sync.Mutex
	var lat []time.Duration
	var wg sync.WaitGroup
	stop := time.Now().Add(duration)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(eps, ","), DialTimeout: 5 * time.Second})
			if err != nil {
				t.Error(err)
				return
			}
			defer cli.Close()
			var mine []time.Duration
			for i := 0; time.Now().Before(stop); i++ {
				key := fmt.Sprintf("/bench/%d/%d", c, i%1000)
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				// The write kube-apiserver issues: a transaction guarded
				// by the key's revision.
				_, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), ">", -1)).
					Then(clientv3.OpPut(key, value)).Commit()
				cancel()
				if err != nil {
					t.Errorf("write: %v", err)
					return
				}
				mine = append(mine, time.Since(start))
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if len(lat) == 0 {
		t.Fatal("no writes")
	}
	q := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)].Round(10 * time.Microsecond) }
	t.Logf("%d clients, 1 KiB values: %.0f writes/s, p50 %s, p99 %s", clients, float64(len(lat))/duration.Seconds(), q(0.5), q(0.99))
}
