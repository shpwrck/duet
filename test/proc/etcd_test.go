package proc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Counters incremented by compare-and-swap through the stock etcd client
// while the owner is killed. A counter may never fall behind the number of
// acknowledged increments, and may run ahead only by increments whose
// outcome the client never learned.
func TestEtcdClientAcrossKill(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	var eps []string
	for i := 0; i < 3; i++ {
		ep := freeAddr(t)
		eps = append(eps, ep)
		c.nodes = append(c.nodes, c.start(i, freeAddr(t), "-etcd", ep, "-state-dir", t.TempDir()))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	first := c.owner()

	// One watcher on every counter, opened before the first write. The
	// client resumes it on the new owner from the last revision it saw.
	wcli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer wcli.Close()
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	var seenMu sync.Mutex
	seen := map[string][]int{}
	go func() {
		for r := range wcli.Watch(clientv3.WithRequireLeader(wctx), "/counter/", clientv3.WithPrefix(), clientv3.WithRev(1)) {
			seenMu.Lock()
			for _, e := range r.Events {
				v, _ := strconv.Atoi(string(e.Kv.Value))
				seen[string(e.Kv.Key)] = append(seen[string(e.Kv.Key)], v)
			}
			seenMu.Unlock()
		}
	}()
	// A key on a two-second lease that nobody keeps alive must disappear,
	// even though the node that granted the lease is killed first.
	lg, err := wcli.Grant(wctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wcli.Put(wctx, "/leased", "x", clientv3.WithLease(lg.ID)); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	stop := make(chan struct{})
	acked, unknown := make([]int, workers), make([]int, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
			if err != nil {
				t.Error(err)
				return
			}
			defer cli.Close()
			key := fmt.Sprintf("/counter/%d", w)
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				get, err := cli.Get(ctx, key)
				if err != nil {
					cancel()
					time.Sleep(20 * time.Millisecond)
					continue
				}
				val, mod := 0, int64(0)
				if len(get.Kvs) == 1 {
					val, _ = strconv.Atoi(string(get.Kvs[0].Value))
					mod = get.Kvs[0].ModRevision
				}
				resp, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", mod)).
					Then(clientv3.OpPut(key, strconv.Itoa(val+1))).Commit()
				cancel()
				switch {
				case err != nil:
					unknown[w]++
				case resp.Succeeded:
					acked[w]++
				}
			}
		}(w)
	}

	time.Sleep(time.Second)
	first.cmd.Process.Kill()
	first.cmd.Wait()
	t.Logf("killed owner, node %d", first.id)
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	total := 0
	for w := 0; w < workers; w++ {
		var val int
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			get, err := cli.Get(ctx, fmt.Sprintf("/counter/%d", w))
			cancel()
			if err == nil && len(get.Kvs) == 1 {
				val, _ = strconv.Atoi(string(get.Kvs[0].Value))
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("counter %d unreadable: %v", w, err)
			}
		}
		if val < acked[w] || val > acked[w]+unknown[w] {
			t.Errorf("counter %d = %d, with %d acknowledged and %d unknown increments", w, val, acked[w], unknown[w])
		}
		total += acked[w]
		// The watcher saw every value of this counter once, in order.
		key := fmt.Sprintf("/counter/%d", w)
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			seenMu.Lock()
			got := append([]int(nil), seen[key]...)
			seenMu.Unlock()
			if len(got) >= val || time.Now().After(deadline) {
				if len(got) != val {
					t.Errorf("watch on %s saw %d events, want %d", key, len(got), val)
				}
				for i, v := range got {
					if v != i+1 {
						t.Errorf("watch on %s: event %d carries %d", key, i, v)
						break
					}
				}
				break
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if g, err := cli.Get(ctx, "/leased"); err != nil || g.Count != 0 {
		t.Errorf("leased key still present after its lease ran out: %v count %d", err, func() int64 {
			if g == nil {
				return -1
			}
			return g.Count
		}())
	}
	if total < 100 {
		t.Errorf("only %d acknowledged increments", total)
	}
	t.Logf("%d acknowledged increments across the kill", total)
}
