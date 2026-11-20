package proc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	eps := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	for i := 0; i < 3; i++ {
		c.nodes = append(c.nodes, c.start(i, freeAddr(t), "-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", t.TempDir()))
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
	// The new owner extends every lease by one disk-lease period when it
	// takes over, so allow for that before calling the key stuck.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		g, err := cli.Get(ctx, "/leased")
		cancel()
		if err == nil && g.Count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("leased key still present 10s after the run: %v", err)
			break
		}
	}
	t.Logf("%d acknowledged increments across the kill", total)
}

type nodeStatus struct {
	Owner                         bool
	Applied, Checkpoint, Restored uint64
}

func (p *proc) status() (nodeStatus, error) {
	var st nodeStatus
	resp, err := (&http.Client{Timeout: time.Second}).Get(p.url + "/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// A node that restarts takes the newest checkpoint from the disk, replays
// only the log after it, and can then serve everything as the owner.
func TestRestartFromCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	eps := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	var addrs, dirs []string
	args := func(i int) []string {
		return []string{"-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", dirs[i], "-checkpoint-every", "1500"}
	}
	for i := 0; i < 3; i++ {
		addrs, dirs = append(addrs, freeAddr(t)), append(dirs, t.TempDir())
		c.nodes = append(c.nodes, c.start(i, addrs[i], args(i)...))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	owner := c.owner()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	put := func(from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			// The same put again is harmless here only because nothing
			// else writes these keys; revisions are checked at the end.
			var err error
			for try := 0; try < 3; try++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = cli.Put(ctx, fmt.Sprintf("/k/%05d", i), strconv.Itoa(i))
				cancel()
				if err == nil {
					break
				}
			}
			if err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
	}
	put(0, 2000)

	// Restart a node that is not the owner once any node has checkpointed.
	victim := c.nodes[(owner.id+1)%3]
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		done := false
		for _, p := range c.nodes {
			if st, err := p.status(); err == nil && st.Checkpoint > 0 {
				done = true
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no checkpoint within 10s")
		}
	}
	victim.cmd.Process.Kill()
	victim.cmd.Wait()
	put(2000, 2300)
	c.nodes[victim.id] = c.start(victim.id, addrs[victim.id], args(victim.id)...)
	victim = c.nodes[victim.id]

	want, err := owner.status()
	if err != nil {
		t.Fatal(err)
	}
	var st nodeStatus
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if st, err = victim.status(); err == nil && st.Applied >= want.Applied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %+v, want applied %d (%v)", st, want.Applied, err)
		}
	}
	// With a checkpoint every 1500 records, the only checkpoints on disk
	// were taken between 1500 and 2000, so the last 300 writes at least
	// must come from the log.
	if st.Restored < 1500 || st.Restored > 2001 || st.Applied == st.Restored {
		t.Fatalf("restarted node should have used a checkpoint and then the log: %+v", st)
	}
	t.Logf("restarted node started from the checkpoint at index %d and replayed %d records", st.Restored, st.Applied-st.Restored)

	// Leave only the restarted node, and read everything back through it.
	for _, p := range c.nodes {
		if p != victim {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}
	var get *clientv3.GetResponse
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		get, err = cli.Get(ctx, "/k/", clientv3.WithPrefix())
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node never served: %v", err)
		}
	}
	if len(get.Kvs) != 2300 {
		t.Fatalf("restarted node has %d keys, want 2300", len(get.Kvs))
	}
	for i, kv := range get.Kvs {
		if string(kv.Key) != fmt.Sprintf("/k/%05d", i) || string(kv.Value) != strconv.Itoa(i) || kv.ModRevision != int64(i+2) {
			t.Fatalf("key %d: %s=%s at revision %d", i, kv.Key, kv.Value, kv.ModRevision)
		}
	}
}

// A client that knows only one node, which never owns the log, still gets
// linearizable reads, writes and watch events, across a change of owner.
func TestClientPinnedToFollower(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	eps := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	for i := 0; i < 3; i++ {
		args := []string{"-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", t.TempDir()}
		if i == 2 {
			args = append(args, "-campaign=false") // node 2 never owns the log
		}
		c.nodes = append(c.nodes, c.start(i, freeAddr(t), args...))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	first := c.owner()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps[2:], DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	wctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var seen []int
	go func() {
		for r := range cli.Watch(wctx, "/n", clientv3.WithRev(1)) {
			mu.Lock()
			for _, e := range r.Events {
				v, _ := strconv.Atoi(string(e.Kv.Value))
				seen = append(seen, v)
			}
			mu.Unlock()
		}
	}()
	// Each step reads the value back before writing the next, so a stale
	// read from the follower's own state would break the sequence.
	n, worst := 0, time.Duration(0)
	step := func() {
		start := time.Now()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			get, err := cli.Get(ctx, "/n")
			if err == nil {
				val, mod := 0, int64(0)
				if len(get.Kvs) == 1 {
					val, _ = strconv.Atoi(string(get.Kvs[0].Value))
					mod = get.Kvs[0].ModRevision
				}
				if val < n {
					cancel()
					t.Fatalf("read %d after writing %d: a stale read", val, n)
				}
				if val > n {
					n = val // an earlier attempt landed although the client saw an error
					cancel()
					break
				}
				var resp *clientv3.TxnResponse
				resp, err = cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/n"), "=", mod)).
					Then(clientv3.OpPut("/n", strconv.Itoa(n+1))).Commit()
				if err == nil && resp.Succeeded {
					n++
					cancel()
					break
				}
			}
			cancel()
			if time.Since(start) > 20*time.Second {
				t.Fatalf("no progress through the follower for 20s: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
	for n < 300 {
		step()
	}
	first.cmd.Process.Kill()
	first.cmd.Wait()
	for target := n + 300; n < target; {
		step()
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		mu.Lock()
		got := append([]int(nil), seen...)
		mu.Unlock()
		if len(got) >= n || time.Now().After(deadline) {
			if len(got) != n {
				t.Fatalf("watch through the follower saw %d events, want %d", len(got), n)
			}
			for i, v := range got {
				if v != i+1 {
					t.Fatalf("watch event %d carries %d", i, v)
				}
			}
			break
		}
	}
	t.Logf("%d steps through a follower across an owner kill; longest step %s", n, worst.Round(10*time.Millisecond))
}

// The two nodes cannot reach each other, but both reach the disk. The
// standby cannot forward, so only the owner can serve. Ownership must not
// move while the owner is alive, a client that knows both nodes must keep
// working, and when the owner then dies the standby must take over.
func TestNodesCannotReachEachOther(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	eps := []string{freeAddr(t), freeAddr(t)}
	dead := []string{freeAddr(t), freeAddr(t)} // nothing listens here: forwarding goes nowhere
	for i := 0; i < 2; i++ {
		c.nodes = append(c.nodes, c.start(i, freeAddr(t), "-etcd", eps[i], "-etcd-peers", strings.Join(dead, ","), "-state-dir", t.TempDir()))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	first := c.owner()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	n, failed, worst := 0, 0, time.Duration(0)
	step := func() {
		start := time.Now()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			get, err := cli.Get(ctx, "/n")
			if err == nil {
				val, mod := 0, int64(0)
				if len(get.Kvs) == 1 {
					val, _ = strconv.Atoi(string(get.Kvs[0].Value))
					mod = get.Kvs[0].ModRevision
				}
				if val < n {
					cancel()
					t.Fatalf("read %d after writing %d: a stale read", val, n)
				}
				if val > n {
					n = val
					cancel()
					break
				}
				var resp *clientv3.TxnResponse
				resp, err = cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/n"), "=", mod)).
					Then(clientv3.OpPut("/n", strconv.Itoa(n+1))).Commit()
				if err == nil && resp.Succeeded {
					n++
					cancel()
					break
				}
			}
			cancel()
			if err != nil {
				failed++
			}
			if time.Since(start) > 30*time.Second {
				t.Fatalf("no progress for 30s with the nodes partitioned: %v", err)
			}
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
	began := time.Now()
	for n < 200 {
		step()
	}
	if o := c.owner(); o.id != first.id {
		t.Fatalf("ownership moved from node %d to node %d although the owner never stopped", first.id, o.id)
	}
	t.Logf("partitioned, owner alive: 200 steps in %s, %d failed calls, slowest step %s",
		time.Since(began).Round(100*time.Millisecond), failed, worst.Round(10*time.Millisecond))

	first.cmd.Process.Kill()
	first.cmd.Wait()
	failed, worst, began = 0, 0, time.Now()
	for target := n + 200; n < target; {
		step()
	}
	t.Logf("owner killed while partitioned: 200 steps in %s, %d failed calls, slowest step %s",
		time.Since(began).Round(100*time.Millisecond), failed, worst.Round(10*time.Millisecond))
}

// Planned stops, over and over: the owner is told to stop, the other node
// takes over, the stopped node comes back. A client that knows both nodes
// should barely notice any of them.
func TestRepeatedPlannedStops(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	eps := []string{freeAddr(t), freeAddr(t)}
	addrs := []string{freeAddr(t), freeAddr(t)}
	dirs := []string{t.TempDir(), t.TempDir()}
	start := func(i int) *proc {
		return c.start(i, addrs[i], "-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", dirs[i])
	}
	for i := 0; i < 2; i++ {
		c.nodes = append(c.nodes, start(i))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	c.owner()
	const workers = 4
	var worst atomic.Int64
	var total atomic.Int64
	stop := make(chan struct{})
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
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				begin := time.Now()
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, err := cli.Put(ctx, fmt.Sprintf("/p/%d", w), strconv.Itoa(i))
					cancel()
					if err == nil {
						break
					}
				}
				total.Add(1)
				if d := int64(time.Since(begin)); d > worst.Load() {
					worst.Store(d)
				}
			}
		}(w)
	}
	var gaps []string
	slowest := time.Duration(0)
	for cycle := 0; cycle < 8; cycle++ {
		time.Sleep(1500 * time.Millisecond)
		o := c.owner()
		worst.Store(0)
		o.cmd.Process.Signal(syscall.SIGTERM)
		o.cmd.Wait()
		time.Sleep(1500 * time.Millisecond)
		d := time.Duration(worst.Load())
		gaps = append(gaps, d.Round(10*time.Millisecond).String())
		if d > slowest {
			slowest = d
		}
		c.nodes[o.id] = start(o.id)
	}
	close(stop)
	wg.Wait()
	t.Logf("%d writes; slowest write around each of 8 planned stops: %s", total.Load(), strings.Join(gaps, ", "))
	if slowest > 1500*time.Millisecond {
		t.Fatalf("a planned stop held a write for %s; the handover itself takes a fraction of a second", slowest)
	}
}
