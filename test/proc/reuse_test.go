package proc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func lapsOf(p *proc) float64 {
	resp, err := (&http.Client{Timeout: time.Second}).Get(p.url + "/status")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var st struct{ Laps float64 }
	json.NewDecoder(resp.Body).Decode(&st)
	return st.Laps
}

// The log is 8 MiB and the test writes many times that through the etcd
// API: across an owner kill and restart, past a standby that is frozen
// until it has been overtaken, and past an owner that dies and never
// returns, which must stop the log being reused until the node is declared
// gone.
func TestLogReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t, "-log-size", "8388608", "-checkpoint-size", "33554432")
	eps := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	// Node 2 never stands for ownership and clients are not told about it,
	// so it can be frozen later without any client hanging on it.
	start := func(i int) *proc {
		args := []string{"-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", dirs[i], "-checkpoint-every", "300"}
		if i == 2 {
			args = append(args, "-campaign=false")
		}
		return c.start(i, addrs[i], args...)
	}
	clientEps := eps[:2]
	for i := 0; i < 3; i++ {
		c.nodes = append(c.nodes, start(i))
	}
	alive := []bool{true, true, true}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	c.owner()

	// Writers go round a fixed set of keys, as a real cluster rewrites the
	// same objects, so the state stays small while the log goes round many
	// times. Each value carries its sequence number; the newest
	// acknowledged one per key is recorded.
	const workers = 8
	padding := strings.Repeat("v", 1016)
	var mu sync.Mutex
	acked := map[string]int{}
	var lastErr atomic.Value
	var lastAck atomic.Int64
	lastAck.Store(time.Now().UnixNano())
	var want atomic.Int64 // writers run while fewer than this many keys are acknowledged
	var count atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: clientEps, DialTimeout: 2 * time.Second})
			if err != nil {
				t.Error(err)
				return
			}
			defer cli.Close()
			for i := 0; ; {
				select {
				case <-stop:
					return
				default:
				}
				if count.Load() >= want.Load() {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				key := fmt.Sprintf("/r/%d/%03d", w, i%400)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := cli.Put(ctx, key, fmt.Sprintf("%08d%s", i, padding))
				cancel()
				if err != nil {
					lastErr.Store(err.Error())
					time.Sleep(20 * time.Millisecond)
					continue // the same key again: a put is safe to repeat
				}
				mu.Lock()
				acked[key] = i
				mu.Unlock()
				count.Add(1)
				lastAck.Store(time.Now().UnixNano())
				i++
			}
		}(w)
	}
	defer func() { close(stop); wg.Wait() }()
	// Compact old revisions as kube-apiserver does, or the state would keep
	// every value ever written.
	go func() {
		cli, err := clientv3.New(clientv3.Config{Endpoints: clientEps, DialTimeout: 2 * time.Second})
		if err != nil {
			return
		}
		defer cli.Close()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if g, err := cli.Get(ctx, "/r/0/000"); err == nil && g.Header.Revision > 200 {
				cli.Compact(ctx, g.Header.Revision-100)
			}
			cancel()
		}
	}()
	// writeUntil lets the writers run until done reports true, and fails if
	// nothing is acknowledged for stall.
	writeUntil := func(what string, stall time.Duration, done func() bool) bool {
		t.Helper()
		lastAck.Store(time.Now().UnixNano())
		for !done() {
			want.Store(count.Load() + 200)
			time.Sleep(50 * time.Millisecond)
			if time.Since(time.Unix(0, lastAck.Load())) > stall {
				want.Store(0)
				return false
			}
		}
		want.Store(0)
		time.Sleep(100 * time.Millisecond)
		return true
	}
	maxLaps := func() float64 {
		m := 0.0
		for i, p := range c.nodes {
			if alive[i] {
				if l := lapsOf(p); l > m {
					m = l
				}
			}
		}
		return m
	}
	began := time.Now()
	must := func(what string, ok bool) {
		t.Helper()
		t.Logf("%-28s reached at %5.1fs: %d writes, %.1f laps", what, time.Since(began).Seconds(), count.Load(), maxLaps())
		if !ok {
			e, _ := lastErr.Load().(string)
			t.Fatalf("%s: writes stalled at %.1f laps, %d acknowledged; last error: %s", what, maxLaps(), count.Load(), e)
		}
	}
	verify := func(what string) {
		t.Helper()
		cli, err := clientv3.New(clientv3.Config{Endpoints: clientEps, DialTimeout: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		found := map[string]int{}
		key, end := "/r/", clientv3.GetPrefixRangeEnd("/r/")
		for deadline := time.Now().Add(30 * time.Second); ; {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(500))
			cancel()
			if err != nil {
				if time.Now().After(deadline) {
					t.Fatalf("%s: cannot read back: %v", what, err)
				}
				time.Sleep(200 * time.Millisecond)
				continue
			}
			for _, kv := range resp.Kvs {
				var seq int
				fmt.Sscanf(string(kv.Value[:8]), "%d", &seq)
				found[string(kv.Key)] = seq
			}
			if !resp.More {
				break
			}
			key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
		}
		mu.Lock()
		defer mu.Unlock()
		for k, seq := range acked {
			if got, ok := found[k]; !ok || got < seq {
				t.Fatalf("%s: key %s was acknowledged at write %d but holds %d (present: %v)", what, k, seq, got, ok)
			}
		}
		t.Logf("%s: %d writes acknowledged, all %d keys hold their newest acknowledged value, %.1f laps of the log", what, count.Load(), len(acked), maxLaps())
	}

	// 1. Round the log, with the owner killed and restarted on the way.
	must("first lap", writeUntil("first lap", 10*time.Second, func() bool { return maxLaps() >= 0.6 }))
	o := c.owner()
	o.cmd.Process.Kill()
	o.cmd.Wait()
	alive[o.id] = false
	must("after owner kill", writeUntil("after owner kill", 10*time.Second, func() bool { return maxLaps() >= 0.9 }))
	c.nodes[o.id] = start(o.id)
	alive[o.id] = true
	must("three laps", writeUntil("three laps", 10*time.Second, func() bool { return maxLaps() >= 3 }))
	verify("after three laps, an owner kill and its restart")

	// 2. A standby is frozen while the others go round the log. When it
	// wakes it must find it has been overtaken, exit, and come back from a
	// checkpoint.
	z := c.nodes[2]
	z.cmd.Process.Signal(syscall.SIGSTOP)
	alive[2] = false // do not ask a frozen process for its status
	base := maxLaps()
	must("past the frozen standby", writeUntil("past the frozen standby", 10*time.Second, func() bool { return maxLaps() >= base+1.5 }))
	z.cmd.Process.Signal(syscall.SIGCONT)
	exited := make(chan error, 1)
	go func() { exited <- z.cmd.Wait() }()
	select {
	case err := <-exited:
		if err == nil {
			t.Fatal("the overtaken standby exited cleanly; it should report that it was overtaken")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the overtaken standby kept running")
	}
	c.nodes[z.id] = start(z.id)
	alive[2] = true
	var st nodeStatus
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var err error
		if st, err = c.nodes[z.id].status(); err == nil && st.Restored > 0 && lapsOf(c.nodes[z.id]) >= maxLaps()-0.2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the overtaken standby did not come back from a checkpoint: %+v", st)
		}
	}
	t.Logf("overtaken standby exited, restarted from the checkpoint at index %d and caught up", st.Restored)

	// 3. The owner dies and never returns. The others must not reuse the
	// space its last write may still be aimed at, so the log fills. Once
	// the node is declared gone, writing continues.
	o = c.owner()
	o.cmd.Process.Kill()
	o.cmd.Wait()
	alive[o.id] = false
	base = maxLaps()
	if writeUntil("past the dead owner", 8*time.Second, func() bool { return maxLaps() >= base+2.5 }) {
		t.Fatalf("the log was reused for %.1f laps although the dead owner was never retired", maxLaps()-base)
	}
	if e, _ := lastErr.Load().(string); !strings.Contains(e, "full") {
		t.Fatalf("writes stopped, but not because the log is full: %s", e)
	}
	blocked := maxLaps() - base
	verify("log full behind a dead owner")
	out, err := exec.Command(c.bin, "retire", "-device", c.device, "-node", fmt.Sprint(o.id), "-confirmed-gone", c.direct).CombinedOutput()
	if err != nil {
		t.Fatalf("retire: %v\n%s", err, out)
	}
	base = maxLaps()
	must("after retire", writeUntil("after retire", 15*time.Second, func() bool { return maxLaps() >= base+2 }))
	verify(fmt.Sprintf("after the dead owner was declared gone (the log had stopped %.1f laps on)", blocked))
}

// A state too large for a checkpoint slot must be reported and survived,
// not crash the node.
func TestCheckpointTooLargeIsSurvived(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t, "-checkpoint-size", "65536")
	eps := []string{freeAddr(t), freeAddr(t)}
	for i := 0; i < 2; i++ {
		c.nodes = append(c.nodes, c.start(i, freeAddr(t), "-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", t.TempDir(), "-checkpoint-every", "100"))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	c.owner()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	value := strings.Repeat("v", 1024)
	for i := 0; i < 1500; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cli.Put(ctx, fmt.Sprintf("/big/%05d", i), value)
		cancel()
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	time.Sleep(time.Second)
	for _, p := range c.nodes {
		st, err := p.status()
		if err != nil {
			t.Fatalf("node %d is gone: %v", p.id, err)
		}
		if st.Applied < 1500 || st.Checkpoint != 0 {
			t.Fatalf("node %d: %+v", p.id, st)
		}
	}
}
