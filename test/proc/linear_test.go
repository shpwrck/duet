package proc

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type regIn struct {
	write bool
	key   int
	value string
}

type regOut struct {
	value   string
	unknown bool // a write whose outcome the client never learned
}

// registers models each key as a register: a read returns the last value
// written. The history is split by key.
var registers = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		parts := map[int][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(regIn).key
			parts[k] = append(parts[k], op)
		}
		var out [][]porcupine.Operation
		for _, p := range parts {
			out = append(out, p)
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regIn)
		if in.write {
			return true, in.value
		}
		return output.(regOut).value == state.(string), state
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(regIn)
		if in.write {
			return fmt.Sprintf("put(%d, %s)", in.key, in.value)
		}
		return fmt.Sprintf("get(%d) -> %q", in.key, output.(regOut).value)
	},
}

// Clients read and write a few keys while owners are killed, stopped and
// restarted, and every call is recorded with its start and end time. The
// history is then checked for linearizability: there must be one order of
// the operations, consistent with real time, in which every read returns
// the latest write. A stale read from a deposed owner, or a write that was
// acknowledged and then lost, has no such order.
func TestLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	c, _ := setup(t)
	eps := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	start := func(i int) *proc {
		return c.start(i, addrs[i], "-etcd", eps[i], "-etcd-peers", strings.Join(eps, ","), "-state-dir", dirs[i])
	}
	for i := 0; i < 3; i++ {
		c.nodes = append(c.nodes, start(i))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	c.owner()

	const clients, keys = 6, 3
	begin := time.Now()
	var mu sync.Mutex
	var history []porcupine.Operation
	var pending []porcupine.Operation // writes with unknown outcome
	var seq atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(cl) + 1))
			cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second})
			if err != nil {
				t.Error(err)
				return
			}
			defer cli.Close()
			// An operation with an unknown outcome may still take effect
			// later, so the client that issued it must not be reused as
			// if it had finished: give each such client a new identity.
			id := cl
			for {
				select {
				case <-stop:
					return
				default:
				}
				in := regIn{write: rng.Intn(2) == 0, key: rng.Intn(keys)}
				name := fmt.Sprintf("/lin/%d", in.key)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				call := time.Since(begin).Nanoseconds()
				var out regOut
				var err error
				if in.write {
					in.value = fmt.Sprint(seq.Add(1))
					_, err = cli.Put(ctx, name, in.value)
				} else {
					var g *clientv3.GetResponse
					if g, err = cli.Get(ctx, name); err == nil && len(g.Kvs) == 1 {
						out.value = string(g.Kvs[0].Value)
					}
				}
				ret := time.Since(begin).Nanoseconds()
				cancel()
				op := porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret}
				mu.Lock()
				switch {
				case err == nil:
					history = append(history, op)
				case in.write:
					// It may have happened, at any time from now on.
					op.Output = regOut{unknown: true}
					pending = append(pending, op)
					id = clients + int(seq.Add(1))
				}
				mu.Unlock()
				time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
			}
		}(cl)
	}

	// Faults: kill the owner, bring it back; stop the owner on purpose,
	// bring it back; kill the owner again.
	for round := 0; round < 3; round++ {
		time.Sleep(2 * time.Second)
		o := c.owner()
		if round == 1 {
			o.cmd.Process.Signal(syscall.SIGTERM)
		} else {
			o.cmd.Process.Kill()
		}
		o.cmd.Wait()
		time.Sleep(2500 * time.Millisecond)
		c.nodes[o.id] = start(o.id)
	}
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	end := time.Since(begin).Nanoseconds() + int64(time.Hour)
	for _, op := range pending {
		op.Return = end // free to take effect at any point after it was sent
		history = append(history, op)
	}
	res, info := porcupine.CheckOperationsVerbose(registers, history, 2*time.Minute)
	t.Logf("%d operations (%d writes with unknown outcome) across 3 owner changes: %v", len(history), len(pending), res)
	if res != porcupine.Ok {
		path := filepath.Join(os.TempDir(), "duet-linearizability.html")
		if err := porcupine.VisualizePath(registers, info, path); err == nil {
			t.Logf("history written to %s", path)
		}
		t.Fatalf("history is not linearizable: %v", res)
	}
}
