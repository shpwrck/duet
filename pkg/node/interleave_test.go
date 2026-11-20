package node

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestRandomInterleavings runs every node on its own goroutine and stops
// each one before every block read and write, so that any number of nodes
// can be in the middle of an operation at once, and a node can be left
// stalled part-way through one while the others complete whole ballots.
//
// TestRandomSchedules cannot do that: it runs other nodes' operations from
// inside one node's block operation, so at most two operations are ever in
// progress, and one of them always runs to its end. Both faults described
// in stalled_test.go need three nodes mid-operation and went unseen by it.
func TestRandomInterleavings(t *testing.T) {
	seeds := 300
	if testing.Short() {
		seeds = 30
	}
	if v, err := strconv.Atoi(os.Getenv("DUET_SEEDS")); err == nil && v > 0 {
		seeds = v
	}
	var ops, acked, epochs int
	for seed := 1; seed <= seeds; seed++ {
		o, a, e := runInterleaved(t, int64(seed), 3, 1500)
		ops, acked, epochs = ops+o, acked+a, epochs+e
		if t.Failed() {
			t.Fatalf("seed %d", seed)
		}
	}
	t.Logf("%d schedules: %d operations, %d acknowledged writes, %d ownership changes", seeds, ops, acked, epochs)
}

type stopped struct{}

func runInterleaved(t *testing.T, seed int64, nodes, steps int) (ops, acked, epochs int) {
	w := newWorld(t, nodes)
	rng := rand.New(rand.NewSource(seed))
	sims := make([]*simNode, nodes)
	for i := range sims {
		sims[i] = w.start(i, 1)
	}
	grant := make([]chan struct{}, nodes)
	for i := range grant {
		grant[i] = make(chan struct{})
	}
	parked, gone := make(chan int), make(chan int, nodes)
	stop := false
	seq := 0

	w.nested = true
	w.hook = func(node int, op string, off int64) {
		parked <- node
		<-grant[node]
		if stop {
			panic(stopped{})
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(id int) {
			clean := false
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(stopped); !ok {
						panic(r)
					}
					clean = true
				}
				if !clean {
					gone <- id // a check failed on this goroutine
				}
			}()
			for {
				parked <- id
				<-grant[id]
				if stop {
					clean = true
					return
				}
				s := sims[id]
				var err error
				switch r := rng.Intn(12); {
				case r < 4:
					err = w.append(s, payload(id, &seq))
				case r < 7:
					err = s.n.Campaign()
				case r < 9:
					err = s.n.Tail()
				case r < 11:
					err = s.n.Renew()
				default: // the process dies and a new one starts
					s.dev.dead = true
					sims[id] = w.start(id, 1)
				}
				ops++
				if errors.Is(err, ErrCorrupt) {
					t.Errorf("node %d: %v", id, err)
					return
				}
			}
		}(i)
	}

	waiting := map[int]bool{}
	for len(waiting) < nodes {
		waiting[<-parked] = true
	}
	frozenUntil := make([]int, nodes)
	clockStopped := make([]bool, nodes)
	failed := false
	for step := 0; step < steps && !failed; step++ {
		if rng.Intn(12) == 0 {
			dt := time.Duration(rng.Int63n(int64(3 * simTTL / 2)))
			w.now += dt
			for i := range sims {
				if frozenUntil[i] > step && clockStopped[i] {
					*sims[i].lag += dt // a suspended VM does not see time pass
				}
			}
		}
		if rng.Intn(20) == 0 {
			i := rng.Intn(nodes)
			frozenUntil[i] = step + 10 + rng.Intn(300)
			clockStopped[i] = rng.Intn(2) == 0
		}
		var free []int
		for i := 0; i < nodes; i++ {
			if waiting[i] && frozenUntil[i] <= step {
				free = append(free, i)
			}
		}
		if len(free) == 0 {
			continue
		}
		id := free[rng.Intn(len(free))]
		delete(waiting, id)
		grant[id] <- struct{}{}
		select {
		case p := <-parked:
			waiting[p] = true
		case <-gone:
			failed = true
		}
	}
	// Unwind every node wherever it is.
	stop = true
	for id := range waiting {
		grant[id] <- struct{}{}
	}
	wg.Wait()
	w.hook, w.nested = nil, false
	if failed || t.Failed() {
		t.Errorf("interleaving with seed %d failed", seed)
		return
	}
	for _, s := range sims {
		s.dev.dead = true
	}
	// Nodes were stopped mid-ballot, and a reader rightly waits while a
	// ballot is open. Let a new process settle ownership, then replay.
	w.settle(w.start(1, 1))
	w.verify(0)
	last, _, err := w.start(0, 1).n.m.Latest()
	if err != nil {
		t.Fatal(fmt.Errorf("seed %d: %w", seed, err))
	}
	return ops, len(w.acked), int(last.Epoch)
}
