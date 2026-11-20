package node

import (
	"testing"
)

// actor runs one node operation on its own goroutine and holds it just
// before chosen block operations, so that a test can interleave several
// nodes that are each in the middle of something. Only one goroutine runs
// at a time.
type actor struct {
	t      *testing.T
	pred   func(op string, off int64) bool
	at     chan struct{}
	resume chan struct{}
	done   chan error
}

type stage struct {
	w      *world
	actors map[int]*actor
}

func newStage(w *world) *stage {
	s := &stage{w: w, actors: map[int]*actor{}}
	w.nested = true
	w.hook = func(node int, op string, off int64) {
		a := s.actors[node]
		if a == nil || a.pred == nil || !a.pred(op, off) {
			return
		}
		a.pred = nil
		a.at <- struct{}{}
		<-a.resume
	}
	return s
}

// start runs f for node and returns once it is held before the first block
// operation that until accepts.
func (s *stage) start(t *testing.T, node int, f func() error, until func(op string, off int64) bool) *actor {
	t.Helper()
	a := &actor{t: t, pred: until, at: make(chan struct{}), resume: make(chan struct{}), done: make(chan error, 1)}
	s.actors[node] = a
	go func() { a.done <- f() }()
	a.wait()
	return a
}

func (a *actor) wait() {
	a.t.Helper()
	select {
	case <-a.at:
	case err := <-a.done:
		a.t.Fatalf("operation finished before reaching the point it was to be held at: %v", err)
	}
}

// until lets the operation continue to the next held point.
func (a *actor) until(pred func(op string, off int64) bool) {
	a.t.Helper()
	a.pred = pred
	a.resume <- struct{}{}
	a.wait()
}

// finish lets the operation run to its end.
func (a *actor) finish() error {
	a.resume <- struct{}{}
	return <-a.done
}

// nth accepts the nth operation of one kind on one block.
func nth(n int, wantOp string, wantOff int64) func(string, int64) bool {
	return func(op string, off int64) bool {
		if op == wantOp && off == wantOff {
			n--
		}
		return n == 0 && op == wantOp && off == wantOff
	}
}

// after accepts the first operation matching then that follows one
// matching first.
func after(first, then func(string, int64) bool) func(string, int64) bool {
	seen := false
	return func(op string, off int64) bool {
		if !seen {
			seen = first(op, off)
			return false
		}
		return then(op, off)
	}
}

// tryStart is start for an operation that may finish without reaching the
// point it was to be held at.
func (s *stage) tryStart(node int, f func() error, until func(op string, off int64) bool) (a *actor, held bool) {
	a = &actor{pred: until, at: make(chan struct{}), resume: make(chan struct{}), done: make(chan error, 1)}
	s.actors[node] = a
	go func() { a.done <- f() }()
	select {
	case <-a.at:
		return a, true
	case err := <-a.done:
		a.done <- err
		return a, false
	}
}

// An owner checks for a later epoch after every write, and a reader after
// every read, by looking at every node's ballot block and at the next
// epoch slot. Those are separate block reads. If the node stalls between
// them for long enough, a whole epoch can be decided and every ballot block
// wiped again by withdrawn ballots for the epoch after that.
//
// With the slot read first, a node stalled after that read then found
// nothing in the ballot blocks either: an owner acknowledged a record the
// new epoch had cut off, and a reader applied one. Found by reasoning
// about the block-level model in docs/tla, not by a test run: it needs
// three nodes, a restart and five exact interleavings, and 6,000 random
// schedules had not hit it.
//
// The slot has to be read last. A ballot block is only wiped by its node
// after the epoch it stood for has its slot, so a slot read that follows
// the ballot reads cannot miss both. This runs the schedule with the
// stalled node held before each read of its check in turn.
func TestStalledBetweenTheReadsOfTheCheck(t *testing.T) {
	for _, who := range []string{"owner", "reader"} {
		for k := 0; k <= 4; k++ { // before the read of each ballot block, then of the slot
			stalledBetweenReads(t, who, k)
		}
	}
}

func stalledBetweenReads(t *testing.T, who string, k int) {
	nodes := 3
	if who == "reader" {
		nodes = 4
	}
	w := newWorld(t, nodes)
	lay := w.lay
	isLogWrite := func(op string, off int64) bool { return op == "write" && off >= lay.LogStart() }
	isLogRead := func(op string, off int64) bool { return op == "read" && off >= lay.LogStart() }
	read := func(off int64) func(string, int64) bool { return nth(1, "read", off) }
	write := func(off int64) func(string, int64) bool { return nth(1, "write", off) }

	a, b, c := w.start(0, 1), w.start(1, 1), w.start(2, 1)
	var d *simNode
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	if who == "reader" {
		d = w.start(3, 1)
		mustOK(t, d.n.Tail())
	}
	if err := b.n.Campaign(); err == nil {
		t.Fatal("b took over a live owner")
	}
	// A's VM is suspended: time passes for everyone else.
	w.now += 2 * simTTL
	*a.lag += 2 * simTTL

	st := newStage(w)

	// B runs its ballot as far as having read the log for the cutoff.
	gb := st.start(t, 1, b.n.Campaign, nth(2, "write", lay.PaxosOff(1)))
	// Where the stalled node is held: before it reads ballot block k, or,
	// for k past the last node, before it reads the slot for epoch 2.
	hold := read(lay.SlotOff(2))
	if k < nodes {
		hold = read(lay.PaxosOff(k))
	}
	// A, resumed with its clock where it was, writes a record beyond that
	// cutoff and starts its check.
	var appendErr error
	appendA := func() error { appendErr = w.append(a, "beyond-the-cutoff"); return appendErr }
	var stalled *actor
	held := false
	if who == "owner" {
		stalled, held = st.tryStart(0, appendA, after(isLogWrite, hold))
	} else {
		// A's record reaches the disk; A itself gets no further.
		st.start(t, 0, appendA, after(isLogWrite, func(op string, off int64) bool { return op == "read" }))
		// The reader reads the record and is held in its check.
		stalled, held = st.tryStart(3, d.n.Tail, after(isLogRead, hold))
	}
	// B finishes: epoch 2 exists and does not include A's record.
	mustOK(t, gb.finish())

	// C decides B's lease has run out and gets as far as writing a ballot
	// for epoch 3.
	if err := c.n.Campaign(); err == nil {
		t.Fatal("c took over on first sight")
	}
	w.now += 2 * simTTL
	*a.lag += 2 * simTTL
	*b.lag += 2 * simTTL // B did not see that time pass either
	gc := st.start(t, 2, c.n.Campaign, write(lay.PaxosOff(2)))
	// B, alive after all, renews, and then its process dies and restarts.
	mustOK(t, b.n.Renew())
	b.dev.dead = true
	b2 := w.start(1, 1)
	// The new B stands for epoch 3 with a ballot lower than C's is about
	// to be, and is held before it looks at C's block.
	gb2 := st.start(t, 1, b2.n.Campaign, after(write(lay.PaxosOff(1)), read(lay.PaxosOff(2))))
	// C writes its ballot and is held before re-reading B's lease.
	gc.until(read(lay.LeaseOff(1)))
	// B sees C's higher ballot and withdraws, wiping its block.
	if err := gb2.finish(); err == nil {
		t.Fatal("the restarted b won epoch 3; the schedule did not happen as intended")
	}
	// C sees that B's lease moved and withdraws, wiping its block.
	if err := gc.finish(); err == nil {
		t.Fatal("c won epoch 3; the schedule did not happen as intended")
	}
	// Every ballot block is empty again and epoch 2 is in the table.

	// The stalled node carries on with its check.
	if held {
		stalled.finish()
	}
	w.hook, w.nested = nil, false
	if who == "owner" && appendErr == nil {
		t.Errorf("held at point %d of its check, the owner acknowledged a record that epoch 2 had already cut off", k)
	}
	if who == "reader" && len(d.applied) != 1 {
		t.Errorf("held at point %d of its check, the reader applied a record that epoch 2 had already cut off: %v", k, d.applied)
	}
}

// A node works out which epoch to stand for by reading the table, and may
// stall before it starts its ballot. By then that epoch can have been
// decided, and every ballot block that would say so wiped again by
// withdrawn ballots for the epoch after. Disk Paxos proper never wipes a
// block; duet does (retreat), so the table has to be consulted as well.
//
// Without that, the stalled node ran a ballot for an epoch that already
// had an owner, saw nothing, chose itself, found the slot already written,
// left it alone and carried on as owner anyway: two owners of one epoch.
func TestStalledBeforeBallotForAnEpochAlreadyDecided(t *testing.T) {
	w := newWorld(t, 3)
	lay := w.lay
	read := func(off int64) func(string, int64) bool { return nth(1, "read", off) }
	write := func(off int64) func(string, int64) bool { return nth(1, "write", off) }

	a, b, c := w.start(0, 1), w.start(1, 1), w.start(2, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	for _, s := range []*simNode{b, c} {
		if err := s.n.Campaign(); err == nil {
			t.Fatal("took over a live owner")
		}
	}
	w.now += 2 * simTTL
	*a.lag += 2 * simTTL // A is suspended

	st := newStage(w)
	// C decides to stand for epoch 2 and stalls before its ballot starts.
	gc := st.start(t, 2, c.n.Campaign, after(read(lay.LeaseOff(0)), read(lay.PaxosOff(2))))
	// B takes epoch 2 and acknowledges a write in it.
	w.hook = nil
	mustOK(t, b.n.Campaign())
	mustOK(t, w.append(b, "b1"))
	st = restage(w, st)

	// A wakes, sees it has been replaced, and in time decides B's lease
	// has run out. It stalls as its ballot for epoch 3 starts.
	*a.lag = 0
	if err := a.n.Campaign(); err == nil {
		t.Fatal("a took over on first sight")
	}
	w.now += 2 * simTTL
	*b.lag += 2 * simTTL
	*c.lag += 2 * simTTL
	ga := st.start(t, 0, a.n.Campaign, after(read(lay.LeaseOff(1)), read(lay.PaxosOff(0))))
	// B renews, dies, restarts and stands for epoch 3; held before it
	// looks at A's block.
	mustOK(t, b.n.Renew())
	b.dev.dead = true
	b2 := w.start(1, 1)
	gb2 := st.start(t, 1, b2.n.Campaign, after(write(lay.PaxosOff(1)), read(lay.PaxosOff(0))))
	// A writes a higher ballot and is held before re-reading B's lease.
	ga.until(read(lay.LeaseOff(1)))
	if err := gb2.finish(); err == nil {
		t.Fatal("the restarted b won epoch 3; the schedule did not happen as intended")
	}
	if err := ga.finish(); err == nil {
		t.Fatal("a won epoch 3; the schedule did not happen as intended")
	}
	// Epoch 2 is B's in the table and no ballot block says anything.

	err := gc.finish()
	w.hook, w.nested = nil, false
	if err == nil && c.n.Epoch() == 2 {
		t.Error("the stalled node made itself owner of epoch 2, which the table gives to another node")
		if w.append(c, "c1") == nil {
			t.Error("and acknowledged a write as its owner")
		}
	}
	w.verify(0)
}

// restage re-installs the stage's hook after a stretch run without it.
func restage(w *world, old *stage) *stage {
	s := newStage(w)
	s.actors = old.actors
	return s
}
