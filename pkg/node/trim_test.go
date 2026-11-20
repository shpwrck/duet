package node

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/trim"
	"github.com/shpwrck/duet/pkg/wal"
)

// smallWorld has a log of only logBlocks blocks, so tests go round it.
func smallWorld(t *testing.T, nodes int, logBlocks int64) *world {
	t.Helper()
	lay := disk.Layout{Nodes: nodes, EpochSlots: 600, LogSize: logBlocks * disk.BlockSize, TTL: simTTL, MaxBatch: disk.BlockSize}
	w := &world{t: t, disk: disk.NewMemDevice(lay.Total()), lay: lay,
		history: map[uint64]string{}, acked: map[uint64]string{}}
	if err := disk.Format(w.disk, lay); err != nil {
		t.Fatal(err)
	}
	return w
}

// checkpoint is what a real checkpoint holds: the state at a log position.
type checkpoint struct {
	applied []string
	cursor  Cursor
}

func snapshot(s *simNode) checkpoint {
	return checkpoint{applied: append([]string(nil), s.applied...), cursor: s.n.Cursor()}
}

// restore starts a new incarnation of a node from a checkpoint.
func (w *world) restore(id int, c checkpoint) *simNode {
	w.t.Helper()
	s := w.start(id, 1)
	s.applied = append([]string(nil), c.applied...)
	if err := s.n.Seek(c.cursor); err != nil {
		w.t.Fatalf("seek to checkpoint: %v", err)
	}
	return s
}

// verifyFrom replays the log from a checkpoint with a fresh node and
// checks every acknowledged write.
func (w *world) verifyFrom(readerID int, c checkpoint) {
	w.t.Helper()
	saved := w.hook
	w.hook = nil
	defer func() { w.hook = saved }()
	r := w.restore(readerID, c)
	if err := r.n.Tail(); err != nil {
		w.t.Fatalf("replay from checkpoint at index %d: %v", len(c.applied), err)
	}
	for idx, p := range w.acked {
		if idx > uint64(len(r.applied)) {
			w.t.Fatalf("acknowledged index %d (%q) missing: replay from the checkpoint reached %d", idx, p, len(r.applied))
		}
		if r.applied[idx-1] != p {
			w.t.Fatalf("acknowledged index %d was %q, replay has %q", idx, p, r.applied[idx-1])
		}
	}
}

func laps(w *world, s *simNode) float64 {
	return float64(s.n.Cursor().Offset-w.lay.LogStart()) / float64(w.lay.LogSize)
}

// Without a published head the log fills once and stops; publishing one
// lets the owner continue into the space behind it, lap after lap.
func TestLogWrapsOnlyBehindThePublishedHead(t *testing.T) {
	w := smallWorld(t, 3, 32)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	n := 0
	for ; ; n++ {
		if err := w.append(a, fmt.Sprint("r", n)); err != nil {
			mustErr(t, err, wal.ErrLogFull)
			break
		}
		if n > 100 {
			t.Fatal("the log never filled")
		}
	}
	if n < 30 || n > 32 {
		t.Fatalf("log of 32 blocks took %d one-block batches", n)
	}
	mustOK(t, a.n.Renew())
	// The standby reads everything, a checkpoint is taken there, and the
	// standby publishes it. The owner finds the space on its next write.
	mustOK(t, b.n.Tail())
	ck := snapshot(b)
	mustOK(t, b.n.Trim(ck.cursor.Offset))
	for i := 0; i < 200; i++ {
		if err := w.append(a, fmt.Sprint("s", i)); err != nil {
			mustErr(t, err, wal.ErrLogFull)
			mustOK(t, b.n.Tail())
			ck = snapshot(b)
			mustOK(t, b.n.Trim(ck.cursor.Offset))
			mustOK(t, w.append(a, fmt.Sprint("s", i)))
		}
		if i%7 == 0 {
			mustOK(t, a.n.Renew())
		}
	}
	if l := laps(w, a); l < 6 {
		t.Fatalf("only %.1f laps", l)
	}
	w.verifyFrom(2, ck)
}

// Ownership changes while the log is being reused, with both nodes taking
// turns. A node that hands over retires its epoch when it reads past it,
// which is what frees the gap after it.
func TestWrapAcrossTakeovers(t *testing.T) {
	w := smallWorld(t, 3, 48)
	nodes := []*simNode{w.start(0, 1), w.start(1, 1)}
	owner := 0
	mustOK(t, nodes[0].n.Campaign())
	var ck checkpoint
	for i := 0; i < 600; i++ {
		o, f := nodes[owner], nodes[1-owner]
		err := w.append(o, fmt.Sprint("r", i))
		if errors.Is(err, wal.ErrLogFull) {
			t.Fatalf("log full at step %d although every past owner has read on", i)
		}
		mustOK(t, err)
		if i%5 == 0 {
			mustOK(t, f.n.Tail())
			ck = snapshot(f)
			mustOK(t, f.n.Trim(ck.cursor.Offset))
			mustOK(t, o.n.Renew())
		}
		if i%40 == 39 {
			mustOK(t, o.n.Release())
			w.settle(f)
			mustOK(t, o.n.Tail()) // the old owner reads past its epoch and retires it
			owner = 1 - owner
		}
	}
	if l := laps(w, nodes[owner]); l < 8 {
		t.Fatalf("only %.1f laps", l)
	}
	w.verifyFrom(2, ck)
}

// The hazard reuse creates. A is deposed with one write still to come and
// then stalls for longer than it takes B to go all the way round the log.
// B must stop before the space A's write is aimed at, however much has
// been checkpointed, until A is known to have stopped. Then A's write
// lands, and nothing acknowledged is hurt.
func TestDeposedOwnersLateWriteAfterALap(t *testing.T) {
	for _, how := range []string{"A reads on and retires", "A is declared dead"} {
		t.Run(how, func(t *testing.T) {
			w := smallWorld(t, 3, 40)
			a, b := w.start(0, 1), w.start(1, 1)
			mustOK(t, a.n.Campaign())
			for i := 0; i < 6; i++ {
				mustOK(t, w.append(a, fmt.Sprint("a", i)))
			}
			// A's next write, built but not yet on disk.
			stale := wal.NewWriter(w.disk, w.lay, 1, a.n.off, a.n.next, 1<<62)
			w.now += 2 * simTTL
			w.settle(b)

			var ck checkpoint
			full := -1
			for i := 0; i < 200 && full < 0; i++ {
				err := w.append(b, fmt.Sprint("b", i))
				if errors.Is(err, wal.ErrLogFull) {
					full = i
					break
				}
				mustOK(t, err)
				ck = snapshot(b)
				mustOK(t, b.n.Trim(ck.cursor.Offset)) // everything is checkpointed, always
				mustOK(t, b.n.Renew())
			}
			if full < 0 {
				t.Fatal("B went round the log across A's gap although A had not retired")
			}
			// B must have stopped short of where A's write will land, one
			// lap on.
			if got, max := b.n.Cursor().Offset, disk.AlignUp(a.n.off)+w.lay.LogSize; got > max {
				t.Fatalf("B wrote up to %d, into the space A's write is aimed at (%d one lap on)", got, max)
			}
			// A wakes up and its write lands.
			if _, err := stale.Append([][]byte{[]byte("stale")}); err != nil {
				t.Fatal(err)
			}
			w.verifyFrom(2, ck)

			if how == "A is declared dead" {
				mustOK(t, trim.DeclareDead(w.disk, w.lay, 0))
			} else {
				// A notices its write was not acknowledged and reads on. It
				// is told it has been overtaken, and does what a node must
				// then do: start again from the checkpoint as a new
				// incarnation, which is what retires its old epoch.
				a.n.w = nil
				mustErr(t, a.n.Tail(), ErrLapped)
				a.dev.dead = true
				w.restore(0, ck)
			}
			for i := 0; i < 200; i++ {
				mustOK(t, w.append(b, fmt.Sprint("c", i)))
				ck = snapshot(b)
				mustOK(t, b.n.Trim(ck.cursor.Offset))
				mustOK(t, b.n.Renew())
			}
			if l := laps(w, b); l < 4 {
				t.Fatalf("only %.1f laps after A retired", l)
			}
			w.verifyFrom(2, ck)
		})
	}
}

// A node that stopped reading and is then overtaken must be told so, not
// left reading whatever now lies where it stopped.
func TestOvertakenReaderIsTold(t *testing.T) {
	w := smallWorld(t, 3, 32)
	a, c := w.start(0, 1), w.start(2, 1)
	mustOK(t, a.n.Campaign())
	for i := 0; i < 5; i++ {
		mustOK(t, w.append(a, fmt.Sprint("r", i)))
	}
	mustOK(t, c.n.Tail())
	seen := len(c.applied)
	var ck checkpoint
	for i := 0; i < 100; i++ {
		mustOK(t, w.append(a, fmt.Sprint("s", i)))
		ck = snapshot(a)
		mustOK(t, a.n.Trim(ck.cursor.Offset))
		mustOK(t, a.n.Renew())
	}
	mustErr(t, c.n.Tail(), ErrLapped)
	if len(c.applied) != seen {
		t.Fatalf("overtaken reader applied %d more records", len(c.applied)-seen)
	}
	// It recovers from the checkpoint, under a new incarnation.
	c.dev.dead = true
	c2 := w.restore(2, ck)
	mustOK(t, c2.n.Tail())
	if len(c2.applied) != len(a.applied) {
		t.Fatalf("restored reader has %d records, owner has %d", len(c2.applied), len(a.applied))
	}
}

// Heads must survive the restart of the node that published them.
func TestHeadSurvivesRestart(t *testing.T) {
	w := smallWorld(t, 3, 32)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	for i := 0; i < 10; i++ {
		mustOK(t, w.append(a, fmt.Sprint("r", i)))
	}
	mustOK(t, b.n.Tail())
	mustOK(t, b.n.Trim(b.n.Cursor().Offset))
	before, err := trim.Head(w.disk, w.lay)
	mustOK(t, err)
	if before != b.n.Cursor().Offset {
		t.Fatalf("head = %d, want %d", before, b.n.Cursor().Offset)
	}
	b.dev.dead = true
	w.start(1, 1) // same id, new incarnation, rewrites the trim block
	after, err := trim.Head(w.disk, w.lay)
	mustOK(t, err)
	if after != before {
		t.Fatalf("head moved from %d to %d across a restart", before, after)
	}
	if err := a.n.Trim(a.n.Cursor().Offset + disk.BlockSize); err == nil {
		t.Fatal("a node published a head beyond what it has read")
	}
}

// Random schedules on a small log: writes, takeovers, restarts, and
// checkpoints published by whichever node took them. Any node that is told
// it was overtaken is rebuilt from the newest checkpoint.
func TestRandomSchedulesWithTrimming(t *testing.T) {
	seeds := 300
	if testing.Short() {
		seeds = 30
	}
	var totalLaps float64
	for seed := 1; seed <= seeds; seed++ {
		totalLaps += runRandomTrim(t, int64(seed))
	}
	if totalLaps/float64(seeds) < 3 {
		t.Fatalf("schedules averaged only %.1f laps; the test is not exercising reuse", totalLaps/float64(seeds))
	}
	t.Logf("%d schedules, %.1f laps of the log on average", seeds, totalLaps/float64(seeds))
}

func runRandomTrim(t *testing.T, seed int64) float64 {
	rng := rand.New(rand.NewSource(seed))
	const members = 3
	w := smallWorld(t, members+1, 40)
	nodes := make([]*simNode, members)
	for i := range nodes {
		nodes[i] = w.start(i, 1)
	}
	var ck checkpoint
	fail := func(what string, err error) {
		t.Fatalf("seed %d: %s: %v", seed, what, err)
	}
	// rebuild replaces a node with a new incarnation restored from the
	// newest checkpoint, as a restarted process would be.
	rebuild := func(i int) {
		nodes[i].dev.dead = true
		head, err := trim.Head(w.disk, w.lay)
		if err != nil {
			fail("read head", err)
		}
		if ck.cursor.Offset < head && head > w.lay.LogStart() {
			t.Fatalf("seed %d: head %d is beyond the newest checkpoint at %d", seed, head, ck.cursor.Offset)
		}
		nodes[i] = w.restore(i, ck)
	}
	tolerated := func(i int, err error) bool {
		switch {
		case err == nil:
			return true
		case errors.Is(err, ErrLapped):
			rebuild(i)
			return true
		case errors.Is(err, ErrCorrupt), errors.Is(err, errDead):
			return false
		}
		return true // not the owner, lease not expired, contended, log full: all part of life
	}
	seq := 0
	for step := 0; step < 1500; step++ {
		i := rng.Intn(members)
		r := rng.Intn(100)
		if r < 45 && rng.Intn(4) > 0 { // most writes go to whoever owns the log
			for j, s := range nodes {
				if s.n.IsOwner() {
					i = j
				}
			}
		}
		s := nodes[i]
		switch {
		case r < 45:
			seq++
			if err := w.append(s, fmt.Sprintf("s%d-%d", seed, seq)); !tolerated(i, err) {
				fail("append", err)
			}
		case r < 60:
			if err := s.n.Tail(); !tolerated(i, err) {
				fail("tail", err)
			}
		case r < 72:
			if err := s.n.Campaign(); !tolerated(i, err) {
				fail("campaign", err)
			}
		case r < 80:
			if err := s.n.Renew(); !tolerated(i, err) {
				fail("renew", err)
			}
		case r < 90:
			// Checkpoint what this node has applied and publish it.
			if c := snapshot(s); c.cursor.Offset > ck.cursor.Offset {
				ck = c
				if err := s.n.Trim(c.cursor.Offset); err != nil {
					fail("trim", err)
				}
			}
		case r < 94:
			rebuild(i)
		default:
			w.now += simTTL * 2
		}
	}
	// Let everything settle, then check from the newest checkpoint.
	best := 0.0
	for i, s := range nodes {
		if err := s.n.Tail(); errors.Is(err, ErrLapped) {
			rebuild(i)
		} else if err != nil {
			fail("final tail", err)
		}
		if l := laps(w, nodes[i]); l > best {
			best = l
		}
	}
	if c := snapshot(nodes[0]); c.cursor.Offset > ck.cursor.Offset {
		ck = c
	}
	w.verifyFrom(members, ck)
	return best
}
