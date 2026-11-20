package node

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/wal"
)

func mustErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got error %v, want %v", err, want)
	}
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// One owner writes, a standby stays warm by tailing.
func TestWriteAndTail(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "x", "y"))
	mustOK(t, w.append(a, "z"))
	mustOK(t, b.n.Tail())
	if len(b.applied) != 3 || b.applied[2] != "z" {
		t.Fatalf("standby applied %v", b.applied)
	}
	if got := w.verify(2); len(got) != 3 {
		t.Fatalf("replay has %d records", len(got))
	}
}

// A standby may not take over while the owner keeps renewing, and may once
// the owner has been silent for a full lease period.
func TestTakeoverOnlyAfterExpiry(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))

	for i := 0; i < 10; i++ {
		mustErr(t, b.n.Campaign(), lease.ErrNotExpired)
		w.now += simTTL / 2
		mustOK(t, a.n.Renew())
	}
	// The owner goes silent.
	mustErr(t, b.n.Campaign(), lease.ErrNotExpired)
	w.now += simTTL
	mustErr(t, b.n.Campaign(), lease.ErrNotExpired) // TTL*(1+drift) not yet reached
	w.now += simTTL / 5
	mustOK(t, b.n.Campaign())
	if a.n.IsOwner() {
		t.Fatal("old owner still believes it holds the lease")
	}
	mustOK(t, w.append(b, "b1"))
	if got := w.verify(2); len(got) != 2 || got[0] != "a1" || got[1] != "b1" {
		t.Fatalf("replay = %v", got)
	}
}

// The case the design lives or dies on: the owner is paused in the middle
// of a write for longer than its lease. The standby takes over. The old
// owner then wakes up and its write lands on disk. That write must not be
// acknowledged and must not become part of the log.
func TestPausedOwnerWriteLandsAfterTakeover(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1", "a2"))
	mustErr(t, b.n.Campaign(), lease.ErrNotExpired) // standby starts watching

	fired := false
	w.hook = func(node int, op string, off int64) {
		if fired || node != 0 || op != "write" || off < w.lay.LogStart() {
			return
		}
		fired = true
		// A is frozen here, just before its log write reaches the disk.
		w.now += 2 * simTTL
		mustOK(t, b.n.Campaign())
		mustOK(t, w.append(b, "b1"))
	}
	mustErr(t, w.append(a, "stale"), ErrLeaseLost)
	w.hook = nil
	if !fired {
		t.Fatal("hook never fired")
	}

	mustOK(t, w.append(b, "b2"))
	got := w.verify(2)
	want := []string{"a1", "a2", "b1", "b2"}
	if len(got) != len(want) {
		t.Fatalf("replay = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replay = %v, want %v", got, want)
		}
	}
	// The deposed owner rejoins as a standby and converges on the same log.
	mustOK(t, a.n.Tail())
	if len(a.applied) != 4 || a.applied[3] != "b2" {
		t.Fatalf("old owner applied %v", a.applied)
	}
}

// The same pause, but the owner's own clock stops while it is paused, as a
// guest clock does when a hypervisor suspends the VM. By its own clock the
// lease never expired. Found on Google Cloud in lab run 3: a suspended and
// resumed owner acknowledged a write after another node had taken over.
// Acknowledgement must not depend on the local clock.
func TestSuspendedOwnerWhoseClockStopped(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1", "a2"))
	mustErr(t, b.n.Campaign(), lease.ErrNotExpired)

	fired := false
	w.hook = func(node int, op string, off int64) {
		if fired || node != 0 || op != "write" || off < w.lay.LogStart() {
			return
		}
		fired = true
		w.now += 2 * simTTL
		*a.lag += 2 * simTTL // A's clock saw none of it
		mustOK(t, b.n.Campaign())
		mustOK(t, w.append(b, "b1"))
	}
	if err := w.append(a, "stale"); err == nil {
		t.Fatal("a suspended owner acknowledged a write after losing ownership")
	}
	w.hook = nil
	mustOK(t, w.append(b, "b2"))
	got := w.verify(2)
	if len(got) != 4 || got[2] != "b1" || got[3] != "b2" {
		t.Fatalf("replay = %v", got)
	}
	mustOK(t, a.n.Tail())
	if len(a.applied) != 4 {
		t.Fatalf("old owner applied %v", a.applied)
	}
}

// Owner renewal and challenger ballot at the same moment: whichever way the
// block operations interleave, they must not both proceed.
func TestRenewRacesBallot(t *testing.T) {
	// Count the device operations in one Campaign by the challenger, then
	// replay the scenario injecting the owner's renewal before each one.
	ops := func(inject int) (ownerValid, challengerWon bool, total int) {
		w := newWorld(t, 3)
		a, b := w.start(0, 1), w.start(1, 1)
		mustOK(t, a.n.Campaign())
		mustOK(t, w.append(a, "a1"))
		mustErr(t, b.n.Campaign(), lease.ErrNotExpired)
		w.now += 2 * simTTL // A looks dead to B, and A's own lease has lapsed
		count := 0
		w.hook = func(node int, op string, off int64) {
			if node != 1 {
				return
			}
			count++
			if count == inject {
				_ = a.n.Renew() // A turns out to be alive after all
			}
		}
		err := b.n.Campaign()
		w.hook = nil
		return a.n.m.Valid(), err == nil, count
	}
	_, _, total := ops(0)
	for i := 1; i <= total; i++ {
		ownerValid, challengerWon, _ := ops(i)
		if ownerValid && challengerWon {
			t.Fatalf("renewal injected before challenger op %d of %d: both sides believe they own the log", i, total)
		}
	}
}

// Two nodes start a ballot for the same epoch at every possible interleaving.
func TestRacingBallots(t *testing.T) {
	run := func(inject int) int {
		w := newWorld(t, 3)
		a, b := w.start(0, 1), w.start(1, 1)
		count := 0
		w.hook = func(node int, op string, off int64) {
			if node != 0 {
				return
			}
			count++
			if count == inject {
				_ = b.n.Campaign()
			}
		}
		_ = a.n.Campaign()
		w.hook = nil
		if a.n.IsOwner() && b.n.IsOwner() {
			t.Fatalf("interleaving %d: two owners", inject)
		}
		// Whatever happened, the system must still be able to make progress
		// and agree on one log.
		w.settle(a)
		mustOK(t, w.append(a, "after"))
		w.verify(2)
		return count
	}
	total := run(0)
	for i := 1; i <= total; i++ {
		run(i)
	}
}

// A node dies after its ballot value was accepted but before the epoch
// table was written. The survivor must arrive at the same decision.
func TestCrashMidBallot(t *testing.T) {
	probe := newWorld(t, 3)
	pa := probe.start(0, 1)
	total := 0
	probe.hook = func(int, string, int64) { total++ }
	mustOK(t, pa.n.Campaign())

	for crashAt := 1; crashAt <= total; crashAt++ {
		w := newWorld(t, 3)
		a, b := w.start(0, 1), w.start(1, 1)
		count := 0
		w.hook = func(node int, op string, off int64) {
			if node != 0 {
				return
			}
			count++
			if count == crashAt {
				a.dev.dead = true
			}
		}
		_ = a.n.Campaign()
		w.hook = nil
		w.settle(b)
		mustOK(t, w.append(b, "b1"))
		w.verify(2)
	}
}

// A reader must not apply a record that lands after the cutoff was computed
// but before the epoch table says so. It can tell from the ballot block.
func TestReaderHoldsWhileBallotInProgress(t *testing.T) {
	w := newWorld(t, 4)
	a, b, c := w.start(0, 1), w.start(1, 1), w.start(2, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	mustOK(t, c.n.Tail())
	mustErr(t, b.n.Campaign(), lease.ErrNotExpired)
	w.now += 2 * simTTL

	paxosWrites, fired := 0, false
	w.hook = func(node int, op string, off int64) {
		if node != 1 || op != "write" || off != w.lay.PaxosOff(1) {
			return
		}
		paxosWrites++
		if paxosWrites != 2 {
			return
		}
		// B has computed the cutoff and is about to accept its value. Now
		// A's in-flight write lands, past the cutoff, and C reads the log.
		fired = true
		stale := wal.NewWriter(w.disk, w.lay, 1, a.n.off, a.n.next)
		if _, err := stale.Append([][]byte{[]byte("stale")}); err != nil {
			t.Fatal(err)
		}
		mustOK(t, c.n.Tail())
		if len(c.applied) != 1 {
			t.Fatalf("reader applied a record past the cutoff: %v", c.applied)
		}
	}
	mustOK(t, b.n.Campaign())
	w.hook = nil
	if !fired {
		t.Fatal("hook never fired")
	}
	mustOK(t, w.append(b, "b1"))
	mustOK(t, c.n.Tail())
	if len(c.applied) != 2 || c.applied[1] != "b1" {
		t.Fatalf("reader applied %v", c.applied)
	}
	w.verify(3)
}

// An owner whose lease lapsed during a write, with nobody challenging, may
// carry on after a clean renewal. The record it could not acknowledge is on
// disk and a standby may already have applied it, so the owner must keep it
// and continue after it.
func TestResumeKeepsUnacknowledgedRecord(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))

	fired := false
	w.hook = func(node int, op string, off int64) {
		if !fired && node == 0 && op == "write" && off >= w.lay.LogStart() {
			fired = true
			w.now += 2 * simTTL // slow write; no challenger appears
		}
	}
	mustErr(t, w.append(a, "x"), ErrLeaseLost)
	w.hook = nil
	mustOK(t, b.n.Tail())
	if len(b.applied) != 2 || b.applied[1] != "x" {
		t.Fatalf("standby applied %v", b.applied)
	}
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "y"))
	got := w.verify(2)
	if len(got) != 3 || got[1] != "x" || got[2] != "y" {
		t.Fatalf("replay = %v", got)
	}
}

// The same, but a ballot was briefly visible while the owner looked at the
// log, so its first read held back. The ballot is then withdrawn and the
// renewal is clean. The owner must still continue after the record.
func TestResumeAfterWithdrawnBallot(t *testing.T) {
	w := newWorld(t, 4)
	a, c := w.start(0, 1), w.start(2, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	fired := false
	w.hook = func(node int, op string, off int64) {
		if !fired && node == 0 && op == "write" && off >= w.lay.LogStart() {
			fired = true
			w.now += 2 * simTTL
		}
	}
	mustErr(t, w.append(a, "x"), ErrLeaseLost)
	w.hook = nil
	mustOK(t, c.n.Tail())
	if len(c.applied) != 2 {
		t.Fatalf("standby applied %v", c.applied)
	}

	// Node 1 announces a ballot for epoch 2 and later withdraws it.
	ballot := make([]byte, 68)
	binary.LittleEndian.PutUint64(ballot[0:], 2) // instance
	binary.LittleEndian.PutUint64(ballot[8:], 5) // mbal
	mustOK(t, disk.WriteBlock(w.disk, w.lay.PaxosOff(1), ballot))
	w.hook = func(node int, op string, off int64) {
		if node == 0 && op == "write" && off == w.lay.LeaseOff(0) {
			mustOK(t, disk.WriteBlock(w.disk, w.lay.PaxosOff(1), make([]byte, 68)))
		}
	}
	mustOK(t, a.n.Campaign())
	w.hook = nil
	mustOK(t, w.append(a, "y"))
	got := w.verify(3)
	if len(got) != 3 || got[1] != "x" || got[2] != "y" {
		t.Fatalf("replay = %v", got)
	}
}

// A planned handover needs no waiting.
func TestReleaseHandsOverImmediately(t *testing.T) {
	w := newWorld(t, 3)
	a, b := w.start(0, 1), w.start(1, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	mustOK(t, a.n.Release())
	mustOK(t, b.n.Campaign()) // no time has passed
	mustOK(t, w.append(b, "b1"))
	mustErr(t, w.append(a, "late"), ErrNotOwner)
	if got := w.verify(2); len(got) != 2 {
		t.Fatalf("replay = %v", got)
	}
}

// A restarted owner does not wait for its own previous incarnation.
func TestRestartReacquires(t *testing.T) {
	w := newWorld(t, 3)
	a := w.start(0, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1", "a2"))
	a2 := w.start(0, 1) // same node, new process
	mustOK(t, a2.n.Campaign())
	if len(a2.applied) != 2 {
		t.Fatalf("restarted node recovered %v", a2.applied)
	}
	mustOK(t, w.append(a2, "a3"))
	if got := w.verify(2); len(got) != 3 {
		t.Fatalf("replay = %v", got)
	}
}

// A torn final record is not part of the log.
func TestTornTailIsIgnored(t *testing.T) {
	w := newWorld(t, 3)
	a := w.start(0, 1)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	mustOK(t, w.append(a, "a2"))
	// Damage one payload byte of the last record.
	off := a.n.off - 8
	var b [1]byte
	w.disk.ReadAt(b[:], off)
	b[0] ^= 0xff
	w.disk.WriteAt(b[:], off)
	delete(w.acked, 2) // the test destroyed it on purpose
	if got := w.verify(2); len(got) != 1 || got[0] != "a1" {
		t.Fatalf("replay = %v", got)
	}
}

// More than two nodes: one writer, the rest warm, any of them can take over.
func TestThreeNodes(t *testing.T) {
	w := newWorld(t, 4)
	a, b, c := w.start(0, 1), w.start(1, 1.04), w.start(2, 0.96)
	mustOK(t, a.n.Campaign())
	mustOK(t, w.append(a, "a1"))
	mustOK(t, b.n.Tail())
	mustOK(t, c.n.Tail())
	w.settle(c)
	mustOK(t, w.append(c, "c1"))
	w.settle(b)
	mustOK(t, w.append(b, "b1"))
	mustOK(t, a.n.Tail())
	mustOK(t, c.n.Tail())
	got := w.verify(3)
	if len(got) != 3 || len(a.applied) != 3 || len(c.applied) != 3 {
		t.Fatalf("replay = %v, a = %v, c = %v", got, a.applied, c.applied)
	}
}

// Randomised schedules: appends, renewals, campaigns, tailing, pauses, slow
// I/O, clock-rate skew, crashes and restarts, interleaved at the level of
// single block operations. After each schedule the log must contain every
// acknowledged write, and no two nodes may ever have applied different
// records at the same index (checked continuously by the apply callback).
func TestRandomSchedules(t *testing.T) {
	seeds := 400
	if testing.Short() {
		seeds = 40
	}
	if v, err := strconv.Atoi(os.Getenv("DUET_SEEDS")); err == nil && v > 0 {
		seeds = v // for long soak runs
	}
	for seed := 1; seed <= seeds; seed++ {
		runRandom(t, int64(seed), 300)
	}
	t.Logf("%d schedules: %d records, %d acknowledged, %d ownership changes",
		seeds, statRecords, statAcked, statEpochs)
}

var statAcked, statRecords, statEpochs int

func runRandom(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	const members = 3
	w := newWorld(t, members+1) // last id is the verifier
	rates := []float64{1, 1.045, 0.955}
	nodes := make([]*simNode, members)
	for i := range nodes {
		nodes[i] = w.start(i, rates[i])
	}
	paused := make([]bool, members)
	seq := 0

	fatal := func(err error) {
		if errors.Is(err, ErrCorrupt) {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	act := func(i int) {
		s := nodes[i]
		switch r := rng.Intn(100); {
		case r < 35:
			k := 1 + rng.Intn(3)
			batch := make([]string, k)
			for j := range batch {
				batch[j] = payload(i, &seq)
			}
			fatal(w.append(s, batch...))
		case r < 55:
			fatal(s.n.Renew())
		case r < 80:
			fatal(s.n.Campaign())
		case r < 97:
			fatal(s.n.Tail())
		default:
			fatal(s.n.Release())
		}
	}
	restart := func(i int) {
		nodes[i] = w.start(i, rates[i])
	}

	w.hook = func(node int, op string, off int64) {
		r := rng.Intn(1000)
		if r < 6 { // crash in the middle of an operation
			if node < members {
				nodes[node].dev.dead = true
			}
			return
		}
		if r >= 80 {
			return
		}
		// This operation is slow, or the VM is stunned in the middle of it,
		// and meanwhile other nodes get to act, possibly several times.
		if rng.Intn(2) == 0 {
			dt := time.Duration(rng.Int63n(int64(simTTL * 3 / 2)))
			w.now += dt
			if node < members && rng.Intn(3) == 0 {
				*nodes[node].lag += dt // a VM suspend: this node's clock stood still
			}
		}
		for k := rng.Intn(4); k > 0; k-- {
			j := rng.Intn(members)
			if j != node && !paused[j] && !nodes[j].dev.dead {
				act(j)
			}
			if rng.Intn(3) == 0 {
				w.now += time.Duration(rng.Int63n(int64(simTTL)))
			}
		}
	}

	for step := 0; step < steps; step++ {
		if rng.Intn(4) == 0 {
			w.now += simTTL * 2
		} else {
			w.now += time.Duration(rng.Int63n(int64(simTTL / 3)))
		}
		i := rng.Intn(members)
		if rng.Intn(40) == 0 {
			paused[i] = !paused[i]
		}
		if !paused[i] {
			act(i)
		}
		for j := range nodes {
			if nodes[j].dev.dead {
				restart(j)
			}
		}
	}

	// Quiesce and check.
	w.hook = nil
	final := w.verifyAfterSettle(nodes[rng.Intn(members)], members)
	statAcked += len(w.acked)
	statRecords += len(final)
	statEpochs += int(nodes[0].n.Epoch())
	for _, s := range nodes {
		if err := s.n.Tail(); err != nil {
			t.Fatalf("seed %d: node %d final tail: %v", seed, s.id, err)
		}
		if len(s.applied) != len(final) {
			t.Fatalf("seed %d: node %d applied %d records, log has %d", seed, s.id, len(s.applied), len(final))
		}
	}
}

func (w *world) verifyAfterSettle(s *simNode, readerID int) []string {
	w.t.Helper()
	w.settle(s)
	return w.verify(readerID)
}
