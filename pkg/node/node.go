// Package node ties the lease and the log together into one participant:
// a writer when it owns the current epoch, a tailing reader otherwise.
package node

import (
	"errors"
	"fmt"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/trim"
	"github.com/shpwrck/duet/pkg/wal"
)

var (
	ErrNotOwner  = errors.New("node: not the owner")
	ErrLeaseLost = errors.New("node: lease lost before the write could be acknowledged")
	ErrCorrupt   = errors.New("node: log does not match the epoch table")
	// ErrLapped means the part of the log this node needs next may have
	// been reused. The process must start again from a checkpoint, as a
	// new incarnation; starting is also what retires its earlier epochs.
	ErrLapped = errors.New("node: the log has been trimmed past this node's position")
)

// ApplyFunc receives every record in log order, exactly once per Node.
type ApplyFunc func(index uint64, payload []byte)

// Node is one participant. It is not safe for concurrent use; the caller
// serialises Campaign, Renew, Append and Tail.
type Node struct {
	dev   disk.Device
	l     disk.Layout
	m     *lease.Manager
	apply ApplyFunc

	// Read cursor: the epoch being read and where its next record goes.
	started bool
	cur     lease.EpochRecord
	off     int64
	next    uint64

	w *wal.Writer // non-nil only while this node believes it is the owner

	cfg          lease.Config
	safeEpoch    uint64 // this node will not write for epochs up to here
	head         int64  // the head this node has published
	retiredFloor uint64 // every epoch up to here is known to be retired
}

// New returns a Node. apply may be nil.
func New(dev disk.Device, clock lease.Clock, cfg lease.Config, apply ApplyFunc) (*Node, error) {
	l, err := disk.Open(dev)
	if err != nil {
		return nil, err
	}
	m, err := lease.New(dev, l, clock, cfg)
	if err != nil {
		return nil, err
	}
	if apply == nil {
		apply = func(uint64, []byte) {}
	}
	n := &Node{dev: dev, l: l, m: m, apply: apply, cfg: cfg}
	return n, n.announce()
}

// announce writes this incarnation's trim block. A process that has just
// started has no write in flight for any epoch on the disk, so it can
// promise never to write for any of them. It carries the furthest
// published head forward so that heads never go backwards.
func (n *Node) announce() error {
	head, err := trim.Head(n.dev, n.l)
	if err != nil {
		return err
	}
	latest, have, err := n.m.Latest()
	if err != nil {
		return err
	}
	if have {
		n.safeEpoch = latest.Epoch
	}
	n.head = head
	return n.writeTrim()
}

func (n *Node) writeTrim() error {
	return trim.Write(n.dev, n.l, n.cfg.Node, trim.Block{Incarnation: n.cfg.Incarnation, SafeEpoch: n.safeEpoch, Head: n.head})
}

// retire records that this node will never write for epoch e again. It is
// called only once a later epoch is on disk, which closes e for good, and
// from the node's one loop, so no write for e is still to be issued.
func (n *Node) retire(e uint64) {
	if e <= n.safeEpoch {
		return
	}
	n.safeEpoch = e
	// Best effort: if this write fails, log space is reclaimed later, not
	// unsafely.
	_ = n.writeTrim()
}

// scan is wal.Scan followed by a look at the published heads. The order
// matters: a writer publishes a head before it overwrites anything below
// it, so a reader that finds the head still at or behind where it started
// knows that what it read had not been overwritten.
func (n *Node) scan(epoch uint64, off int64, index uint64, limit int64, fn func(wal.Record)) (int64, uint64, error) {
	var recs []wal.Record
	end, next, err := wal.Scan(n.dev, n.l, epoch, off, index, limit, func(r wal.Record) { recs = append(recs, r) })
	if err != nil {
		return off, index, err
	}
	head, err := trim.Head(n.dev, n.l)
	if err != nil {
		return off, index, err
	}
	if head > off {
		return off, index, ErrLapped
	}
	if fn != nil {
		for _, r := range recs {
			fn(r)
		}
	}
	return end, next, nil
}

// writeLimit is the logical offset the owner of epoch e must stay below.
// Space one lap back may be reused only if a checkpoint covers it (the
// head) and no deposed owner can still have a late write aimed at it: the
// stretch around the cutoff of every earlier epoch stays off limits until
// that epoch's owner has retired it. cutoff is the cutoff of epoch e-1
// when e is not on disk yet, and negative otherwise.
func (n *Node) writeLimit(e uint64, cutoff int64) (int64, error) {
	reuse, err := trim.Head(n.dev, n.l)
	if err != nil {
		return 0, err
	}
	for old := n.retiredFloor + 1; old < e; old++ {
		rec, st, err := n.m.ReadSlot(old)
		if err != nil {
			return 0, err
		}
		if st != disk.BlockValid {
			return 0, fmt.Errorf("%w: epoch %d missing from the table", ErrCorrupt, old)
		}
		retired := rec.Owner == n.cfg.Node // our own past epochs: this loop is not writing them
		if !retired {
			if retired, err = trim.Retired(n.dev, n.l, rec.Owner, old); err != nil {
				return 0, err
			}
		}
		if retired {
			if old == n.retiredFloor+1 {
				n.retiredFloor = old
			}
			continue
		}
		end := cutoff
		if old+1 < e || cutoff < 0 {
			next, st, err := n.m.ReadSlot(old + 1)
			if err != nil {
				return 0, err
			}
			if st != disk.BlockValid {
				return 0, fmt.Errorf("%w: epoch %d missing from the table", ErrCorrupt, old+1)
			}
			end = next.PrevEndOffset
		}
		// The late write was not on disk when the cutoff was measured, so
		// it starts at or after the cutoff; writes are whole blocks, so
		// nothing from the cutoff's own block onwards may be reused.
		zone := end &^ (disk.BlockSize - 1)
		if zone < n.l.LogStart() {
			zone = n.l.LogStart()
		}
		if zone < reuse {
			reuse = zone
		}
	}
	return reuse + n.l.LogSize, nil
}

// Trim publishes that a checkpoint covers the log up to off, which lets
// the owner reuse the space before it. The caller must have that
// checkpoint safely on disk: after this, a node that needs anything before
// off can only get it from a checkpoint. Any node may call it, for a
// position it has itself reached.
func (n *Node) Trim(off int64) error {
	if !n.started || off > n.off {
		return errors.New("node: Trim beyond what this node has read")
	}
	head, err := trim.Head(n.dev, n.l)
	if err != nil {
		return err
	}
	if off > head {
		head = off
	}
	if head <= n.head {
		return nil
	}
	n.head = head
	return n.writeTrim()
}

// Cursor is a position in the log: everything before it has been applied.
type Cursor struct {
	Epoch  uint64 // the epoch being read
	Offset int64  // where its next record goes
	Next   uint64 // that record's index
}

// Cursor returns the node's read position. It is the zero Cursor until
// the node has read something.
func (n *Node) Cursor() Cursor {
	if !n.started {
		return Cursor{}
	}
	return Cursor{Epoch: n.cur.Epoch, Offset: n.off, Next: n.next}
}

// Seek starts a new node at a position taken from Cursor on any node of
// the same disk, for a state machine restored to exactly that position.
func (n *Node) Seek(c Cursor) error {
	if n.started || n.w != nil {
		return errors.New("node: Seek after the node has started reading")
	}
	if c == (Cursor{}) {
		return nil
	}
	rec, st, err := n.m.ReadSlot(c.Epoch)
	if err != nil {
		return err
	}
	if st != disk.BlockValid || rec.Epoch != c.Epoch || c.Offset < rec.StartOffset || c.Next < rec.StartIndex {
		return fmt.Errorf("%w: cursor %+v does not fit epoch record %+v", ErrCorrupt, c, rec)
	}
	n.cur, n.off, n.next, n.started = rec, c.Offset, c.Next, true
	return nil
}

// IsOwner reports whether this node may acknowledge writes right now.
func (n *Node) IsOwner() bool { return n.w != nil && n.m.Valid() }

// Epoch returns the epoch this node is currently reading or writing.
func (n *Node) Epoch() uint64 {
	if !n.started {
		return 0
	}
	return n.cur.Epoch
}

// Owner returns the node that owns the epoch this node is reading, or -1
// if it has read nothing yet. It can be out of date by one takeover.
func (n *Node) Owner() int {
	if !n.started {
		return -1
	}
	return n.cur.Owner
}

// NextIndex is the index of the next record this node expects.
func (n *Node) NextIndex() uint64 { return n.next }

// Tail applies every record that is safe to apply and returns. A standby
// calls it in a loop to stay warm.
//
// The order of reads matters. Records are read first, and only afterwards
// does the node look for signs of a newer epoch. A record that lies beyond
// an epoch's cutoff can only reach the disk after the ballot that fixes the
// cutoff has begun, so a reader that saw such a record will also see the
// ballot, and holds back until the epoch table says where the cutoff is.
func (n *Node) Tail() error {
	for {
		if !n.started {
			first, st, err := n.m.ReadSlot(1)
			if err != nil {
				return err
			}
			if st != disk.BlockValid {
				return nil // nothing has ever been written
			}
			n.cur, n.off, n.next, n.started = first, first.StartOffset, first.StartIndex, true
		}

		type rec struct {
			index   uint64
			payload []byte
			end     int64
		}
		var got []rec
		end, next, err := n.scan(n.cur.Epoch, n.off, n.next, -1, func(r wal.Record) {
			got = append(got, rec{r.Index, r.Payload, r.End})
		})
		if err != nil {
			return err
		}

		nxt, st, err := n.m.ReadSlot(n.cur.Epoch + 1)
		if err != nil {
			return err
		}
		if st == disk.BlockValid {
			// The epoch is closed. Apply exactly up to its cutoff.
			for _, r := range got {
				if r.end > nxt.PrevEndOffset {
					break
				}
				n.apply(r.index, r.payload)
				n.off, n.next = r.end, r.index+1
			}
			if n.off < nxt.PrevEndOffset {
				// Records landed between our scan and the cutoff scan.
				var scanErr error
				n.off, n.next, scanErr = n.scan(n.cur.Epoch, n.off, n.next, nxt.PrevEndOffset, func(r wal.Record) {
					n.apply(r.Index, r.Payload)
				})
				if scanErr != nil {
					return scanErr
				}
			}
			if n.off != nxt.PrevEndOffset || n.next != nxt.StartIndex {
				return fmt.Errorf("%w: epoch %d ends at offset %d index %d, table says %d and %d",
					ErrCorrupt, n.cur.Epoch, n.off, n.next, nxt.PrevEndOffset, nxt.StartIndex)
			}
			if n.cur.Owner == n.cfg.Node && n.cur.Incarnation == n.cfg.Incarnation {
				// We owned the epoch that has just been closed.
				n.w = nil
				n.retire(n.cur.Epoch)
			}
			n.cur, n.off, n.next = nxt, nxt.StartOffset, nxt.StartIndex
			continue
		}

		stirring, err := n.m.NextEpochStirring(n.cur.Epoch)
		if err != nil {
			return err
		}
		if stirring {
			return nil // hold what we read until the cutoff is known
		}
		for _, r := range got {
			n.apply(r.index, r.payload)
		}
		n.off, n.next = end, next
		return nil
	}
}

// Campaign tries to become the owner. It returns nil only if this node now
// owns the latest epoch with a valid lease. lease.ErrNotExpired means the
// current owner still holds the lease; call again later.
func (n *Node) Campaign() error {
	if n.IsOwner() {
		return nil
	}
	n.w = nil
	if err := n.Tail(); err != nil {
		return err
	}
	rec, err := n.m.TryAcquire(func(prev *lease.EpochRecord) (lease.EpochRecord, error) {
		if prev == nil {
			return lease.EpochRecord{StartIndex: 1, StartOffset: n.l.LogStart()}, nil
		}
		// Find the cutoff of the previous epoch. Start from our cursor if it
		// is already inside that epoch, so a warm standby scans only the tail.
		off, idx := prev.StartOffset, prev.StartIndex
		if n.started && n.cur.Epoch == prev.Epoch {
			off, idx = n.off, n.next
		}
		end, nextIdx, err := n.scan(prev.Epoch, off, idx, -1, nil)
		if err != nil {
			return lease.EpochRecord{}, err
		}
		// Leave a gap one batch wide and start on a block boundary. A deposed
		// writer can have at most one batch in flight; it starts at the
		// boundary at or after its own tail, which is at or before the
		// cutoff, and is at most MaxBatch long. MaxBatch is a whole number
		// of blocks, so that write ends at or before this start.
		start := disk.AlignUp(end + int64(n.l.MaxBatch))
		limit, err := n.writeLimit(prev.Epoch+1, end)
		if err != nil {
			return lease.EpochRecord{}, err
		}
		if start+int64(n.l.MaxBatch) > limit {
			return lease.EpochRecord{}, wal.ErrLogFull
		}
		return lease.EpochRecord{StartIndex: nextIdx, StartOffset: start, PrevEndOffset: end}, nil
	})
	if err != nil {
		return err
	}
	if !n.started || n.cur.Epoch != rec.Epoch {
		// A new epoch. Catch up through the previous epoch's cutoff; this
		// moves the cursor to the start of ours.
		if err := n.Tail(); err != nil {
			return err
		}
		if n.started && n.cur.Epoch > rec.Epoch {
			// We were slow enough that another node has already taken a
			// later epoch. Nothing is wrong; we are simply not the owner.
			return ErrLeaseLost
		}
		if !n.started || n.cur.Epoch != rec.Epoch {
			return fmt.Errorf("%w: could not reach epoch %d after winning it", ErrCorrupt, rec.Epoch)
		}
	}
	// We hold a valid lease on rec and no later epoch exists, so every valid
	// record of this epoch on disk is part of history, including any we
	// wrote earlier but could not acknowledge. Other nodes may already have
	// applied them. Apply them too and continue after them; never overwrite.
	if rec.Epoch <= n.safeEpoch {
		return fmt.Errorf("%w: won epoch %d after promising not to write up to %d", ErrCorrupt, rec.Epoch, n.safeEpoch)
	}
	n.off, n.next, err = n.scan(rec.Epoch, n.off, n.next, -1, func(r wal.Record) {
		n.apply(r.Index, r.Payload)
	})
	if err != nil {
		return err
	}
	limit, err := n.writeLimit(rec.Epoch, -1)
	if err != nil {
		return err
	}
	n.w = wal.NewWriter(n.dev, n.l, rec.Epoch, n.off, n.next, limit)
	return nil
}

// Renew extends the lease. Call it several times per lease period.
func (n *Node) Renew() error {
	err := n.m.Renew()
	if errors.Is(err, lease.ErrLeaseLost) || errors.Is(err, lease.ErrNotOwner) {
		n.w = nil
	}
	return err
}

// Release hands ownership back so a standby can take over immediately.
func (n *Node) Release() error {
	n.w = nil
	return n.m.Release()
}

// Confirm reports whether this node still owned the log at some point
// after the call began, by reading the disk. It is the check a
// linearizable read needs.
func (n *Node) Confirm() error {
	if !n.IsOwner() {
		n.w = nil
		return ErrNotOwner
	}
	if err := n.m.Confirm(); err != nil || !n.m.Valid() {
		n.w = nil
		return ErrLeaseLost
	}
	return nil
}

// Append writes a batch and returns the index of its first record. A nil
// error is the acknowledgement: the batch is durable and will be seen by
// every future owner. ErrLeaseLost means the outcome is unknown to the
// caller, exactly like a timeout: the batch may or may not survive.
func (n *Node) Append(batch [][]byte) (uint64, error) {
	if !n.IsOwner() {
		n.w = nil
		return 0, ErrNotOwner
	}
	first, err := n.w.Append(batch)
	if errors.Is(err, wal.ErrLogFull) {
		// Nothing was written. More space may have become reusable since
		// the limit was last worked out.
		limit, lerr := n.writeLimit(n.cur.Epoch, -1)
		if lerr != nil {
			return 0, lerr
		}
		n.w.SetLimit(limit)
		if first, err = n.w.Append(batch); errors.Is(err, wal.ErrLogFull) {
			return 0, err
		}
	}
	if err != nil {
		// Part of the batch may be on disk. Drop the writer so the next
		// Campaign rescans from the cursor instead of overwriting it.
		n.w = nil
		return 0, err
	}
	// Confirm after write: acknowledge only if, with the data already on
	// disk, there is still no sign of a newer epoch. This check reads the
	// disk and does not trust the clock. The lease check stays as a second
	// line of defence.
	if err := n.m.Confirm(); err != nil || !n.m.Valid() {
		n.w = nil
		return 0, ErrLeaseLost
	}
	for i, p := range batch {
		n.apply(first+uint64(i), p)
	}
	n.off, n.next = n.w.Offset(), n.w.NextIndex()
	return first, nil
}
