// Package node ties the lease and the log together into one participant:
// a writer when it owns the current epoch, a tailing reader otherwise.
package node

import (
	"errors"
	"fmt"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/wal"
)

var (
	ErrNotOwner  = errors.New("node: not the owner")
	ErrLeaseLost = errors.New("node: lease lost before the write could be acknowledged")
	ErrCorrupt   = errors.New("node: log does not match the epoch table")
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
	return &Node{dev: dev, l: l, m: m, apply: apply}, nil
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
		end, next, err := wal.Scan(n.dev, n.l, n.cur.Epoch, n.off, n.next, -1, func(r wal.Record) {
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
				n.off, n.next, scanErr = wal.Scan(n.dev, n.l, n.cur.Epoch, n.off, n.next, nxt.PrevEndOffset, func(r wal.Record) {
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
		end, nextIdx, err := wal.Scan(n.dev, n.l, prev.Epoch, off, idx, -1, nil)
		if err != nil {
			return lease.EpochRecord{}, err
		}
		// Leave a gap one batch wide and start on a block boundary. A deposed
		// writer can have at most one batch in flight; it starts at the
		// boundary at or after its own tail, which is at or before the
		// cutoff, and is at most MaxBatch long. MaxBatch is a whole number
		// of blocks, so that write ends at or before this start.
		start := disk.AlignUp(end + int64(n.l.MaxBatch))
		if start+int64(n.l.MaxBatch) > n.l.LogEnd() {
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
	n.off, n.next, err = wal.Scan(n.dev, n.l, rec.Epoch, n.off, n.next, -1, func(r wal.Record) {
		n.apply(r.Index, r.Payload)
	})
	if err != nil {
		return err
	}
	n.w = wal.NewWriter(n.dev, n.l, rec.Epoch, n.off, n.next)
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
