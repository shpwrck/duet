package node

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
)

// The simulation runs every node in one goroutine against one in-memory
// disk. Interleavings are produced by a hook that fires before each device
// operation and may advance time or run another node's action in the
// middle of the first node's operation. That reaches every ordering of
// block reads and writes between nodes, which is what the protocol's
// safety depends on.

const (
	simTTL   = 10 * time.Second
	simDrift = 0.1
)

type world struct {
	t     *testing.T
	now   time.Duration
	disk  *disk.MemDevice
	lay   disk.Layout
	depth int
	hook  func(node int, op string, off int64)

	history map[uint64]string // first payload any node applied at each index
	acked   map[uint64]string // payloads acknowledged to a client
	nextInc uint64
}

type simClock struct {
	w    *world
	rate float64
}

func (c simClock) Now() time.Duration { return time.Duration(float64(c.w.now) * c.rate) }

type hookDev struct {
	w    *world
	node int
	dead bool
}

var errDead = errors.New("sim: node crashed")

func (d *hookDev) pre(op string, off int64) error {
	if d.dead {
		return errDead
	}
	if d.w.hook != nil && d.w.depth == 0 {
		d.w.depth++
		d.w.hook(d.node, op, off)
		d.w.depth--
	}
	if d.dead {
		return errDead
	}
	return nil
}

func (d *hookDev) ReadAt(p []byte, off int64) (int, error) {
	if err := d.pre("read", off); err != nil {
		return 0, err
	}
	return d.w.disk.ReadAt(p, off)
}

func (d *hookDev) WriteAt(p []byte, off int64) (int, error) {
	if err := d.pre("write", off); err != nil {
		return 0, err
	}
	return d.w.disk.WriteAt(p, off)
}

func (d *hookDev) Sync() error { return nil }
func (d *hookDev) Size() int64 { return d.w.disk.Size() }

type simNode struct {
	id      int
	rate    float64
	dev     *hookDev
	n       *Node
	applied []string
}

func newWorld(t *testing.T, nodes int) *world {
	t.Helper()
	lay := disk.Layout{Nodes: nodes, EpochSlots: 600, LogSize: 2 << 20, TTL: simTTL, MaxBatch: 512}
	w := &world{t: t, disk: disk.NewMemDevice(lay.Total()), lay: lay,
		history: map[uint64]string{}, acked: map[uint64]string{}}
	if err := disk.Format(w.disk, lay); err != nil {
		t.Fatal(err)
	}
	return w
}

// start creates (or restarts, with a new incarnation) a node.
func (w *world) start(id int, rate float64) *simNode {
	w.t.Helper()
	w.nextInc++
	s := &simNode{id: id, rate: rate, dev: &hookDev{w: w, node: id}}
	saved := w.hook
	w.hook = nil // opening the superblock is not part of the protocol
	n, err := New(s.dev, simClock{w, rate}, lease.Config{Node: id, Incarnation: w.nextInc, Drift: simDrift},
		func(index uint64, payload []byte) {
			if index != uint64(len(s.applied))+1 {
				w.t.Fatalf("node %d applied index %d after %d records", id, index, len(s.applied))
			}
			p := string(payload)
			if prev, ok := w.history[index]; ok && prev != p {
				w.t.Fatalf("divergence at index %d: node %d applied %q, another node applied %q", index, id, p, prev)
			}
			w.history[index] = p
			s.applied = append(s.applied, p)
		})
	w.hook = saved
	if err != nil {
		w.t.Fatal(err)
	}
	s.n = n
	return s
}

// append writes a batch and, if it is acknowledged, records that fact.
func (w *world) append(s *simNode, payloads ...string) error {
	batch := make([][]byte, len(payloads))
	for i, p := range payloads {
		batch[i] = []byte(p)
	}
	first, err := s.n.Append(batch)
	if err != nil {
		return err
	}
	for i, p := range payloads {
		idx := first + uint64(i)
		if prev, ok := w.acked[idx]; ok && prev != p {
			w.t.Fatalf("index %d acknowledged twice: %q and %q", idx, prev, p)
		}
		w.acked[idx] = p
	}
	return nil
}

// verify replays the log with a fresh reader and checks it against every
// acknowledgement and everything any node applied.
func (w *world) verify(readerID int) []string {
	w.t.Helper()
	saved := w.hook
	w.hook = nil
	defer func() { w.hook = saved }()
	r := w.start(readerID, 1)
	if err := r.n.Tail(); err != nil {
		w.t.Fatalf("replay: %v", err)
	}
	for idx, p := range w.acked {
		if idx > uint64(len(r.applied)) {
			w.t.Fatalf("acknowledged index %d (%q) missing from replay of %d records", idx, p, len(r.applied))
		}
		if r.applied[idx-1] != p {
			w.t.Fatalf("acknowledged index %d was %q, replay has %q", idx, p, r.applied[idx-1])
		}
	}
	return r.applied
}

// settle advances time and campaigns until s owns the log.
func (w *world) settle(s *simNode) {
	w.t.Helper()
	for i := 0; i < 50; i++ {
		err := s.n.Campaign()
		if err == nil {
			return
		}
		if errors.Is(err, ErrCorrupt) {
			w.t.Fatal(err)
		}
		w.now += simTTL
	}
	w.t.Fatalf("node %d could not acquire ownership", s.id)
}

func payload(node int, seq *int) string {
	*seq++
	return fmt.Sprintf("n%d-%d", node, *seq)
}
