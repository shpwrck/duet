// Package trim holds what makes it safe to reuse log space.
//
// Every node has one trim block and is the only writer of it. It carries
// two promises, both of which only ever grow:
//
//   - SafeEpoch: this node will never again write a log record for an
//     epoch at or below it. A deposed owner can have one late write still
//     to come, aimed at the gap after its epoch; until its block says that
//     epoch is behind it, the gap must not be reused.
//   - Head: a checkpoint that is safely on disk covers the log up to this
//     offset, so nothing before it is needed to rebuild the state.
package trim

import (
	"encoding/binary"
	"math"

	"github.com/shpwrck/duet/pkg/disk"
)

// Block is one node's trim block.
type Block struct {
	Incarnation uint64
	SafeEpoch   uint64
	Head        int64
	// Dead is set from outside the node, by whoever has confirmed that the
	// node is gone for good (an operator, or a controller asking vCenter).
	Dead bool
}

// Read returns a node's block. A block that was never written, or is torn,
// reads as the zero Block: no promises.
func Read(dev disk.Device, l disk.Layout, node int) (Block, error) {
	p, st, err := disk.ReadBlock(dev, l.TrimOff(node))
	if err != nil {
		return Block{}, err
	}
	if st != disk.BlockValid || len(p) < 25 {
		return Block{}, nil
	}
	return Block{
		Incarnation: binary.LittleEndian.Uint64(p[0:]),
		SafeEpoch:   binary.LittleEndian.Uint64(p[8:]),
		Head:        int64(binary.LittleEndian.Uint64(p[16:])),
		Dead:        p[24] == 1,
	}, nil
}

// Write stores a node's block. Only the node itself may call it for its
// id, except through DeclareDead.
func Write(dev disk.Device, l disk.Layout, node int, b Block) error {
	p := make([]byte, 25)
	binary.LittleEndian.PutUint64(p[0:], b.Incarnation)
	binary.LittleEndian.PutUint64(p[8:], b.SafeEpoch)
	binary.LittleEndian.PutUint64(p[16:], uint64(b.Head))
	if b.Dead {
		p[24] = 1
	}
	return disk.WriteBlock(dev, l.TrimOff(node), p)
}

// Head is the furthest head any node has published, and never less than
// the start of the log.
func Head(dev disk.Device, l disk.Layout) (int64, error) {
	head := l.LogStart()
	for i := 0; i < l.Nodes; i++ {
		b, err := Read(dev, l, i)
		if err != nil {
			return 0, err
		}
		if b.Head > head {
			head = b.Head
		}
	}
	return head, nil
}

// Retired reports whether a node has promised never to write for epoch
// again, or has been declared dead.
func Retired(dev disk.Device, l disk.Layout, node int, epoch uint64) (bool, error) {
	b, err := Read(dev, l, node)
	return b.SafeEpoch >= epoch, err
}

// DeclareDead records that a node is gone for good and will never write
// again. It is the one write to a trim block not made by its node. The
// caller must know it to be true: that the machine is powered off or
// destroyed. If the node is in fact alive, acknowledged data can be lost.
// A node started later under the same id replaces the block with its own.
func DeclareDead(dev disk.Device, l disk.Layout, node int) error {
	b, err := Read(dev, l, node)
	if err != nil {
		return err
	}
	b.SafeEpoch, b.Dead = math.MaxUint64, true
	return Write(dev, l, node, b)
}
