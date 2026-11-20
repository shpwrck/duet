// Package ckpt stores checkpoints of the state machine on the shared disk,
// so that a starting node does not have to replay the whole log.
//
// Every node has two slots of its own and writes only to those, so no
// agreement is needed to write one: any node, owner or not, may checkpoint
// the state it has applied, because a node applies only acknowledged
// records and every node applying the same log reaches the same state. A
// node always overwrites the older of its two slots, so a crash during a
// write leaves its previous checkpoint intact. A reader takes the newest
// valid checkpoint from any node.
package ckpt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/node"
)

// Meta describes one checkpoint.
type Meta struct {
	Applied uint64      // the state includes every record up to this index
	Cursor  node.Cursor // where the log continues from
	Size    int64       // bytes of state
	Sum     uint32      // CRC-32C of the state

	Node, Slot int // where it is stored
}

const (
	metaLen = 48
	chunk   = 1 << 20
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	// ErrNone means the disk holds no usable checkpoint.
	ErrNone = errors.New("ckpt: no checkpoint")
	// ErrTooLarge means the state does not fit a checkpoint slot.
	ErrTooLarge = errors.New("ckpt: state larger than a checkpoint slot")
	// ErrChanged means a checkpoint was replaced or damaged while it was
	// being read.
	ErrChanged = errors.New("ckpt: checkpoint changed while reading")
)

func (m Meta) encode() []byte {
	b := make([]byte, metaLen)
	binary.LittleEndian.PutUint64(b[0:], m.Applied)
	binary.LittleEndian.PutUint64(b[8:], m.Cursor.Epoch)
	binary.LittleEndian.PutUint64(b[16:], uint64(m.Cursor.Offset))
	binary.LittleEndian.PutUint64(b[24:], m.Cursor.Next)
	binary.LittleEndian.PutUint64(b[32:], uint64(m.Size))
	binary.LittleEndian.PutUint32(b[40:], m.Sum)
	return b
}

func readMeta(dev disk.Device, l disk.Layout, n, slot int) (Meta, bool, error) {
	p, st, err := disk.ReadBlock(dev, l.CkptOff(n, slot))
	if err != nil {
		return Meta{}, false, err
	}
	if st != disk.BlockValid || len(p) != metaLen {
		return Meta{}, false, nil
	}
	m := Meta{
		Applied: binary.LittleEndian.Uint64(p[0:]),
		Cursor: node.Cursor{Epoch: binary.LittleEndian.Uint64(p[8:]),
			Offset: int64(binary.LittleEndian.Uint64(p[16:])), Next: binary.LittleEndian.Uint64(p[24:])},
		Size: int64(binary.LittleEndian.Uint64(p[32:])),
		Sum:  binary.LittleEndian.Uint32(p[40:]),
		Node: n, Slot: slot,
	}
	if m.Size < 0 || m.Size > l.CkptSize-disk.BlockSize {
		return Meta{}, false, nil
	}
	return m, true, nil
}

// All returns every valid checkpoint header on the disk, newest first. A
// header being valid does not prove its state is intact; Read checks that.
func All(dev disk.Device, l disk.Layout) ([]Meta, error) {
	var out []Meta
	for n := 0; n < l.Nodes && l.CkptSize > 0; n++ {
		for slot := 0; slot < 2; slot++ {
			m, ok, err := readMeta(dev, l, n, slot)
			if err != nil {
				return nil, err
			}
			if ok {
				i := len(out)
				out = append(out, m)
				for ; i > 0 && out[i-1].Applied < m.Applied; i-- {
					out[i], out[i-1] = out[i-1], out[i]
				}
			}
		}
	}
	return out, nil
}

// Write stores a checkpoint in the older of this node's two slots. Only
// the node itself may call it for its id.
func Write(dev disk.Device, l disk.Layout, self int, m Meta, state io.Reader) (Meta, error) {
	if l.CkptSize == 0 {
		return Meta{}, ErrNone
	}
	if m.Size > l.CkptSize-disk.BlockSize {
		return Meta{}, fmt.Errorf("%w: %d bytes, slot holds %d", ErrTooLarge, m.Size, l.CkptSize-disk.BlockSize)
	}
	// Pick the slot that does not hold this node's newest checkpoint.
	slot := 0
	m0, ok0, err := readMeta(dev, l, self, 0)
	if err != nil {
		return Meta{}, err
	}
	m1, ok1, err := readMeta(dev, l, self, 1)
	if err != nil {
		return Meta{}, err
	}
	if ok0 && (!ok1 || m0.Applied >= m1.Applied) {
		slot = 1
	}
	off := l.CkptOff(self, slot)
	// Retire the old header before touching the state it describes.
	zero := make([]byte, disk.BlockSize)
	if _, err := dev.WriteAt(zero, off); err != nil {
		return Meta{}, err
	}
	if err := dev.Sync(); err != nil {
		return Meta{}, err
	}
	buf := make([]byte, chunk)
	var sum uint32
	for done := int64(0); done < m.Size; {
		want := int(min(int64(chunk), m.Size-done))
		if _, err := io.ReadFull(state, buf[:want]); err != nil {
			return Meta{}, err
		}
		sum = crc32.Update(sum, castagnoli, buf[:want])
		padded := int(disk.AlignUp(int64(want)))
		clear(buf[want:padded])
		if _, err := dev.WriteAt(buf[:padded], off+disk.BlockSize+done); err != nil {
			return Meta{}, err
		}
		done += int64(want)
	}
	if err := dev.Sync(); err != nil {
		return Meta{}, err
	}
	m.Sum, m.Node, m.Slot = sum, self, slot
	return m, disk.WriteBlock(dev, off, m.encode())
}

// Read copies a checkpoint's state to w and verifies it. On ErrChanged
// whatever was written to w must be discarded.
func Read(dev disk.Device, l disk.Layout, m Meta, w io.Writer) error {
	off := l.CkptOff(m.Node, m.Slot) + disk.BlockSize
	buf := make([]byte, chunk)
	var sum uint32
	for done := int64(0); done < m.Size; {
		want := int(min(int64(chunk), m.Size-done))
		if _, err := dev.ReadAt(buf[:disk.AlignUp(int64(want))], off+done); err != nil {
			return err
		}
		sum = crc32.Update(sum, castagnoli, buf[:want])
		if _, err := w.Write(buf[:want]); err != nil {
			return err
		}
		done += int64(want)
	}
	// The owner of the slot may have started replacing it meanwhile.
	again, ok, err := readMeta(dev, l, m.Node, m.Slot)
	if err != nil {
		return err
	}
	if !ok || again != m || sum != m.Sum {
		return ErrChanged
	}
	return nil
}
