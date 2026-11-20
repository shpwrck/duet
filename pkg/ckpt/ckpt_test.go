package ckpt_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/shpwrck/duet/pkg/ckpt"
	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/node"
)

func setup(t *testing.T) (*disk.MemDevice, disk.Layout) {
	t.Helper()
	l := disk.Layout{Nodes: 2, EpochSlots: 4, LogSize: 1 << 20, TTL: time.Second, MaxBatch: disk.BlockSize, CkptSize: 4 << 20}
	dev := disk.NewMemDevice(l.Total())
	if err := disk.Format(dev, l); err != nil {
		t.Fatal(err)
	}
	got, err := disk.Open(dev)
	l.ID = got.ID // chosen by Format
	if err != nil || got != l {
		t.Fatalf("layout did not survive format: %+v %v", got, err)
	}
	return dev, l
}

func state(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

func write(t *testing.T, dev disk.Device, l disk.Layout, self int, applied uint64, data []byte) ckpt.Meta {
	t.Helper()
	m, err := ckpt.Write(dev, l, self, ckpt.Meta{Applied: applied, Size: int64(len(data)),
		Cursor: node.Cursor{Epoch: 1, Offset: 4096 * int64(applied), Next: applied + 1}}, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewestWinsAndOlderSurvives(t *testing.T) {
	dev, l := setup(t)
	if all, _ := ckpt.All(dev, l); len(all) != 0 {
		t.Fatalf("fresh disk has checkpoints: %+v", all)
	}
	a, b, c := state(1, 3<<20), state(2, 1234567), state(3, 5)
	write(t, dev, l, 0, 10, a)
	write(t, dev, l, 1, 20, b)
	write(t, dev, l, 0, 30, c) // node 0's second slot; its first still holds 10
	all, err := ckpt.All(dev, l)
	if err != nil || len(all) != 3 || all[0].Applied != 30 || all[1].Applied != 20 || all[2].Applied != 10 {
		t.Fatalf("All: %v %+v", err, all)
	}
	for i, want := range [][]byte{c, b, a} {
		var got bytes.Buffer
		if err := ckpt.Read(dev, l, all[i], &got); err != nil || !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("checkpoint %d: err %v, %d bytes, want %d", all[i].Applied, err, got.Len(), len(want))
		}
	}
	if all[0].Cursor != (node.Cursor{Epoch: 1, Offset: 4096 * 30, Next: 31}) {
		t.Fatalf("cursor: %+v", all[0].Cursor)
	}
	// A third write by node 0 replaces its older slot (10), not 30.
	write(t, dev, l, 0, 40, a)
	all, _ = ckpt.All(dev, l)
	if len(all) != 3 || all[0].Applied != 40 || all[1].Applied != 30 {
		t.Fatalf("after third write: %+v", all)
	}
}

func TestDamageIsDetected(t *testing.T) {
	dev, l := setup(t)
	m := write(t, dev, l, 0, 10, state(1, 100000))
	dev.WriteAt([]byte{0xff}, l.CkptOff(m.Node, m.Slot)+disk.BlockSize+5000)
	if err := ckpt.Read(dev, l, m, &bytes.Buffer{}); !errors.Is(err, ckpt.ErrChanged) {
		t.Fatalf("read of damaged state: %v", err)
	}
}

// failAfter lets a number of writes through and then fails, like a node
// dying part-way through a checkpoint.
type failAfter struct {
	disk.Device
	left int
}

func (f *failAfter) WriteAt(p []byte, off int64) (int, error) {
	if f.left--; f.left < 0 {
		return 0, errors.New("crashed")
	}
	return f.Device.WriteAt(p, off)
}

func TestCrashMidWriteKeepsPrevious(t *testing.T) {
	for crashAt := 0; crashAt < 6; crashAt++ {
		dev, l := setup(t)
		old := state(1, 70000)
		write(t, dev, l, 0, 10, old)
		_, err := ckpt.Write(&failAfter{dev, crashAt}, l, 0, ckpt.Meta{Applied: 20, Size: 3 << 20}, bytes.NewReader(state(2, 3<<20)))
		all, _ := ckpt.All(dev, l)
		if err == nil {
			if len(all) != 2 || all[0].Applied != 20 {
				t.Fatalf("crashAt %d: write succeeded but %+v", crashAt, all)
			}
			continue
		}
		if len(all) != 1 || all[0].Applied != 10 {
			t.Fatalf("crashAt %d: %+v", crashAt, all)
		}
		var got bytes.Buffer
		if err := ckpt.Read(dev, l, all[0], &got); err != nil || !bytes.Equal(got.Bytes(), old) {
			t.Fatalf("crashAt %d: previous checkpoint unreadable: %v", crashAt, err)
		}
	}
}
