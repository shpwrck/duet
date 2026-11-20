package disk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDirectFileDevice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.img")
	l := Layout{Nodes: 2, EpochSlots: 4, LogSize: 64 * BlockSize, TTL: time.Second, MaxBatch: 4 * BlockSize}
	if err := os.WriteFile(path, make([]byte, l.Total()), 0o600); err != nil {
		t.Fatal(err)
	}
	dev, err := OpenFile(path, 0, true)
	if err != nil {
		t.Skipf("direct I/O not available here: %v", err)
	}
	defer dev.Close()
	if err := Format(dev, l); err != nil {
		t.Fatal(err)
	}
	got, err := Open(dev)
	if err != nil || got != l {
		t.Fatalf("Open = %+v, %v; want %+v", got, err, l)
	}
	// An unaligned read spanning two blocks.
	block := bytes.Repeat([]byte{0xab}, 2*BlockSize)
	if _, err := dev.WriteAt(block, l.LogStart()); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 100)
	if _, err := dev.ReadAt(p, l.LogStart()+BlockSize-50); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, bytes.Repeat([]byte{0xab}, 100)) {
		t.Fatal("unaligned read returned wrong bytes")
	}
	if _, err := dev.WriteAt([]byte("x"), l.LogStart()); err == nil {
		t.Fatal("unaligned direct write was accepted")
	}
}
