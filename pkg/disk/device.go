// Package disk defines the shared block device duet runs on and the
// on-disk layout of its metadata blocks.
//
// The protocol assumes three things of the device (see docs/DESIGN.md):
//
//  1. Reads and writes are linearizable across every node that has the
//     device open. A read that starts after a write returned sees it.
//  2. A write of one metadata block (BlockSize bytes) is atomic, or a torn
//     write is detectable by checksum.
//  3. Sync returning means the data survives the failures the underlying
//     storage is configured to tolerate.
//
// On vSAN these are properties to be verified in milestone 0, not facts.
package disk

import (
	"io"
	"sync"
)

// Device is the minimal interface duet needs from shared storage.
type Device interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Size() int64
}

// MemDevice is an in-memory device shared by several nodes in one process.
// It is linearizable and treats every write as immediately durable, which
// is the idealised behaviour the protocol is designed against.
type MemDevice struct {
	mu  sync.Mutex
	buf []byte
}

// NewMemDevice returns a zeroed device of the given size.
func NewMemDevice(size int64) *MemDevice {
	return &MemDevice{buf: make([]byte, size)}
}

func (d *MemDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(d.buf)) {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(p, d.buf[off:]), nil
}

func (d *MemDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(d.buf)) {
		return 0, io.ErrShortWrite
	}
	return copy(d.buf[off:], p), nil
}

func (d *MemDevice) Sync() error { return nil }
func (d *MemDevice) Size() int64 { return int64(len(d.buf)) }
