// Package wal is the record log on the shared disk.
//
// The log is one region of the disk, used in laps: offsets are logical,
// only ever grow, and map onto the region modulo its size. Each ownership
// epoch writes a contiguous run of records, starting at the offset
// recorded for that epoch in the epoch table. Records carry their epoch and index, so a reader can
// tell a record that belongs where it sits from anything else.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/shpwrck/duet/pkg/disk"
)

const (
	headerSize = 32
	recMagic   = 0x6475726c // "durl"
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	ErrLogFull  = errors.New("wal: log region is full")
	ErrTooLarge = errors.New("wal: batch exceeds MaxBatch")
)

func align8(n int64) int64 { return (n + 7) &^ 7 }

// EncodedLen is the space one record with a payload of n bytes occupies.
func EncodedLen(n int) int64 { return headerSize + align8(int64(n)) }

// Record is one decoded log record.
type Record struct {
	Epoch   uint64
	Index   uint64
	Payload []byte
	End     int64 // offset just past this record
}

func encode(dst []byte, epoch, index uint64, payload []byte) []byte {
	start := len(dst)
	n := int(EncodedLen(len(payload)))
	dst = append(dst, make([]byte, n)...)
	b := dst[start:]
	binary.LittleEndian.PutUint32(b[0:], recMagic)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(b[8:], epoch)
	binary.LittleEndian.PutUint64(b[16:], index)
	copy(b[headerSize:], payload)
	crc := crc32.Checksum(b[0:24], castagnoli)
	crc = crc32.Update(crc, castagnoli, payload)
	binary.LittleEndian.PutUint32(b[24:], crc)
	return dst
}

// ring gives the log region logical addressing: offsets only grow, and
// map onto the region modulo its size. A read or write that crosses the
// end of the region continues at its start.
type ring struct {
	dev disk.Device
	l   disk.Layout
}

func (r ring) do(p []byte, off int64, op func([]byte, int64) (int, error)) error {
	for len(p) > 0 {
		phys := r.l.LogPhys(off)
		n := int64(len(p))
		if room := r.l.LogEnd() - phys; n > room {
			n = room
		}
		if _, err := op(p[:n], phys); err != nil {
			return err
		}
		p, off = p[n:], off+n
	}
	return nil
}
func (r ring) readAt(p []byte, off int64) error  { return r.do(p, off, r.dev.ReadAt) }
func (r ring) writeAt(p []byte, off int64) error { return r.do(p, off, r.dev.WriteAt) }

// window is a read-ahead buffer over the log, so that a scan costs one
// device read per chunk instead of two per record.
type window struct {
	dev   disk.Device
	l     disk.Layout
	start int64
	buf   []byte
	next  int64 // size of the next chunk; grows as a scan keeps going
}

const (
	windowMin = 16 << 10
	windowMax = 1 << 20
)

// bytes returns n bytes at logical offset off, or nil before the log's start.
func (w *window) bytes(off int64, n int) ([]byte, error) {
	if off < w.l.LogStart() {
		return nil, nil
	}
	if off < w.start || off+int64(n) > w.start+int64(len(w.buf)) {
		start := off &^ (disk.BlockSize - 1)
		size := disk.AlignUp(off + int64(n) - start)
		if w.next < windowMin {
			w.next = windowMin
		}
		if size < w.next {
			size = w.next
		}
		if w.next < windowMax {
			w.next *= 2 // a short scan stays cheap, a long replay reads big chunks
		}
		if size > w.l.LogSize {
			size = w.l.LogSize
		}
		buf := make([]byte, size)
		if err := (ring{w.dev, w.l}).readAt(buf, start); err != nil {
			return nil, err
		}
		w.start, w.buf = start, buf
	}
	return w.buf[off-w.start : off-w.start+int64(n)], nil
}

// readRecord returns the record at off if it is a valid record with exactly
// this epoch and index. Anything else, including zeroes, a torn write, or a
// record from another epoch, reports ok=false.
func (w *window) readRecord(off int64, epoch, index uint64) (Record, bool, error) {
	h, err := w.bytes(off, headerSize)
	if err != nil || h == nil {
		return Record{}, false, err
	}
	if binary.LittleEndian.Uint32(h[0:]) != recMagic {
		return Record{}, false, nil
	}
	n := int(binary.LittleEndian.Uint32(h[4:]))
	if n > w.l.MaxBatch {
		return Record{}, false, nil
	}
	if binary.LittleEndian.Uint64(h[8:]) != epoch || binary.LittleEndian.Uint64(h[16:]) != index {
		return Record{}, false, nil
	}
	want := binary.LittleEndian.Uint32(h[24:])
	crc := crc32.Checksum(h[0:24], castagnoli)
	body, err := w.bytes(off+headerSize, n)
	if err != nil || (body == nil && n > 0) {
		return Record{}, false, err
	}
	if crc32.Update(crc, castagnoli, body) != want {
		return Record{}, false, nil
	}
	payload := make([]byte, n)
	copy(payload, body)
	return Record{Epoch: epoch, Index: index, Payload: payload, End: off + EncodedLen(n)}, true, nil
}

// Scan reads consecutive valid records of one epoch, starting with the
// record expected at (off, index). It stops at the first position that does
// not hold the expected record, or at limit if limit is non-negative. fn may
// be nil. It returns the offset just past the last record read and the index
// the next record would have.
//
// A batch always starts on a block boundary, so after the last record of a
// batch the next record is found at the following boundary, not directly
// behind it. Scan looks in both places.
func Scan(dev disk.Device, l disk.Layout, epoch uint64, off int64, index uint64, limit int64, fn func(Record)) (int64, uint64, error) {
	w := &window{dev: dev, l: l}
	for {
		if limit >= 0 && off >= limit {
			return off, index, nil
		}
		rec, ok, err := w.readRecord(off, epoch, index)
		if err != nil {
			return off, index, err
		}
		if !ok && off != disk.AlignUp(off) {
			rec, ok, err = w.readRecord(disk.AlignUp(off), epoch, index)
			if err != nil {
				return off, index, err
			}
		}
		if !ok || (limit >= 0 && rec.End > limit) {
			return off, index, nil
		}
		if fn != nil {
			fn(rec)
		}
		off, index = rec.End, index+1
	}
}

// Writer appends records for one epoch. It is not safe for concurrent use.
type Writer struct {
	dev   disk.Device
	l     disk.Layout
	epoch uint64
	off   int64
	next  uint64
	limit int64
}

// NewWriter returns a writer positioned at the start of an epoch. It will
// not write at or beyond limit; see SetLimit.
func NewWriter(dev disk.Device, l disk.Layout, epoch uint64, off int64, index uint64, limit int64) *Writer {
	return &Writer{dev: dev, l: l, epoch: epoch, off: off, next: index, limit: limit}
}

// SetLimit moves the logical offset the writer must stay below. The owner
// of the log raises it as older parts of the log become safe to reuse.
func (w *Writer) SetLimit(limit int64) { w.limit = limit }

// Append writes a batch of records with one device write and one sync. This
// is the group commit: throughput is batches per second times batch size.
// The write starts on a block boundary and is padded to a whole number of
// blocks. It returns the index of the first record. On error nothing is
// advanced.
func (w *Writer) Append(batch [][]byte) (uint64, error) {
	var size int64
	for _, p := range batch {
		size += EncodedLen(len(p))
	}
	start := disk.AlignUp(w.off)
	padded := disk.AlignUp(size)
	if padded > int64(w.l.MaxBatch) {
		return 0, ErrTooLarge
	}
	if start+padded > w.limit {
		return 0, ErrLogFull
	}
	buf := make([]byte, 0, padded)
	for i, p := range batch {
		buf = encode(buf, w.epoch, w.next+uint64(i), p)
	}
	buf = buf[:padded] // zero padding to the block boundary
	if err := (ring{w.dev, w.l}).writeAt(buf, start); err != nil {
		return 0, err
	}
	if err := w.dev.Sync(); err != nil {
		return 0, err
	}
	first := w.next
	w.off = start + size
	w.next += uint64(len(batch))
	return first, nil
}

// Fits reports whether a batch with these payload sizes can be appended in
// one write.
func Fits(l disk.Layout, sizes []int) bool {
	var size int64
	for _, n := range sizes {
		size += EncodedLen(n)
	}
	return disk.AlignUp(size) <= int64(l.MaxBatch)
}

// Offset is the end of the last record written. The next batch starts at
// the block boundary at or after it.
func (w *Writer) Offset() int64 { return w.off }

// NextIndex is the index the next record will get.
func (w *Writer) NextIndex() uint64 { return w.next }
