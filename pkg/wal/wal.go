// Package wal is the record log on the shared disk.
//
// The log is one byte range. Each ownership epoch writes a contiguous run
// of records inside it, starting at the offset recorded for that epoch in
// the epoch table. Records carry their epoch and index, so a reader can
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

// readRecord returns the record at off if it is a valid record with exactly
// this epoch and index. Anything else, including zeroes, a torn write, or a
// record from another epoch, reports ok=false.
func readRecord(dev disk.Device, l disk.Layout, off int64, epoch, index uint64) (Record, bool, error) {
	if off < l.LogStart() || off+headerSize > l.LogEnd() {
		return Record{}, false, nil
	}
	var h [headerSize]byte
	if _, err := dev.ReadAt(h[:], off); err != nil {
		return Record{}, false, err
	}
	if binary.LittleEndian.Uint32(h[0:]) != recMagic {
		return Record{}, false, nil
	}
	n := int(binary.LittleEndian.Uint32(h[4:]))
	if n > l.MaxBatch || off+EncodedLen(n) > l.LogEnd() {
		return Record{}, false, nil
	}
	if binary.LittleEndian.Uint64(h[8:]) != epoch || binary.LittleEndian.Uint64(h[16:]) != index {
		return Record{}, false, nil
	}
	payload := make([]byte, n)
	if n > 0 {
		if _, err := dev.ReadAt(payload, off+headerSize); err != nil {
			return Record{}, false, err
		}
	}
	crc := crc32.Checksum(h[0:24], castagnoli)
	crc = crc32.Update(crc, castagnoli, payload)
	if binary.LittleEndian.Uint32(h[24:]) != crc {
		return Record{}, false, nil
	}
	return Record{Epoch: epoch, Index: index, Payload: payload, End: off + EncodedLen(n)}, true, nil
}

// Scan reads consecutive valid records of one epoch, starting with the
// record expected at (off, index). It stops at the first position that does
// not hold the expected record, or at limit if limit is non-negative. fn may
// be nil. It returns the offset and index where the next record would go.
func Scan(dev disk.Device, l disk.Layout, epoch uint64, off int64, index uint64, limit int64, fn func(Record)) (int64, uint64, error) {
	for {
		if limit >= 0 && off >= limit {
			return off, index, nil
		}
		rec, ok, err := readRecord(dev, l, off, epoch, index)
		if err != nil {
			return off, index, err
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
}

// NewWriter returns a writer positioned at the start of an epoch.
func NewWriter(dev disk.Device, l disk.Layout, epoch uint64, off int64, index uint64) *Writer {
	return &Writer{dev: dev, l: l, epoch: epoch, off: off, next: index}
}

// Append writes a batch of records with one device write and one sync. This
// is the group commit: throughput is batches per second times batch size.
// It returns the index of the first record. On error nothing is advanced.
func (w *Writer) Append(batch [][]byte) (uint64, error) {
	var size int64
	for _, p := range batch {
		size += EncodedLen(len(p))
	}
	if size > int64(w.l.MaxBatch) {
		return 0, ErrTooLarge
	}
	if w.off+size > w.l.LogEnd() {
		return 0, ErrLogFull
	}
	buf := make([]byte, 0, size)
	for i, p := range batch {
		buf = encode(buf, w.epoch, w.next+uint64(i), p)
	}
	if _, err := w.dev.WriteAt(buf, w.off); err != nil {
		return 0, err
	}
	if err := w.dev.Sync(); err != nil {
		return 0, err
	}
	first := w.next
	w.off += size
	w.next += uint64(len(batch))
	return first, nil
}

// Offset is where the next record will be written.
func (w *Writer) Offset() int64 { return w.off }

// NextIndex is the index the next record will get.
func (w *Writer) NextIndex() uint64 { return w.next }
