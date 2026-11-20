package disk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// FileDevice is a Device backed by a file or a raw block device.
//
// With direct I/O it bypasses the page cache, which is required whenever
// the device is shared between machines: a second kernel's page cache knows
// nothing about writes made by the first. Without direct I/O it is only
// correct when every node is a process on the same kernel.
type FileDevice struct {
	f      *os.File
	size   int64
	direct bool
}

// OpenFile opens an existing file or block device. If size is zero the
// whole device is used. direct requests O_DIRECT; it is an error if the
// platform or filesystem cannot provide it.
func OpenFile(path string, size int64, direct bool) (*FileDevice, error) {
	flags := os.O_RDWR
	if direct {
		if oDirect == 0 {
			return nil, errors.New("disk: direct I/O is not supported on this platform")
		}
		flags |= oDirect
	}
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	if size == 0 {
		size = end &^ (BlockSize - 1)
	}
	if end < size {
		f.Close()
		return nil, fmt.Errorf("disk: device has %d bytes, need %d", end, size)
	}
	return &FileDevice{f: f, size: size, direct: direct}, nil
}

// alignedBuf returns n bytes whose address is a multiple of BlockSize.
func alignedBuf(n int) []byte {
	b := make([]byte, n+BlockSize)
	skip := 0
	if rem := int(uintptr(unsafe.Pointer(&b[0])) & (BlockSize - 1)); rem != 0 {
		skip = BlockSize - rem
	}
	return b[skip : skip+n : skip+n]
}

// ReadAt reads any byte range. Under direct I/O it reads the enclosing
// blocks into an aligned buffer and copies out.
func (d *FileDevice) ReadAt(p []byte, off int64) (int, error) {
	if !d.direct {
		return d.f.ReadAt(p, off)
	}
	start := off &^ (BlockSize - 1)
	end := AlignUp(off + int64(len(p)))
	if off < 0 || end > d.size {
		return 0, io.ErrUnexpectedEOF
	}
	buf := alignedBuf(int(end - start))
	if _, err := d.f.ReadAt(buf, start); err != nil {
		return 0, err
	}
	return copy(p, buf[off-start:]), nil
}

// WriteAt writes whole blocks. Every write duet issues is block aligned;
// anything else is a bug, and under direct I/O it is rejected here instead
// of by the kernel with a less helpful error.
func (d *FileDevice) WriteAt(p []byte, off int64) (int, error) {
	if !d.direct {
		return d.f.WriteAt(p, off)
	}
	if off%BlockSize != 0 || len(p)%BlockSize != 0 {
		return 0, fmt.Errorf("disk: unaligned direct write of %d bytes at %d", len(p), off)
	}
	if off < 0 || off+int64(len(p)) > d.size {
		return 0, io.ErrShortWrite
	}
	buf := alignedBuf(len(p))
	copy(buf, p)
	return d.f.WriteAt(buf, off)
}

// Sync flushes the device's write cache. Direct I/O skips the page cache
// but does not by itself make a write durable.
func (d *FileDevice) Sync() error  { return d.f.Sync() }
func (d *FileDevice) Size() int64  { return d.size }
func (d *FileDevice) Close() error { return d.f.Close() }
