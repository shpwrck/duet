package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// BlockSize is the size of every metadata block.
const BlockSize = 4096

const (
	superMagic   = 0x44554554 // "DUET"
	blockMagic   = 0x64626c6b // "dblk"
	formatVer    = 1
	payloadLimit = BlockSize - 12 // magic + length + crc
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Layout describes where everything lives on the shared disk.
//
//	block 0                      superblock
//	blocks 1 .. N                lease block per node
//	blocks 1+N .. 2N             Disk Paxos block per node
//	next EpochSlots blocks       epoch table, slot e-1 holds epoch e
//	remainder                    log
type Layout struct {
	Nodes      int           // how many nodes may share the disk (2 or more)
	EpochSlots int           // how many ownership changes the disk can record
	LogSize    int64         // bytes reserved for the log
	TTL        time.Duration // lease length, cluster-wide
	MaxBatch   int           // largest single log write in bytes
}

func (l Layout) Validate() error {
	switch {
	case l.Nodes < 2 || l.Nodes > 64:
		return errors.New("disk: Nodes must be between 2 and 64")
	case l.EpochSlots < 1:
		return errors.New("disk: EpochSlots must be positive")
	case l.TTL <= 0:
		return errors.New("disk: TTL must be positive")
	case l.MaxBatch < 64:
		return errors.New("disk: MaxBatch too small")
	case l.LogSize < int64(4*l.MaxBatch):
		return errors.New("disk: LogSize must hold at least four batches")
	}
	return nil
}

func (l Layout) LeaseOff(node int) int64 { return BlockSize * int64(1+node) }
func (l Layout) PaxosOff(node int) int64 { return BlockSize * int64(1+l.Nodes+node) }
func (l Layout) SlotOff(epoch uint64) int64 {
	return BlockSize * (int64(1+2*l.Nodes) + int64(epoch-1))
}
func (l Layout) LogStart() int64 { return BlockSize * int64(1+2*l.Nodes+l.EpochSlots) }
func (l Layout) LogEnd() int64   { return l.LogStart() + l.LogSize }
func (l Layout) Total() int64    { return l.LogEnd() }

// WriteBlock writes payload as one checksummed metadata block and syncs.
func WriteBlock(dev Device, off int64, payload []byte) error {
	if len(payload) > payloadLimit {
		return errors.New("disk: block payload too large")
	}
	var b [BlockSize]byte
	binary.LittleEndian.PutUint32(b[0:], blockMagic)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(payload)))
	copy(b[8:], payload)
	binary.LittleEndian.PutUint32(b[BlockSize-4:], crc32.Checksum(b[:BlockSize-4], castagnoli))
	if _, err := dev.WriteAt(b[:], off); err != nil {
		return err
	}
	return dev.Sync()
}

// BlockState says what a metadata block read found.
type BlockState int

const (
	BlockEmpty   BlockState = iota // never written
	BlockValid                     // checksum matches
	BlockCorrupt                   // written, but torn or damaged
)

// ReadBlock reads one metadata block.
func ReadBlock(dev Device, off int64) ([]byte, BlockState, error) {
	var b [BlockSize]byte
	if _, err := dev.ReadAt(b[:], off); err != nil {
		return nil, BlockCorrupt, err
	}
	zero := true
	for _, c := range b {
		if c != 0 {
			zero = false
			break
		}
	}
	if zero {
		return nil, BlockEmpty, nil
	}
	if binary.LittleEndian.Uint32(b[0:]) != blockMagic {
		return nil, BlockCorrupt, nil
	}
	n := binary.LittleEndian.Uint32(b[4:])
	if n > payloadLimit {
		return nil, BlockCorrupt, nil
	}
	if binary.LittleEndian.Uint32(b[BlockSize-4:]) != crc32.Checksum(b[:BlockSize-4], castagnoli) {
		return nil, BlockCorrupt, nil
	}
	out := make([]byte, n)
	copy(out, b[8:8+n])
	return out, BlockValid, nil
}

// Format writes a fresh superblock and zeroes all metadata. It destroys
// any existing contents.
func Format(dev Device, l Layout) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if dev.Size() < l.Total() {
		return fmt.Errorf("disk: device has %d bytes, layout needs %d", dev.Size(), l.Total())
	}
	zero := make([]byte, BlockSize)
	for off := int64(BlockSize); off < l.LogStart(); off += BlockSize {
		if _, err := dev.WriteAt(zero, off); err != nil {
			return err
		}
	}
	// The first bytes of the log must not look like a record.
	if _, err := dev.WriteAt(zero, l.LogStart()); err != nil {
		return err
	}
	p := make([]byte, 44)
	binary.LittleEndian.PutUint32(p[0:], superMagic)
	binary.LittleEndian.PutUint32(p[4:], formatVer)
	binary.LittleEndian.PutUint32(p[8:], uint32(l.Nodes))
	binary.LittleEndian.PutUint32(p[12:], uint32(l.EpochSlots))
	binary.LittleEndian.PutUint64(p[16:], uint64(l.LogSize))
	binary.LittleEndian.PutUint64(p[24:], uint64(l.TTL))
	binary.LittleEndian.PutUint32(p[32:], uint32(l.MaxBatch))
	return WriteBlock(dev, 0, p)
}

// Open reads the superblock and returns the layout.
func Open(dev Device) (Layout, error) {
	p, st, err := ReadBlock(dev, 0)
	if err != nil {
		return Layout{}, err
	}
	if st != BlockValid || len(p) < 36 || binary.LittleEndian.Uint32(p[0:]) != superMagic {
		return Layout{}, errors.New("disk: no duet superblock found")
	}
	if v := binary.LittleEndian.Uint32(p[4:]); v != formatVer {
		return Layout{}, fmt.Errorf("disk: unsupported format version %d", v)
	}
	l := Layout{
		Nodes:      int(binary.LittleEndian.Uint32(p[8:])),
		EpochSlots: int(binary.LittleEndian.Uint32(p[12:])),
		LogSize:    int64(binary.LittleEndian.Uint64(p[16:])),
		TTL:        time.Duration(binary.LittleEndian.Uint64(p[24:])),
		MaxBatch:   int(binary.LittleEndian.Uint32(p[32:])),
	}
	if err := l.Validate(); err != nil {
		return Layout{}, err
	}
	return l, nil
}
