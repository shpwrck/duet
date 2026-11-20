package disk

import (
	"crypto/rand"
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
	formatVer    = 2
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
	// CkptSize is the space for one checkpoint, in bytes. Every node has
	// two such slots. Zero means the disk holds no checkpoints.
	CkptSize int64
	// ID is chosen at random when the disk is formatted. State a node
	// keeps on its own disk is tied to it, so that state built from one
	// log is never continued from another.
	ID [16]byte
}

func (l Layout) Validate() error {
	switch {
	case l.Nodes < 2 || l.Nodes > 64:
		return errors.New("disk: Nodes must be between 2 and 64")
	case l.EpochSlots < 1:
		return errors.New("disk: EpochSlots must be positive")
	case l.TTL <= 0:
		return errors.New("disk: TTL must be positive")
	case l.MaxBatch < BlockSize || l.MaxBatch%BlockSize != 0:
		return errors.New("disk: MaxBatch must be a positive multiple of BlockSize")
	case l.LogSize < int64(4*l.MaxBatch):
		return errors.New("disk: LogSize must hold at least four batches")
	case l.LogSize%BlockSize != 0:
		return errors.New("disk: LogSize must be a multiple of BlockSize")
	case l.CkptSize < 0 || l.CkptSize%BlockSize != 0 || (l.CkptSize != 0 && l.CkptSize < 2*BlockSize):
		return errors.New("disk: CkptSize must be zero or a multiple of BlockSize of at least two blocks")
	}
	return nil
}

func (l Layout) LeaseOff(node int) int64 { return BlockSize * int64(1+node) }
func (l Layout) PaxosOff(node int) int64 { return BlockSize * int64(1+l.Nodes+node) }
func (l Layout) SlotOff(epoch uint64) int64 {
	return BlockSize * (int64(1+2*l.Nodes) + int64(epoch-1))
}

// TrimOff is a node's trim block: what it promises about its own past
// writes and how much of the log it knows to be covered by a checkpoint.
func (l Layout) TrimOff(node int) int64 {
	return BlockSize * (int64(1+2*l.Nodes+l.EpochSlots) + int64(node))
}
func (l Layout) LogStart() int64 { return BlockSize * int64(1+3*l.Nodes+l.EpochSlots) }

// LogPhys maps a logical log offset to its place on the device. Logical
// offsets start at LogStart and only grow; the log region is reused in
// laps, so an offset one LogSize further on lands on the same bytes.
func (l Layout) LogPhys(off int64) int64 {
	return l.LogStart() + (off-l.LogStart())%l.LogSize
}
func (l Layout) LogEnd() int64 { return l.LogStart() + l.LogSize }

// CkptOff is where one of a node's two checkpoint slots starts.
func (l Layout) CkptOff(node, slot int) int64 {
	return l.LogEnd() + int64(2*node+slot)*l.CkptSize
}
func (l Layout) Total() int64 { return l.LogEnd() + int64(2*l.Nodes)*l.CkptSize }

// AlignUp rounds n up to the next multiple of BlockSize. Log batches start
// and end on block boundaries so that every write is a whole number of
// blocks, which direct I/O requires and which keeps one writer's blocks
// from overlapping another's.
func AlignUp(n int64) int64 { return (n + BlockSize - 1) &^ (BlockSize - 1) }

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
	for n := 0; n < l.Nodes && l.CkptSize > 0; n++ {
		for slot := 0; slot < 2; slot++ {
			if _, err := dev.WriteAt(zero, l.CkptOff(n, slot)); err != nil {
				return err
			}
		}
	}
	if l.ID == ([16]byte{}) {
		if _, err := rand.Read(l.ID[:]); err != nil {
			return err
		}
	}
	p := make([]byte, 60)
	copy(p[44:], l.ID[:])
	binary.LittleEndian.PutUint32(p[0:], superMagic)
	binary.LittleEndian.PutUint32(p[4:], formatVer)
	binary.LittleEndian.PutUint32(p[8:], uint32(l.Nodes))
	binary.LittleEndian.PutUint32(p[12:], uint32(l.EpochSlots))
	binary.LittleEndian.PutUint64(p[16:], uint64(l.LogSize))
	binary.LittleEndian.PutUint64(p[24:], uint64(l.TTL))
	binary.LittleEndian.PutUint32(p[32:], uint32(l.MaxBatch))
	binary.LittleEndian.PutUint64(p[36:], uint64(l.CkptSize))
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
	if len(p) >= 44 {
		l.CkptSize = int64(binary.LittleEndian.Uint64(p[36:]))
	}
	if len(p) >= 60 {
		copy(l.ID[:], p[44:60])
	}
	if err := l.Validate(); err != nil {
		return Layout{}, err
	}
	return l, nil
}
