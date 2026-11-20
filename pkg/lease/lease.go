// Package lease decides which node may write the log.
//
// Ownership is agreed entirely through blocks on the shared disk: a Disk
// Paxos ballot (Gafni and Lamport) chooses the owner of each epoch, and a
// timed lease tells the others when they may start a ballot. No message
// passes between nodes, so a network partition between them cannot produce
// two owners.
//
// The safety argument, in short (docs/DESIGN.md has the long form):
//
//   - An owner treats its lease as valid only for TTL*(1-drift) after the
//     START of a renewal that was clean. A renewal is clean if, after its
//     write returned, the owner read the disk and saw no ballot for a later
//     epoch.
//   - A challenger starts a ballot only after it has watched the owner's
//     lease block stay unchanged for TTL*(1+drift), and after writing its
//     own ballot block it reads the owner's lease block once more and backs
//     off if it changed.
//   - Each side writes, then reads. So if both act at once, at least one of
//     them sees the other. Either the owner sees the ballot and stops, or
//     the challenger sees the renewal and backs off.
//
// The one timing assumption is that two nodes' clocks do not differ in rate
// by more than the configured drift over one lease period.
package lease

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/shpwrck/duet/pkg/disk"
)

var (
	ErrNotExpired      = errors.New("lease: current owner's lease has not expired")
	ErrOwnerAlive      = errors.New("lease: owner renewed during takeover; backing off")
	ErrContended       = errors.New("lease: another ballot is in progress")
	ErrLostBallot      = errors.New("lease: another node owns the new epoch")
	ErrLeaseLost       = errors.New("lease: ownership lost")
	ErrNotOwner        = errors.New("lease: not the owner")
	ErrUnclean         = errors.New("lease: renewal could not be confirmed")
	ErrEpochsExhausted = errors.New("lease: epoch table is full")
)

// Clock is a monotonic time source. Only differences are used.
type Clock interface {
	Now() time.Duration
}

// EpochRecord is one row of the epoch table: who owns an epoch and where
// its records live.
type EpochRecord struct {
	Epoch         uint64
	Owner         int
	Incarnation   uint64 // distinguishes restarts of the same node
	StartIndex    uint64 // index of the epoch's first record
	StartOffset   int64  // where that record goes in the log
	PrevEndOffset int64  // cutoff: end of the previous epoch's valid records
}

const epochRecordSize = 44

func (r EpochRecord) put(b []byte) {
	binary.LittleEndian.PutUint64(b[0:], r.Epoch)
	binary.LittleEndian.PutUint32(b[8:], uint32(r.Owner))
	binary.LittleEndian.PutUint64(b[12:], r.Incarnation)
	binary.LittleEndian.PutUint64(b[20:], r.StartIndex)
	binary.LittleEndian.PutUint64(b[28:], uint64(r.StartOffset))
	binary.LittleEndian.PutUint64(b[36:], uint64(r.PrevEndOffset))
}

func getEpochRecord(b []byte) EpochRecord {
	return EpochRecord{
		Epoch:         binary.LittleEndian.Uint64(b[0:]),
		Owner:         int(binary.LittleEndian.Uint32(b[8:])),
		Incarnation:   binary.LittleEndian.Uint64(b[12:]),
		StartIndex:    binary.LittleEndian.Uint64(b[20:]),
		StartOffset:   int64(binary.LittleEndian.Uint64(b[28:])),
		PrevEndOffset: int64(binary.LittleEndian.Uint64(b[36:])),
	}
}

// leaseBlock is what an owner rewrites to show it is alive.
type leaseBlock struct {
	Incarnation uint64
	Epoch       uint64
	Counter     uint64
	Released    bool
}

// paxosBlock is one node's Disk Paxos state for one instance. The instance
// number is the epoch being decided.
type paxosBlock struct {
	Instance uint64
	MBal     uint64 // ballot this node is running
	Bal      uint64 // ballot in which Inp was set, 0 if none
	Inp      EpochRecord
}

// leaseKey is everything a challenger can see of an owner's lease. If it
// stays identical for long enough, the lease has expired.
type leaseKey struct {
	Owner int
	State disk.BlockState
	leaseBlock
}

// Config configures a Manager.
type Config struct {
	Node        int
	Incarnation uint64  // must differ on every start of the same node
	Drift       float64 // bound on relative clock rate error, for example 0.1
}

// Manager runs the ownership protocol for one node.
type Manager struct {
	dev   disk.Device
	l     disk.Layout
	clock Clock
	cfg   Config

	maxSeen uint64 // highest epoch slot known to be written

	owned      *EpochRecord
	lost       bool
	haveRenew  bool
	renewStart time.Duration
	counter    uint64

	obs      leaseKey
	obsSince time.Duration
	haveObs  bool
}

// New returns a Manager. The device must already be formatted.
func New(dev disk.Device, l disk.Layout, clock Clock, cfg Config) (*Manager, error) {
	if cfg.Node < 0 || cfg.Node >= l.Nodes {
		return nil, errors.New("lease: node id outside layout")
	}
	if cfg.Drift < 0 || cfg.Drift >= 0.5 {
		return nil, errors.New("lease: drift must be in [0, 0.5)")
	}
	return &Manager{dev: dev, l: l, clock: clock, cfg: cfg}, nil
}

func (m *Manager) validFor() time.Duration {
	return time.Duration(float64(m.l.TTL) * (1 - m.cfg.Drift))
}

func (m *Manager) waitFor() time.Duration {
	return time.Duration(float64(m.l.TTL) * (1 + m.cfg.Drift))
}

// Valid reports whether this node may acknowledge writes right now.
func (m *Manager) Valid() bool {
	return m.owned != nil && !m.lost && m.haveRenew &&
		m.clock.Now()-m.renewStart < m.validFor()
}

// Owned returns the epoch this node last acquired, if any. It says nothing
// about whether the lease is still valid.
func (m *Manager) Owned() (EpochRecord, bool) {
	if m.owned == nil {
		return EpochRecord{}, false
	}
	return *m.owned, true
}

// ---- block I/O ----

func (m *Manager) readSlot(epoch uint64) (EpochRecord, disk.BlockState, error) {
	if epoch == 0 || epoch > uint64(m.l.EpochSlots) {
		return EpochRecord{}, disk.BlockEmpty, nil
	}
	p, st, err := disk.ReadBlock(m.dev, m.l.SlotOff(epoch))
	if err != nil || st != disk.BlockValid {
		return EpochRecord{}, st, err
	}
	if len(p) < epochRecordSize {
		return EpochRecord{}, disk.BlockCorrupt, nil
	}
	r := getEpochRecord(p)
	if r.Epoch != epoch {
		return EpochRecord{}, disk.BlockCorrupt, nil
	}
	return r, disk.BlockValid, nil
}

// ReadSlot returns the epoch table entry for an epoch.
func (m *Manager) ReadSlot(epoch uint64) (EpochRecord, disk.BlockState, error) {
	return m.readSlot(epoch)
}

func (m *Manager) writeSlot(r EpochRecord) error {
	p := make([]byte, epochRecordSize)
	r.put(p)
	return disk.WriteBlock(m.dev, m.l.SlotOff(r.Epoch), p)
}

func (m *Manager) readLease(node int) (leaseBlock, disk.BlockState, error) {
	p, st, err := disk.ReadBlock(m.dev, m.l.LeaseOff(node))
	if err != nil || st != disk.BlockValid {
		return leaseBlock{}, st, err
	}
	if len(p) < 25 {
		return leaseBlock{}, disk.BlockCorrupt, nil
	}
	return leaseBlock{
		Incarnation: binary.LittleEndian.Uint64(p[0:]),
		Epoch:       binary.LittleEndian.Uint64(p[8:]),
		Counter:     binary.LittleEndian.Uint64(p[16:]),
		Released:    p[24] == 1,
	}, disk.BlockValid, nil
}

func (m *Manager) writeLease(b leaseBlock) error {
	p := make([]byte, 25)
	binary.LittleEndian.PutUint64(p[0:], b.Incarnation)
	binary.LittleEndian.PutUint64(p[8:], b.Epoch)
	binary.LittleEndian.PutUint64(p[16:], b.Counter)
	if b.Released {
		p[24] = 1
	}
	return disk.WriteBlock(m.dev, m.l.LeaseOff(m.cfg.Node), p)
}

func (m *Manager) readPaxos(node int) (paxosBlock, disk.BlockState, error) {
	p, st, err := disk.ReadBlock(m.dev, m.l.PaxosOff(node))
	if err != nil || st != disk.BlockValid {
		return paxosBlock{}, st, err
	}
	if len(p) < 24+epochRecordSize {
		return paxosBlock{}, disk.BlockCorrupt, nil
	}
	return paxosBlock{
		Instance: binary.LittleEndian.Uint64(p[0:]),
		MBal:     binary.LittleEndian.Uint64(p[8:]),
		Bal:      binary.LittleEndian.Uint64(p[16:]),
		Inp:      getEpochRecord(p[24:]),
	}, disk.BlockValid, nil
}

func (m *Manager) writePaxos(b paxosBlock) error {
	p := make([]byte, 24+epochRecordSize)
	binary.LittleEndian.PutUint64(p[0:], b.Instance)
	binary.LittleEndian.PutUint64(p[8:], b.MBal)
	binary.LittleEndian.PutUint64(p[16:], b.Bal)
	b.Inp.put(p[24:])
	return disk.WriteBlock(m.dev, m.l.PaxosOff(m.cfg.Node), p)
}

// Latest returns the newest epoch recorded in the epoch table.
func (m *Manager) Latest() (EpochRecord, bool, error) {
	var last EpochRecord
	have := false
	if m.maxSeen > 0 {
		r, st, err := m.readSlot(m.maxSeen)
		if err != nil {
			return EpochRecord{}, false, err
		}
		if st == disk.BlockValid {
			last, have = r, true
		} else {
			m.maxSeen = 0 // should not happen; rescan from the start
		}
	}
	for e := m.maxSeen + 1; e <= uint64(m.l.EpochSlots); e++ {
		r, st, err := m.readSlot(e)
		if err != nil {
			return EpochRecord{}, false, err
		}
		if st != disk.BlockValid {
			break
		}
		last, have, m.maxSeen = r, true, e
	}
	return last, have, nil
}

// NextEpochStirring reports whether anything on disk suggests that an epoch
// after e is being decided or already exists. A reader that sees this must
// not trust records it read from epoch e until the next slot tells it where
// e was cut off.
func (m *Manager) NextEpochStirring(e uint64) (bool, error) {
	for j := 0; j < m.l.Nodes; j++ {
		pb, st, err := m.readPaxos(j)
		if err != nil {
			return false, err
		}
		if st == disk.BlockCorrupt || (st == disk.BlockValid && pb.Instance > e && pb.MBal > 0) {
			return true, nil
		}
	}
	// The slot last, for the reason given in fence.
	_, st, err := m.readSlot(e + 1)
	if err != nil {
		return false, err
	}
	return st != disk.BlockEmpty, nil
}

// ---- owner side ----

// Renew extends the lease. The owner calls it several times per TTL.
func (m *Manager) Renew() error {
	if m.owned == nil || m.lost {
		return ErrNotOwner
	}
	start := m.clock.Now() // validity counts from before the write is issued
	m.counter++
	if err := m.writeLease(leaseBlock{Incarnation: m.cfg.Incarnation, Epoch: m.owned.Epoch, Counter: m.counter}); err != nil {
		return err
	}
	// Write, then read: a challenger does the same in the other order of
	// blocks, so one of us is guaranteed to see the other.
	if err := m.fence(); err != nil {
		return err
	}
	m.renewStart, m.haveRenew = start, true
	return nil
}

// fence reads the disk for any sign that a later epoch exists or is being
// decided. It is the read half of the owner's write-then-read handshake.
func (m *Manager) fence() error {
	unclean := false
	for j := 0; j < m.l.Nodes; j++ {
		if j == m.cfg.Node {
			continue
		}
		pb, st, err := m.readPaxos(j)
		if err != nil {
			return err
		}
		if st == disk.BlockCorrupt {
			unclean = true // possibly a ballot block in mid-write
			continue
		}
		if st == disk.BlockValid && pb.Instance > m.owned.Epoch && pb.MBal > 0 {
			m.lost = true
			return ErrLeaseLost
		}
	}
	// The slot is read last, and the order matters. These are separate
	// reads and this node may stall between them. A ballot block that
	// showed a later epoch can be wiped again (retreat), but a node only
	// moves on to the epoch after, where that can happen, once the slot
	// for this one is written. So a ballot that fixed a cutoff is visible
	// either in its block when that is read, or in the slot afterwards.
	// Reading the slot first leaves a gap: see
	// TestOwnerStalledBetweenTheReadsOfItsCheck.
	if _, st, err := m.readSlot(m.owned.Epoch + 1); err != nil {
		return err
	} else if st != disk.BlockEmpty {
		m.lost = true
		return ErrLeaseLost
	}
	if unclean {
		return ErrUnclean
	}
	return nil
}

// Confirm reports whether this node may acknowledge what it has just
// written. It must be called after the write has been flushed.
//
// This is what makes acknowledgements safe, and it uses no clock. The owner
// writes its records and then reads the ballot blocks; a challenger writes
// its ballot block and then reads the log to find the cutoff. If the owner
// sees no ballot, its records were on disk before the challenger's ballot
// block was, so the challenger's cutoff includes them. If it sees one, it
// does not acknowledge.
//
// A lease judged by the local clock is not enough for this. A VM that is
// suspended by its hypervisor and resumed finds its clock where it left
// it, so by that clock the lease never expired.
func (m *Manager) Confirm() error {
	if m.owned == nil || m.lost {
		return ErrNotOwner
	}
	return m.fence()
}

// Release gives up ownership so another node can take over without waiting
// for the lease to expire. The node stops acknowledging writes before the
// release is written.
func (m *Manager) Release() error {
	if m.owned == nil {
		return ErrNotOwner
	}
	m.lost = true
	m.counter++
	return m.writeLease(leaseBlock{Incarnation: m.cfg.Incarnation, Epoch: m.owned.Epoch, Counter: m.counter, Released: true})
}

// ---- challenger side ----

// ProposeFunc builds the record for a new epoch. It is called after this
// node's ballot block is on disk and the old owner is known to be excluded,
// which is the earliest moment at which the previous epoch's tail is final.
// prev is nil for the first epoch. The callee fills StartIndex, StartOffset
// and PrevEndOffset; the Manager fills the rest.
type ProposeFunc func(prev *EpochRecord) (EpochRecord, error)

// TryAcquire attempts to become owner of the next epoch. It never blocks
// waiting for a lease to expire: it returns ErrNotExpired and the caller
// tries again later. On success the returned record is this node's epoch
// and Valid() is true.
func (m *Manager) TryAcquire(propose ProposeFunc) (EpochRecord, error) {
	if m.Valid() {
		return *m.owned, nil
	}
	latest, have, err := m.Latest()
	if err != nil {
		return EpochRecord{}, err
	}

	var watched leaseKey
	dekker := false
	if have {
		mine := latest.Owner == m.cfg.Node
		switch {
		case mine && latest.Incarnation == m.cfg.Incarnation && (m.owned == nil || m.owned.Epoch != latest.Epoch):
			// The table says this incarnation owns the latest epoch, but we
			// did not record winning it: another node finished our ballot
			// for us. Take it up; a clean renewal is still required.
			r := latest
			m.owned, m.lost, m.haveRenew = &r, false, false
			if err := m.Renew(); err != nil {
				return EpochRecord{}, err
			}
			return r, nil
		case mine && latest.Incarnation == m.cfg.Incarnation:
			// We own it but the lease lapsed or was lost. If nobody has
			// started a ballot, a clean renewal is enough to carry on.
			if !m.lost {
				if err := m.Renew(); err == nil {
					return *m.owned, nil
				} else if !errors.Is(err, ErrLeaseLost) {
					return EpochRecord{}, err
				}
			}
			// Otherwise give it up explicitly and stand for the next epoch.
			if err := m.Release(); err != nil {
				return EpochRecord{}, err
			}
		case mine:
			// A previous incarnation of this node owned it. That process is
			// gone, so there is nothing to wait for.
		default:
			lb, st, err := m.readLease(latest.Owner)
			if err != nil {
				return EpochRecord{}, err
			}
			released := st == disk.BlockValid && lb.Released &&
				lb.Epoch == latest.Epoch && lb.Incarnation == latest.Incarnation
			if !released {
				key := leaseKey{Owner: latest.Owner, State: st, leaseBlock: lb}
				now := m.clock.Now() // taken after the read returned
				if !m.haveObs || m.obs != key {
					m.obs, m.obsSince, m.haveObs = key, now, true
					return EpochRecord{}, ErrNotExpired
				}
				if now-m.obsSince < m.waitFor() {
					return EpochRecord{}, ErrNotExpired
				}
				watched, dekker = key, true
			}
		}
	}

	inst := uint64(1)
	if have {
		inst = latest.Epoch + 1
	}
	if inst > uint64(m.l.EpochSlots) {
		return EpochRecord{}, ErrEpochsExhausted
	}
	chosen, err := m.ballot(inst, latest, have, watched, dekker, propose)
	if err != nil {
		return EpochRecord{}, err
	}
	if chosen.Owner != m.cfg.Node || chosen.Incarnation != m.cfg.Incarnation {
		return chosen, ErrLostBallot
	}
	m.owned, m.lost, m.haveRenew, m.haveObs = &chosen, false, false, false
	if err := m.Renew(); err != nil {
		return EpochRecord{}, err
	}
	return chosen, nil
}

// ballot runs one Disk Paxos ballot for instance inst on the single shared
// disk and returns the chosen record.
func (m *Manager) ballot(inst uint64, latest EpochRecord, have bool, watched leaseKey, dekker bool, propose ProposeFunc) (EpochRecord, error) {
	// Recover our own state for this instance, if any.
	own, st, err := m.readPaxos(m.cfg.Node)
	if err != nil {
		return EpochRecord{}, err
	}
	if st != disk.BlockValid || own.Instance != inst {
		own = paxosBlock{Instance: inst}
	}
	// Pick a ballot number above everything visible, unique to this node.
	top := own.MBal
	for j := 0; j < m.l.Nodes; j++ {
		if j == m.cfg.Node {
			continue
		}
		pb, st, err := m.readPaxos(j)
		if err != nil {
			return EpochRecord{}, err
		}
		if st == disk.BlockValid && pb.Instance == inst && pb.MBal > top {
			top = pb.MBal
		}
	}
	n := uint64(m.l.Nodes)
	own.MBal = (top/n+1)*n + uint64(m.cfg.Node)

	// Phase 1: announce the ballot, then look.
	if err := m.writePaxos(own); err != nil {
		return EpochRecord{}, err
	}
	best, err := m.survey(inst, own)
	if err != nil {
		m.retreat(own)
		return EpochRecord{}, err
	}
	if dekker {
		lb, st, err := m.readLease(watched.Owner)
		if err != nil {
			return EpochRecord{}, err
		}
		if (leaseKey{Owner: watched.Owner, State: st, leaseBlock: lb}) != watched {
			m.haveObs = false
			m.retreat(own)
			return EpochRecord{}, ErrOwnerAlive
		}
	}

	// Disk Paxos never erases a ballot block. duet does (retreat), so the
	// blocks alone cannot be trusted to show that this instance was already
	// decided: the node that decided it may since have stood for the next
	// one and withdrawn. It could only do that after the slot was written,
	// so the slot, read after the blocks, settles it. This matters when
	// this node chose the instance from a view of the table that had gone
	// stale by the time it got here.
	if r, st, err := m.readSlot(inst); err != nil {
		return EpochRecord{}, err
	} else if st == disk.BlockValid {
		m.retreat(own)
		if inst > m.maxSeen {
			m.maxSeen = inst
		}
		return r, nil
	}

	// Choose the value: one already accepted in this instance wins.
	value := best.Inp
	if best.Bal == 0 {
		var prev *EpochRecord
		if have {
			prev = &latest
		}
		value, err = propose(prev)
		if err != nil {
			m.retreat(own)
			return EpochRecord{}, err
		}
		value.Epoch, value.Owner, value.Incarnation = inst, m.cfg.Node, m.cfg.Incarnation
	}

	// Phase 2: accept the value, then look again.
	own.Bal, own.Inp = own.MBal, value
	if err := m.writePaxos(own); err != nil {
		return EpochRecord{}, err
	}
	if _, err := m.survey(inst, own); err != nil {
		return EpochRecord{}, err
	}

	// Chosen. Record it in the epoch table; the write is idempotent because
	// every node that completes this instance arrives at the same value.
	if r, st, err := m.readSlot(inst); err != nil {
		return EpochRecord{}, err
	} else if st == disk.BlockValid {
		// Already recorded. If a ballot block was erased between our two
		// surveys, what is recorded need not be what we arrived at, and
		// the table is what every reader follows.
		value = r
	} else if err := m.writeSlot(value); err != nil {
		return EpochRecord{}, err
	}
	if inst > m.maxSeen {
		m.maxSeen = inst
	}
	return value, nil
}

// survey reads every other node's ballot block. It fails if anyone is
// running a higher ballot in this instance or has moved past it. Otherwise
// it returns the block (possibly our own) holding the value accepted in the
// highest ballot, with Bal == 0 if no value has been accepted yet.
func (m *Manager) survey(inst uint64, own paxosBlock) (paxosBlock, error) {
	best := own
	for j := 0; j < m.l.Nodes; j++ {
		if j == m.cfg.Node {
			continue
		}
		pb, st, err := m.readPaxos(j)
		if err != nil {
			return paxosBlock{}, err
		}
		switch {
		case st == disk.BlockCorrupt:
			return paxosBlock{}, ErrContended // mid-write; try again
		case st != disk.BlockValid:
			continue
		case pb.Instance > inst:
			return paxosBlock{}, ErrContended // this instance is already decided
		case pb.Instance == inst && pb.MBal > own.MBal:
			return paxosBlock{}, ErrContended
		case pb.Instance == inst && pb.Bal > best.Bal:
			best = pb
		}
	}
	return best, nil
}

// retreat withdraws a ballot that never reached phase 2, so that an owner
// who is in fact alive is not forced to step down by a stale ballot block.
// It is only safe while nothing was accepted under this ballot.
func (m *Manager) retreat(own paxosBlock) {
	if own.Bal != 0 {
		return
	}
	_ = m.writePaxos(paxosBlock{})
}
