// Package sm is the state machine: etcd's own storage, transaction and
// lease code, driven by duet's log instead of by raft.
//
// Reusing etcd's packages means range, revision, transaction, watch and
// compaction behaviour are etcd's by construction, not a reimplementation.
// The cost is a dependency on go.etcd.io/etcd/server/v3, which makes no
// API stability promise to importers, so it is pinned and kept behind this
// package.
//
// A Store is rebuilt by replaying the log, from empty, from a checkpoint
// (Restore), or from the file a previous run left behind (OpenLocal).
package sm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/etcdserver/txn"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Command is one log record: an etcd request in etcd's own internal
// envelope. Exactly one of Put, DeleteRange, Txn, Compaction, LeaseGrant
// or LeaseRevoke is set.
type Command = pb.InternalRaftRequest

var marshal = proto.MarshalOptions{Deterministic: true}

// Encode serialises a command for the log.
func Encode(c *Command) ([]byte, error) { return marshal.Marshal(c) }

// Result is the outcome of applying one command. Err is part of the
// outcome: every replica that applies the command gets the same Err.
type Result struct {
	Index    uint64
	Revision int64 // store revision after the command
	Resp     proto.Message
	Err      error
}

// Store is one replica's state.
type Store struct {
	lg      *zap.Logger
	dir     string
	be      backend.Backend
	le      lease.Lessor
	kv      mvcc.WatchableKV
	applied atomic.Uint64

	// Guarded by the backend's transaction lock, except applying, which
	// only the applying goroutine touches.
	applying uint64 // record being applied
	durable  uint64 // last record whose writes are in the open transaction
	tag      []byte
	armed    atomic.Bool
}

// The state file lives at a fixed place under the state directory so that a
// node that restarts with its own disk intact can carry on from it.
const stateSub = "state"

var appliedKey = []byte("duetApplied")

// ErrNoLocal means the state directory holds nothing OpenLocal can use.
var ErrNoLocal = errors.New("sm: no usable local state")

// Open starts an empty store in dir, discarding any state left there.
func Open(dir string) (*Store, error) { return Restore(dir, nil, 0) }

// Restore starts a store in dir from a snapshot taken at index applied,
// discarding any state left there. A nil snapshot gives an empty store.
func Restore(dir string, snapshot io.Reader, applied uint64) (*Store, error) {
	d := filepath.Join(dir, stateSub)
	if err := os.RemoveAll(d); err != nil {
		return nil, err
	}
	if old, _ := filepath.Glob(filepath.Join(dir, "duet-sm-*")); len(old) > 0 {
		for _, o := range old { // left by versions that used a new directory per start
			os.RemoveAll(o)
		}
	}
	if err := os.MkdirAll(d, 0o700); err != nil {
		return nil, err
	}
	if snapshot != nil {
		f, err := os.OpenFile(filepath.Join(d, "db"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if _, err = io.Copy(f, snapshot); err == nil {
				err = f.Sync()
			}
			if e := f.Close(); err == nil {
				err = e
			}
		}
		if err != nil {
			os.RemoveAll(d)
			return nil, err
		}
	}
	return build(d, func(*Store, []byte) error { return nil }, applied)
}

// OpenLocal opens the state a previous run left in dir, if that run had
// bound it to the same tag (see Bind). The store's Applied is the last
// record whose effects are certainly in the file. A record after it may be
// in the file too, but only one that changed no keys, and applying such a
// record again changes nothing.
func OpenLocal(dir string, tag []byte) (*Store, error) {
	d := filepath.Join(dir, stateSub)
	if fi, err := os.Stat(filepath.Join(d, "db")); err != nil || fi.Size() == 0 || len(tag) == 0 {
		return nil, ErrNoLocal
	}
	var applied uint64
	st, err := build(d, func(st *Store, v []byte) error {
		if len(v) < 8 || !bytes.Equal(v[8:], tag) {
			return ErrNoLocal
		}
		applied = binary.BigEndian.Uint64(v)
		st.tag = append([]byte(nil), tag...)
		return nil
	}, 0)
	if err != nil {
		return nil, err
	}
	st.applied.Store(applied)
	st.applying, st.durable = applied, applied
	return st, nil
}

func build(d string, check func(*Store, []byte) error, applied uint64) (st *Store, err error) {
	lg := zap.NewNop()
	st = &Store{lg: lg, dir: d}
	defer func() {
		if r := recover(); r != nil { // a file bbolt or etcd cannot open
			st, err = nil, fmt.Errorf("%w: %v", ErrNoLocal, r)
		}
	}()
	be := backend.NewDefaultBackend(lg, filepath.Join(d, "db"), func(c *backend.BackendConfig) { c.Hooks = (*commitHook)(st) })
	var v []byte
	func() {
		defer func() { recover() }() // no meta bucket yet in a new file
		tx := be.ReadTx()
		tx.RLock()
		defer tx.RUnlock()
		if _, vs := tx.UnsafeRange(schema.Meta, appliedKey, nil, 0); len(vs) == 1 {
			v = append([]byte(nil), vs[0]...)
		}
	}()
	if err := check(st, v); err != nil {
		be.Close()
		return nil, err
	}
	st.applied.Store(applied)
	st.applying, st.durable = applied, applied
	le := lease.NewLessor(lg, be, nil, lease.LessorConfig{MinLeaseTTL: 1})
	kv := mvcc.New(lg, be, le, mvcc.StoreConfig{})
	le.SetRangeDeleter(func() lease.TxnDelete { return kv.Write(traceutil.TODO()) })
	st.be, st.le, st.kv = be, le, kv
	// etcd's own way of tying "which record has been applied" to the data:
	// every write made while applying a record takes the transaction lock
	// through this hook, and the index is written out by commitHook in
	// the same database transaction as the write itself.
	be.SetTxPostLockInsideApplyHook(func() { st.durable = st.applying })
	st.armed.Store(true)
	return st, nil
}

// commitHook records the applied index in every database commit.
type commitHook Store

func (h *commitHook) OnPreCommitUnsafe(tx backend.UnsafeReadWriter) {
	if !h.armed.Load() {
		return
	}
	v := make([]byte, 8, 8+len(h.tag))
	binary.BigEndian.PutUint64(v, h.durable)
	tx.UnsafePut(schema.Meta, appliedKey, append(v, h.tag...))
}

// Bind marks the state as belonging to tag, which should identify the log
// it was built from. OpenLocal only accepts state bound to the same tag.
func (s *Store) Bind(tag []byte) {
	tx := s.be.BatchTx()
	tx.LockOutsideApply()
	s.tag = append([]byte(nil), tag...)
	tx.Unlock()
}

// Close stops the store and leaves its file in place for OpenLocal.
func (s *Store) Close() error {
	s.le.Stop()
	err := s.kv.Close()
	if e := s.be.Close(); err == nil {
		err = e
	}
	return err
}

// KV exposes the store for reads and watches.
func (s *Store) KV() mvcc.WatchableKV { return s.kv }

// Lessor exposes the lease manager.
func (s *Store) Lessor() lease.Lessor { return s.le }

// Snapshot is a consistent copy of a store as of one log index.
type Snapshot struct {
	Applied uint64
	Size    int64
	snap    backend.Snapshot
}

// WriteTo writes the snapshot. It may run while the store keeps applying.
func (s *Snapshot) WriteTo(w io.Writer) (int64, error) { return s.snap.WriteTo(w) }
func (s *Snapshot) Close() error                       { return s.snap.Close() }

// Snapshot captures the store as it is now. It must not run concurrently
// with Apply. The caller must Close the result.
func (s *Store) Snapshot() *Snapshot {
	s.be.ForceCommit()
	snap := s.be.Snapshot()
	return &Snapshot{Applied: s.applied.Load(), Size: snap.Size(), snap: snap}
}

// Applied is the index of the last command applied.
func (s *Store) Applied() uint64 { return s.applied.Load() }

// EtcdVersion is the etcd release whose storage code this package runs,
// reported to clients that ask which etcd they are talking to.
const EtcdVersion = "3.7.4"

// DbSize is the size of the store's database file in bytes.
func (s *Store) DbSize() int64 { return s.be.Size() }

// Revision is the store's current revision.
func (s *Store) Revision() int64 { return s.kv.Rev() }

// Hash is a checksum of the store's contents up to its current revision.
// Two replicas that applied the same log have the same hash.
func (s *Store) Hash() (uint32, int64, error) {
	h, _, err := s.kv.HashStorage().HashByRev(0)
	return h.Hash, h.Revision, err
}

// Apply applies the command stored at a log index. Commands must be
// applied in index order, each exactly once.
func (s *Store) Apply(index uint64, payload []byte) Result {
	if index != s.applied.Load()+1 {
		return Result{Index: index, Err: fmt.Errorf("sm: apply index %d after %d", index, s.applied.Load())}
	}
	s.applied.Store(index)
	s.applying = index
	res := Result{Index: index}
	var c Command
	if err := proto.Unmarshal(payload, &c); err != nil {
		res.Err = fmt.Errorf("sm: undecodable command at index %d: %w", index, err)
		res.Revision = s.kv.Rev()
		return res
	}
	ctx := context.Background()
	switch {
	case c.Txn != nil:
		res.Resp, _, res.Err = txn.Txn(ctx, s.lg, c.Txn, true, s.kv, s.le, false)
	case c.Put != nil:
		res.Resp, _, res.Err = txn.Put(ctx, s.lg, s.le, s.kv, c.Put)
	case c.DeleteRange != nil:
		res.Resp, _, res.Err = txn.DeleteRange(ctx, s.lg, s.kv, c.DeleteRange)
	case c.Compaction != nil:
		_, res.Err = s.kv.Compact(traceutil.TODO(), c.Compaction.Revision)
		res.Resp = &pb.CompactionResponse{Header: &pb.ResponseHeader{Revision: s.kv.Rev()}}
	case c.LeaseGrant != nil:
		var l *lease.Lease
		l, res.Err = s.le.Grant(lease.LeaseID(c.LeaseGrant.ID), c.LeaseGrant.TTL)
		if res.Err == nil {
			res.Resp = &pb.LeaseGrantResponse{ID: int64(l.ID), TTL: l.TTL(), Header: &pb.ResponseHeader{Revision: s.kv.Rev()}}
		}
	case c.LeaseRevoke != nil:
		res.Err = s.le.Revoke(lease.LeaseID(c.LeaseRevoke.ID))
		res.Resp = &pb.LeaseRevokeResponse{Header: &pb.ResponseHeader{Revision: s.kv.Rev()}}
	default:
		res.Err = errors.New("sm: command carries no supported request")
	}
	res.Revision = s.kv.Rev()
	return res
}
