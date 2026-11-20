// Package sm is the state machine: etcd's own storage, transaction and
// lease code, driven by duet's log instead of by raft.
//
// Reusing etcd's packages means range, revision, transaction, watch and
// compaction behaviour are etcd's by construction, not a reimplementation.
// The cost is a dependency on go.etcd.io/etcd/server/v3, which makes no
// API stability promise to importers, so it is pinned and kept behind this
// package.
//
// A Store starts empty and is rebuilt by replaying the log. Checkpoints,
// so that a restart does not replay everything, are not implemented yet.
package sm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/etcdserver/txn"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
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
	applied uint64
}

// Open creates an empty store in a new directory under dir. An empty dir
// uses the system temporary directory; put it on tmpfs to keep the state
// in memory.
func Open(dir string) (*Store, error) {
	d, err := os.MkdirTemp(dir, "duet-sm-")
	if err != nil {
		return nil, err
	}
	lg := zap.NewNop()
	be := backend.NewDefaultBackend(lg, filepath.Join(d, "db"))
	le := lease.NewLessor(lg, be, nil, lease.LessorConfig{MinLeaseTTL: 1})
	kv := mvcc.New(lg, be, le, mvcc.StoreConfig{})
	// Revoking a lease deletes the keys attached to it.
	le.SetRangeDeleter(func() lease.TxnDelete { return kv.Write(traceutil.TODO()) })
	return &Store{lg: lg, dir: d, be: be, le: le, kv: kv}, nil
}

// Close releases the store and deletes its files.
func (s *Store) Close() error {
	s.le.Stop()
	err := s.kv.Close()
	if e := s.be.Close(); err == nil {
		err = e
	}
	os.RemoveAll(s.dir)
	return err
}

// KV exposes the store for reads and watches.
func (s *Store) KV() mvcc.WatchableKV { return s.kv }

// Lessor exposes the lease manager.
func (s *Store) Lessor() lease.Lessor { return s.le }

// Applied is the index of the last command applied.
func (s *Store) Applied() uint64 { return s.applied }

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
	if index != s.applied+1 {
		return Result{Index: index, Err: fmt.Errorf("sm: apply index %d after %d", index, s.applied)}
	}
	s.applied = index
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
		res.Resp, _, res.Err = txn.Txn(ctx, s.lg, c.Txn, false, s.kv, s.le, false)
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
