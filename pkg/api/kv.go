// Package api serves the etcd v3 gRPC API from a duet node.
//
// It covers what kube-apiserver uses: KV (Range, RangeStream, Put, DeleteRange, Txn,
// Compact), Watch, Lease (Grant, Revoke, KeepAlive) and Maintenance
// Status. Auth, Cluster and the rest of Maintenance are not implemented.
package api

import (
	"context"
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/txn"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/shpwrck/duet/pkg/sm"
)

// ErrNotOwner is returned by a Backend that does not own the log.
var ErrNotOwner = errors.New("api: this node does not own the log")

// Backend is what the API needs from a node.
type Backend interface {
	// Propose writes a command to the log and returns the result of
	// applying it. A nil error means the write is acknowledged; Result.Err
	// is then the command's own outcome. Any other error means the write
	// may or may not have happened.
	Propose(ctx context.Context, c *sm.Command) (sm.Result, error)
	// ReadBarrier returns nil once the store reflects every write
	// acknowledged before the call, and this node still owned the log at
	// some point after the call began.
	ReadBarrier(ctx context.Context) error
	Store() *sm.Store
	// Epoch is reported to clients where etcd reports the raft term.
	Epoch() uint64
}

// KV implements etcdserverpb.KVServer.
type KV struct {
	pb.UnimplementedKVServer
	b         Backend
	lg        *zap.Logger
	clusterID uint64
	memberID  uint64

	// Quota, when positive, is the size of the state file past which
	// writes that add data are refused with etcd's "database space
	// exceeded". Deletes and compactions are still accepted. It keeps
	// the state small enough to checkpoint; a state that cannot be
	// checkpointed means the log can never be reused.
	Quota int64
}

func (s *KV) full() bool { return s.Quota > 0 && s.b.Store().DbSize() >= s.Quota }

func txnPuts(r *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			if op.GetRequestPut() != nil {
				return true
			}
			if t := op.GetRequestTxn(); t != nil && txnPuts(t) {
				return true
			}
		}
	}
	return false
}

func NewKV(b Backend, clusterID, memberID uint64) *KV {
	return &KV{b: b, lg: zap.NewNop(), clusterID: clusterID, memberID: memberID}
}

func (s *KV) header(h *pb.ResponseHeader) *pb.ResponseHeader {
	if h == nil {
		h = &pb.ResponseHeader{}
	}
	if h.Revision == 0 {
		h.Revision = s.b.Store().Revision()
	}
	h.ClusterId, h.MemberId, h.RaftTerm = s.clusterID, s.memberID, s.b.Epoch()
	return h
}

// grpcErr maps an error to what an etcd client expects to see.
func grpcErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotOwner):
		// Unavailable makes a client with several endpoints try another.
		return rpctypes.ErrGRPCNoLeader
	case errors.Is(err, mvcc.ErrCompacted):
		return rpctypes.ErrGRPCCompacted
	case errors.Is(err, mvcc.ErrFutureRev):
		return rpctypes.ErrGRPCFutureRev
	case errors.Is(err, lease.ErrLeaseNotFound):
		return rpctypes.ErrGRPCLeaseNotFound
	case errors.Is(err, lease.ErrLeaseExists):
		return rpctypes.ErrGRPCLeaseExist
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unavailable, err.Error())
}

func (s *KV) propose(ctx context.Context, c *sm.Command) (sm.Result, error) {
	r, err := s.b.Propose(ctx, c)
	if err == nil {
		err = r.Err
	}
	return r, grpcErr(err)
}

func (s *KV) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if len(r.Key) == 0 {
		return nil, rpctypes.ErrGRPCEmptyKey
	}
	if !r.Serializable {
		if err := s.b.ReadBarrier(ctx); err != nil {
			return nil, grpcErr(err)
		}
	}
	resp, _, err := txn.Range(ctx, s.lg, s.b.Store().KV(), r, true)
	if err != nil {
		return nil, grpcErr(err)
	}
	resp.Header = s.header(resp.Header)
	return resp, nil
}

// rangeChunk is how many keys one RangeStream message carries.
const rangeChunk = 256

// RangeStream answers a range in several messages, so that no single gRPC
// message has to hold a large list. All messages come from one read at one
// revision. The whole result is still assembled in memory first; only the
// transport is chunked.
func (s *KV) RangeStream(r *pb.RangeRequest, gs grpc.ServerStreamingServer[pb.RangeStreamResponse]) error {
	resp, err := s.Range(gs.Context(), r)
	if err != nil {
		return err
	}
	kvs := resp.Kvs
	for len(kvs) > rangeChunk {
		if err := gs.Send(&pb.RangeStreamResponse{RangeResponse: &pb.RangeResponse{Kvs: kvs[:rangeChunk]}}); err != nil {
			return err
		}
		kvs = kvs[rangeChunk:]
	}
	// Header, More and Count travel on the last message only.
	resp.Kvs = kvs
	return gs.Send(&pb.RangeStreamResponse{RangeResponse: resp})
}

func (s *KV) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if len(r.Key) == 0 {
		return nil, rpctypes.ErrGRPCEmptyKey
	}
	if s.full() {
		return nil, rpctypes.ErrGRPCNoSpace
	}
	res, err := s.propose(ctx, &sm.Command{Put: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.PutResponse)
	resp.Header = s.header(resp.Header)
	return resp, nil
}

func (s *KV) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if len(r.Key) == 0 {
		return nil, rpctypes.ErrGRPCEmptyKey
	}
	res, err := s.propose(ctx, &sm.Command{DeleteRange: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.DeleteRangeResponse)
	resp.Header = s.header(resp.Header)
	return resp, nil
}

// Txn sends every transaction through the log, read-only ones included.
// That is correct but slower than it needs to be for read-only
// transactions; kube-apiserver does not issue those.
func (s *KV) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if s.full() && txnPuts(r) {
		return nil, rpctypes.ErrGRPCNoSpace
	}
	res, err := s.propose(ctx, &sm.Command{Txn: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.TxnResponse)
	resp.Header = s.header(resp.Header)
	return resp, nil
}

func (s *KV) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	res, err := s.propose(ctx, &sm.Command{Compaction: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.CompactionResponse)
	resp.Header = s.header(resp.Header)
	return resp, nil
}
