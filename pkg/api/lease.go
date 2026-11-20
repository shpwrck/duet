package api

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/lease"

	"github.com/shpwrck/duet/pkg/sm"
)

// Lease implements Grant, Revoke and KeepAlive. As in etcd, grants and
// revokes go through the log and keep-alives are held in memory by the
// node that owns the log; a new owner extends every lease on taking over.
type Lease struct {
	pb.UnimplementedLeaseServer
	kv *KV
}

func NewLease(kv *KV) *Lease { return &Lease{kv: kv} }

func (s *Lease) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	if r.ID == 0 {
		// Chosen here, before the log, so that every replica grants the
		// same ID.
		var b [8]byte
		for r.ID <= 0 {
			if _, err := rand.Read(b[:]); err != nil {
				return nil, grpcErr(err)
			}
			r.ID = int64(binary.BigEndian.Uint64(b[:]) &^ (1 << 63))
		}
	}
	res, err := s.kv.propose(ctx, &sm.Command{LeaseGrant: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.LeaseGrantResponse)
	resp.Header = s.kv.header(resp.Header)
	return resp, nil
}

func (s *Lease) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	res, err := s.kv.propose(ctx, &sm.Command{LeaseRevoke: r})
	if err != nil {
		return nil, err
	}
	resp := res.Resp.(*pb.LeaseRevokeResponse)
	resp.Header = s.kv.header(resp.Header)
	return resp, nil
}

func (s *Lease) LeaseKeepAlive(gs pb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := gs.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp := &pb.LeaseKeepAliveResponse{ID: req.ID, Header: s.kv.header(nil)}
		ttl, err := s.kv.b.Store().Lessor().Renew(lease.LeaseID(req.ID))
		switch err {
		case nil:
			resp.TTL = ttl
		case lease.ErrLeaseNotFound:
			// TTL 0 tells the client the lease is gone.
		case lease.ErrNotPrimary:
			return rpctypes.ErrGRPCNoLeader
		default:
			return grpcErr(err)
		}
		if err := gs.Send(resp); err != nil {
			return err
		}
	}
}

// Maintenance implements Status, which clients use to find the server
// version and database size.
type Maintenance struct {
	pb.UnimplementedMaintenanceServer
	kv *KV
}

func NewMaintenance(kv *KV) *Maintenance { return &Maintenance{kv: kv} }

func (s *Maintenance) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := s.kv.b.Store()
	size := st.DbSize()
	return &pb.StatusResponse{
		Header:      s.kv.header(nil),
		Version:     sm.EtcdVersion,
		DbSize:      size,
		DbSizeInUse: size,
		Leader:      s.kv.memberID, // only the owner serves
		RaftIndex:   st.Applied(),
		RaftTerm:    s.kv.b.Epoch(),
	}, nil
}
