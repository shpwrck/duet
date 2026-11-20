package main

import (
	"context"
	"errors"
	"log"
	"net"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"

	"github.com/shpwrck/duet/pkg/api"
	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
	"github.com/shpwrck/duet/pkg/wal"
)

// barrier answers every linearizable read that is waiting with one disk
// read. The store already holds every acknowledged write, because the loop
// queues a batch for the state machine before this answer.
func (s *server) barrier(first chan error) {
	waiting := []chan error{first}
gather:
	for {
		select {
		case b := <-s.barriers:
			waiting = append(waiting, b)
		default:
			break gather
		}
	}
	err := s.n.Confirm()
	// Answer behind everything already queued for the state machine, so
	// the reads that follow see every write acknowledged before them.
	s.applyQ <- applyTask{fn: func() {
		for _, b := range waiting {
			b <- err
		}
	}}
	s.publish()
}

func (s *server) ownerID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ownerNode
}

func (s *server) isOwner() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.owner
}

// etcdBackend adapts the server loop to api.Backend.
type etcdBackend struct{ s *server }

func (b etcdBackend) Store() *sm.Store { return b.s.st }

func (b etcdBackend) Epoch() uint64 {
	b.s.mu.RLock()
	defer b.s.mu.RUnlock()
	return b.s.epoch
}

func notOwner(err error) error {
	if errors.Is(err, node.ErrNotOwner) {
		return api.ErrNotOwner
	}
	return err
}

func (b etcdBackend) Propose(ctx context.Context, c *sm.Command) (sm.Result, error) {
	payload, err := sm.Encode(c)
	if err != nil {
		return sm.Result{}, err
	}
	if !wal.Fits(b.s.l, []int{len(payload)}) {
		return sm.Result{}, rpctypes.ErrGRPCRequestTooLarge
	}
	if !b.s.isOwner() {
		return sm.Result{}, api.ErrNotOwner
	}
	req := &appendReq{payload: payload, resp: make(chan appendResp, 1)}
	select {
	case b.s.reqs <- req:
	case <-ctx.Done():
		return sm.Result{}, ctx.Err()
	}
	// Once queued, wait for the outcome even if the caller gives up: the
	// write may be acknowledged, and the answer must not be invented.
	resp := <-req.resp
	return resp.result, notOwner(resp.err)
}

func (b etcdBackend) ReadBarrier(ctx context.Context) error {
	if !b.s.isOwner() {
		return api.ErrNotOwner
	}
	done := make(chan error, 1)
	select {
	case b.s.barriers <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return notOwner(<-done)
}

// startEtcd opens the etcd listener, which stays open for the life of the
// process whoever owns the log. Connections are served by one of two gRPC
// servers: the real one while this node owns the log, and otherwise a
// proxy that forwards every call to the owner.
//
// Followers must listen. An etcd client keeps to an endpoint while its
// connection is up, and backs off for longer and longer from an endpoint
// that refuses connections, so a node that only listened while it was the
// owner was reached late after every handover.
func (s *server) startEtcd(addr string, peers []string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.front = newFront(lis)
	s.fwd = &forwarder{peers: peers, self: s.id, owner: s.ownerID, wait: 2 * s.l.TTL, conns: map[int]*grpc.ClientConn{}}

	real := grpc.NewServer()
	b, member := etcdBackend{s}, uint64(s.id+1)
	kv := api.NewKV(b, 1, member)
	switch {
	case s.quota > 0:
		kv.Quota = s.quota
	case s.quota == 0 && s.l.CkptSize > 0:
		// Stop taking new data while the state still fits a checkpoint
		// slot with room to spare. See api.KV.Quota.
		kv.Quota = (s.l.CkptSize - disk.BlockSize) / 10 * 9
	}
	pb.RegisterKVServer(real, kv)
	watch := api.NewWatch(b, 1, member)
	watch.Progress = s.watchProgress
	pb.RegisterWatchServer(real, watch)
	pb.RegisterLeaseServer(real, api.NewLease(kv))
	pb.RegisterMaintenanceServer(real, api.NewMaintenance(kv))
	proxy := grpc.NewServer(grpc.ForceServerCodecV2(rawCodec{}), grpc.UnknownServiceHandler(s.fwd.handle))
	go real.Serve(s.front.listeners[modeOwner])
	go proxy.Serve(s.front.listeners[modeFollower])
	go s.front.accept()
	log.Printf("node %d: etcd API on %s", s.id, addr)
	return nil
}

// followOwnership switches the etcd listener between serving and
// forwarding, dropping the connections of the mode being left so that
// clients reconnect into the new one.
func (s *server) followOwnership(owner bool) {
	if s.front == nil {
		return
	}
	mode := modeFollower
	if owner {
		mode = modeOwner
	}
	if !s.front.set(mode) {
		return
	}
	if owner {
		// Leases count down only on the owner. A new owner gives every
		// lease one more disk-lease period, since keep-alives sent to the
		// old owner during the takeover were lost.
		s.st.Lessor().Promote(s.l.TTL)
	} else {
		s.st.Lessor().Demote()
	}
}

// expireLeases turns leases that have run out into revokes in the log, so
// every replica deletes the attached keys at the same revision. The lessor
// reports an expired lease again until it is revoked, so a failed attempt
// is retried.
func (s *server) expireLeases(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case leases := <-s.st.Lessor().ExpiredLeasesC():
			for _, l := range leases {
				etcdBackend{s}.Propose(ctx, &sm.Command{LeaseRevoke: &pb.LeaseRevokeRequest{ID: int64(l.ID)}})
			}
		}
	}
}
