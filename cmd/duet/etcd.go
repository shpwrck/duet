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
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
	"github.com/shpwrck/duet/pkg/wal"
)

// barrier answers every linearizable read that is waiting with one disk
// read. The store already holds every acknowledged write, because the loop
// applies a batch before it acknowledges it.
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
	for _, b := range waiting {
		b <- err
	}
	s.publish()
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

// followOwnership opens the etcd listener when this node becomes the owner
// and closes it, dropping every connection, when it stops being the owner.
//
// The stock etcd client stays on an endpoint for as long as its connection
// is up, whatever errors that endpoint returns, because every etcd member
// can serve or forward any request. duet has no forwarding yet, so the
// owner is the only node that listens. The cost: followers serve nothing,
// not even reads that tolerate staleness.
func (s *server) followOwnership(owner bool) {
	if s.etcdAddr == "" {
		return
	}
	if !owner {
		if s.etcdSrv != nil {
			go s.etcdSrv.Stop()
			s.etcdSrv = nil
		}
		return
	}
	if s.etcdSrv != nil {
		return
	}
	lis, err := net.Listen("tcp", s.etcdAddr)
	if err != nil {
		log.Printf("node %d: etcd API: %v", s.id, err)
		return
	}
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, api.NewKV(etcdBackend{s}, 1, uint64(s.id+1)))
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("node %d: etcd API: %v", s.id, err)
		}
	}()
	s.etcdSrv = srv
}
