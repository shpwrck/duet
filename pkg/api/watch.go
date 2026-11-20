package api

import (
	"context"
	"io"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
)

// Watch implements etcdserverpb.WatchServer on etcd's own watch machinery.
//
// A store applies a write only after it is acknowledged, so a watcher
// never sees an event that could later be lost.
type Watch struct {
	pb.UnimplementedWatchServer
	b                   Backend
	clusterID, memberID uint64
	// Progress is how often watchers that asked for progress
	// notifications get one while idle.
	Progress time.Duration
}

func NewWatch(b Backend, clusterID, memberID uint64) *Watch {
	return &Watch{b: b, clusterID: clusterID, memberID: memberID, Progress: 10 * time.Minute}
}

type watchStream struct {
	srv *Watch
	gs  pb.Watch_WatchServer
	ws  mvcc.WatchStream

	// mu serialises sends and guards the maps. Holding it across "register
	// the watcher, send created" keeps events from overtaking the reply
	// that announces their watcher.
	mu       sync.Mutex
	prevKV   map[mvcc.WatchID]bool
	progress map[mvcc.WatchID]bool
}

func (s *Watch) header(rev int64) *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: s.clusterID, MemberId: s.memberID, Revision: rev, RaftTerm: s.b.Epoch()}
}

func (s *Watch) Watch(gs pb.Watch_WatchServer) error {
	w := &watchStream{srv: s, gs: gs, ws: s.b.Store().KV().NewWatchStream(),
		prevKV: map[mvcc.WatchID]bool{}, progress: map[mvcc.WatchID]bool{}}
	defer w.ws.Close()
	ctx, cancel := context.WithCancel(gs.Context())
	defer cancel()
	errc := make(chan error, 2)
	go func() { errc <- w.recvLoop(ctx) }()
	go func() { errc <- w.sendLoop(ctx) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *watchStream) recvLoop(ctx context.Context) error {
	for {
		req, err := w.gs.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch r := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			if err := w.create(ctx, r.CreateRequest); err != nil {
				return err
			}
		case *pb.WatchRequest_CancelRequest:
			id := mvcc.WatchID(r.CancelRequest.WatchId)
			w.mu.Lock()
			err := w.ws.Cancel(id)
			if err == nil {
				delete(w.prevKV, id)
				delete(w.progress, id)
				err = w.gs.Send(&pb.WatchResponse{Header: w.srv.header(w.ws.Rev()), WatchId: int64(id), Canceled: true})
			} else {
				err = nil // unknown id: etcd ignores it too
			}
			w.mu.Unlock()
			if err != nil {
				return err
			}
		case *pb.WatchRequest_ProgressRequest:
			// The reply comes through the send loop, behind every event
			// already queued, which is what makes it a statement that the
			// stream is caught up to its revision.
			w.ws.RequestProgressAll()
		}
	}
}

func (w *watchStream) create(ctx context.Context, c *pb.WatchCreateRequest) error {
	if c == nil {
		return nil
	}
	end := c.RangeEnd
	switch {
	case len(end) == 0:
		end = nil // a single key
	case len(end) == 1 && end[0] == 0:
		end = []byte{} // every key from Key onwards
	}
	var filters []mvcc.FilterFunc
	for _, f := range c.Filters {
		switch f {
		case pb.WatchCreateRequest_NOPUT:
			filters = append(filters, func(e *mvccpb.Event) bool { return e.Type == mvccpb.Event_PUT })
		case pb.WatchCreateRequest_NODELETE:
			filters = append(filters, func(e *mvccpb.Event) bool { return e.Type == mvccpb.Event_DELETE })
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	id, err := w.ws.Watch(ctx, mvcc.WatchID(c.WatchId), c.Key, end, c.StartRevision, filters...)
	if err != nil {
		return w.gs.Send(&pb.WatchResponse{Header: w.srv.header(w.ws.Rev()), WatchId: c.WatchId,
			Created: true, Canceled: true, CancelReason: err.Error()})
	}
	if c.PrevKv {
		w.prevKV[id] = true
	}
	if c.ProgressNotify {
		w.progress[id] = true
	}
	return w.gs.Send(&pb.WatchResponse{Header: w.srv.header(w.ws.Rev()), WatchId: int64(id), Created: true})
}

func (w *watchStream) sendLoop(ctx context.Context) error {
	tick := time.NewTicker(w.srv.Progress)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			w.mu.Lock()
			for id := range w.progress {
				w.ws.RequestProgress(id)
			}
			w.mu.Unlock()
		case r, ok := <-w.ws.Chan():
			if !ok {
				return nil
			}
			if err := w.send(ctx, r); err != nil {
				return err
			}
		}
	}
}

func (w *watchStream) send(ctx context.Context, r mvcc.WatchResponse) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := &pb.WatchResponse{Header: w.srv.header(r.Revision), WatchId: int64(r.WatchID)}
	if r.CompactRevision != 0 {
		// The watcher asked for history that has been compacted away.
		out.CompactRevision, out.Canceled = r.CompactRevision, true
		w.ws.Cancel(r.WatchID)
		delete(w.prevKV, r.WatchID)
		delete(w.progress, r.WatchID)
		return w.gs.Send(out)
	}
	out.Events = r.Events
	if w.prevKV[r.WatchID] {
		kv := w.srv.b.Store().KV()
		// Events are shared with other streams watching the same keys, so
		// attach previous values to copies.
		out.Events = make([]*mvccpb.Event, len(r.Events))
		for i, e := range r.Events {
			out.Events[i] = e
			if e.Kv == nil || (e.Type == mvccpb.Event_PUT && e.Kv.CreateRevision == e.Kv.ModRevision) {
				continue // a create has no previous value
			}
			// A compacted previous revision leaves PrevKv empty, as in etcd.
			if rr, err := kv.Range(ctx, e.Kv.Key, nil, mvcc.RangeOptions{Rev: e.Kv.ModRevision - 1}); err == nil && len(rr.KVs) == 1 {
				out.Events[i] = &mvccpb.Event{Type: e.Type, Kv: e.Kv, PrevKv: rr.KVs[0]}
			}
		}
	}
	return w.gs.Send(out)
}
