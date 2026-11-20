package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	modeFollower = iota
	modeOwner
)

// front owns the one listening socket and hands each accepted connection
// to the gRPC server for the node's current mode.
type front struct {
	lis       net.Listener
	listeners [2]*chanListener

	mu    sync.Mutex
	mode  int
	conns map[*trackedConn]struct{}
}

func newFront(lis net.Listener) *front {
	f := &front{lis: lis, conns: map[*trackedConn]struct{}{}}
	for i := range f.listeners {
		f.listeners[i] = &chanListener{addr: lis.Addr(), ch: make(chan net.Conn), done: make(chan struct{})}
	}
	return f
}

func (f *front) accept() {
	for {
		c, err := f.lis.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		tc := &trackedConn{Conn: c, f: f, mode: f.mode}
		f.conns[tc] = struct{}{}
		f.mu.Unlock()
		select {
		case f.listeners[tc.mode].ch <- tc:
		case <-f.listeners[tc.mode].done:
			tc.Close()
		}
	}
}

// set changes the mode and closes every connection accepted in another
// mode. It reports whether the mode changed.
func (f *front) set(mode int) bool {
	f.mu.Lock()
	if f.mode == mode {
		f.mu.Unlock()
		return false
	}
	f.mode = mode
	var stale []*trackedConn
	for c := range f.conns {
		if c.mode != mode {
			stale = append(stale, c)
		}
	}
	f.mu.Unlock()
	for _, c := range stale {
		c.Close()
	}
	return true
}

type trackedConn struct {
	net.Conn
	f    *front
	mode int
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.f.mu.Lock()
		delete(c.f.conns, c)
		c.f.mu.Unlock()
	})
	return c.Conn.Close()
}

// chanListener is a net.Listener fed by front.accept.
type chanListener struct {
	addr net.Addr
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *chanListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *chanListener) Addr() net.Addr { return l.addr }

// rawCodec passes gRPC messages through as bytes, so the proxy forwards
// any method without knowing its types.
type rawCodec struct{}

type frame struct{ b []byte }

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	f, ok := v.(*frame)
	if !ok {
		return nil, errors.New("proxy: not a raw frame")
	}
	return mem.BufferSlice{mem.SliceBuffer(f.b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return errors.New("proxy: not a raw frame")
	}
	f.b = data.Materialize()
	return nil
}

const forwardedKey = "duet-forwarded"

// forwarder relays calls from a node that does not own the log to the node
// it believes does. It adds nothing to safety: the owner checks its own
// ownership on every write and every linearizable read, whoever sent them.
type forwarder struct {
	peers []string
	self  int
	owner func() int
	wait  time.Duration // how long a call waits for an owner to be known

	// unreachable is the owner's id plus one once a full wait has failed
	// to reach it, and zero otherwise.
	unreachable atomic.Int64

	mu    sync.Mutex
	conns map[int]*grpc.ClientConn
}

// errNoOwner says that nothing was sent because there is no connection to
// the owner. The wording is not free: it is the one Unavailable message
// for which the etcd client retries a write on its next endpoint, because
// it means the request never left. That is true here, and it is what lets
// a client that knows both nodes keep working when this node cannot reach
// the owner. (etcd's own "no leader" error would instead make the client
// give up on a watch for good.)
var errNoOwner = grpcstatus.Error(codes.Unavailable, "there is no connection available")

// conn returns a ready connection to the node believed to own the log.
// It waits a little for there to be one, instead of failing calls that
// arrive during a takeover or just after this node started: nothing has
// been sent yet, so waiting is safe, and to the client it looks like a
// slow call, which is what an etcd member does during an election.
//
// If the wait runs out with the owner known but unreachable, later calls
// fail at once until it is reachable again or the owner changes: the
// nodes cannot see each other, and the client should use the other one.
func (f *forwarder) conn(ctx context.Context) (*grpc.ClientConn, error) {
	for deadline := time.Now().Add(f.wait); ; {
		var c *grpc.ClientConn
		o := f.owner()
		if o >= 0 && o < len(f.peers) && o != f.self {
			c = f.dial(o)
		}
		if c != nil {
			if c.GetState() == connectivity.Ready {
				f.unreachable.Store(0)
				return c, nil
			}
			c.Connect()
			if f.unreachable.Load() == int64(o)+1 {
				return nil, errNoOwner
			}
		}
		if time.Now().After(deadline) {
			if c != nil {
				f.unreachable.Store(int64(o) + 1)
			}
			return nil, errNoOwner
		}
		select {
		case <-ctx.Done():
			return nil, grpcstatus.FromContextError(ctx.Err()).Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (f *forwarder) dial(o int) *grpc.ClientConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.conns[o]; c != nil {
		return c
	}
	// Reconnect quickly: a peer that was down is expected back.
	c, err := grpc.NewClient(f.peers[o], grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(rawCodec{})),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.5, MaxDelay: 500 * time.Millisecond}, MinConnectTimeout: time.Second}))
	if err != nil {
		return nil
	}
	f.conns[o] = c
	return c
}

func (f *forwarder) handle(_ any, ss grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok {
		return errNoOwner
	}
	md, _ := metadata.FromIncomingContext(ss.Context())
	if len(md.Get(forwardedKey)) > 0 {
		// The sender thought this node was the owner. Refusing stops two
		// nodes with different views from forwarding to each other.
		return errNoOwner
	}
	conn, err := f.conn(ss.Context())
	if err != nil {
		return err
	}
	out := md.Copy()
	out.Set(forwardedKey, "1")
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(ss.Context(), out))
	defer cancel()
	cs, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method)
	if err != nil {
		return err
	}
	go func() { // client to owner
		for {
			var m frame
			if err := ss.RecvMsg(&m); err != nil {
				if err == io.EOF {
					cs.CloseSend()
				} else {
					cancel()
				}
				return
			}
			if err := cs.SendMsg(&m); err != nil {
				return
			}
		}
	}()
	for { // owner to client
		var m frame
		if err := cs.RecvMsg(&m); err != nil {
			ss.SetTrailer(cs.Trailer())
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := ss.SendMsg(&m); err != nil {
			return err
		}
	}
}
