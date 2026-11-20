package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
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

	mu    sync.Mutex
	conns map[int]*grpc.ClientConn
}

func (f *forwarder) conn() (*grpc.ClientConn, error) {
	o := f.owner()
	if o < 0 || o >= len(f.peers) || o == f.self {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.conns[o]; c != nil {
		return c, nil
	}
	c, err := grpc.NewClient(f.peers[o], grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(rawCodec{})))
	if err != nil {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	f.conns[o] = c
	return c, nil
}

func (f *forwarder) handle(_ any, ss grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok {
		return rpctypes.ErrGRPCNoLeader
	}
	md, _ := metadata.FromIncomingContext(ss.Context())
	if len(md.Get(forwardedKey)) > 0 {
		// The sender thought this node was the owner. Refusing stops two
		// nodes with different views from forwarding to each other.
		return rpctypes.ErrGRPCNoLeader
	}
	conn, err := f.conn()
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
