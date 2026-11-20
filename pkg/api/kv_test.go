package api_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/shpwrck/duet/pkg/api"
	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
)

type wall struct{ start time.Time }

func (w wall) Now() time.Duration { return time.Since(w.start) }

// backend is the simplest correct Backend: one lock around a real node on
// an in-memory disk.
type backend struct {
	mu   sync.Mutex
	n    *node.Node
	st   *sm.Store
	last sm.Result
}

func (b *backend) Propose(_ context.Context, c *sm.Command) (sm.Result, error) {
	p, err := sm.Encode(c)
	if err != nil {
		return sm.Result{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.n.IsOwner() {
		return sm.Result{}, api.ErrNotOwner
	}
	if _, err := b.n.Append([][]byte{p}); err != nil {
		return sm.Result{}, err
	}
	return b.last, nil
}

func (b *backend) ReadBarrier(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.n.IsOwner() {
		return api.ErrNotOwner
	}
	return b.n.Renew()
}

func (b *backend) Store() *sm.Store { return b.st }
func (b *backend) Epoch() uint64    { return b.n.Epoch() }

func start(t *testing.T, dev disk.Device, id int) (*backend, *clientv3.Client) {
	t.Helper()
	st, err := sm.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &backend{st: st}
	b.n, err = node.New(dev, wall{time.Now()}, lease.Config{Node: id, Incarnation: uint64(id + 1), Drift: 0.1},
		func(index uint64, payload []byte) { b.last = st.Apply(index, payload) })
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	kv := api.NewKV(b, 1, uint64(id+1))
	pb.RegisterKVServer(srv, kv)
	pb.RegisterWatchServer(srv, api.NewWatch(b, 1, uint64(id+1)))
	pb.RegisterLeaseServer(srv, api.NewLease(kv))
	pb.RegisterMaintenanceServer(srv, api.NewMaintenance(kv))
	go srv.Serve(lis)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close(); srv.Stop(); st.Close() })
	return b, cli
}

// The stock etcd client, doing what kube-apiserver does: create, guarded
// update, a lost race, list, delete, compact.
func TestEtcdClientAgainstDuet(t *testing.T) {
	lay := disk.Layout{Nodes: 2, EpochSlots: 16, LogSize: 4 << 20, TTL: 10 * time.Second, MaxBatch: 4 * disk.BlockSize}
	dev := disk.NewMemDevice(lay.Total())
	if err := disk.Format(dev, lay); err != nil {
		t.Fatal(err)
	}
	b0, c0 := start(t, dev, 0)
	_, c1 := start(t, dev, 1)
	if err := b0.n.Campaign(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const key = "/registry/pods/default/x"

	create, err := c0.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v1")).Else(clientv3.OpGet(key)).Commit()
	if err != nil || !create.Succeeded || create.Header.Revision != 2 {
		t.Fatalf("create: %v %+v", err, create)
	}
	stale, err := c0.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "dup")).Else(clientv3.OpGet(key)).Commit()
	if err != nil || stale.Succeeded {
		t.Fatalf("second create should lose: %v %+v", err, stale)
	}
	if kvs := stale.Responses[0].GetResponseRange().Kvs; len(kvs) != 1 || string(kvs[0].Value) != "v1" {
		t.Fatalf("losing create did not return the current value: %+v", kvs)
	}
	upd, err := c0.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", 2)).
		Then(clientv3.OpPut(key, "v2")).Commit()
	if err != nil || !upd.Succeeded || upd.Header.Revision != 3 {
		t.Fatalf("update: %v %+v", err, upd)
	}
	if _, err := c0.Put(ctx, "/registry/pods/default/y", "v1"); err != nil {
		t.Fatal(err)
	}
	list, err := c0.Get(ctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil || len(list.Kvs) != 1 || !list.More || list.Count != 2 || list.Header.Revision != 4 {
		t.Fatalf("list: %v %+v", err, list)
	}
	old, err := c0.Get(ctx, key, clientv3.WithRev(2))
	if err != nil || string(old.Kvs[0].Value) != "v1" {
		t.Fatalf("read at old revision: %v %+v", err, old)
	}
	del, err := c0.Delete(ctx, "/registry/pods/default/y")
	if err != nil || del.Deleted != 1 {
		t.Fatalf("delete: %v %+v", err, del)
	}
	if _, err := c0.Compact(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c0.Get(ctx, key, clientv3.WithRev(2)); err != rpctypes.ErrCompacted {
		t.Fatalf("read below compaction: %v, want ErrCompacted", err)
	}

	// The node that does not own the log refuses writes and linearizable
	// reads instead of answering from possibly stale state.
	short, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	if _, err := c1.Put(short, "/k", "v"); err == nil {
		t.Fatal("non-owner accepted a write")
	}
	if _, err := c1.Get(short, key); err == nil {
		t.Fatal("non-owner answered a linearizable read")
	}
}

func oneNode(t *testing.T) (*backend, *clientv3.Client, context.Context) {
	t.Helper()
	lay := disk.Layout{Nodes: 2, EpochSlots: 16, LogSize: 4 << 20, TTL: 10 * time.Second, MaxBatch: 4 * disk.BlockSize}
	dev := disk.NewMemDevice(lay.Total())
	if err := disk.Format(dev, lay); err != nil {
		t.Fatal(err)
	}
	b, c := start(t, dev, 0)
	if err := b.n.Campaign(); err != nil {
		t.Fatal(err)
	}
	b.st.Lessor().Promote(0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return b, c, ctx
}

func next(t *testing.T, ch clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case r, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed")
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no watch response within 5s")
	}
	return clientv3.WatchResponse{}
}

// A watch the way kube-apiserver opens one: a prefix, from a past
// revision, with previous values, then a progress request.
func TestWatch(t *testing.T) {
	_, c, ctx := oneNode(t)
	c.Put(ctx, "/registry/pods/a", "1") // rev 2
	c.Put(ctx, "/registry/pods/a", "2") // rev 3
	c.Put(ctx, "/registry/svc/x", "1")  // rev 4, outside the prefix

	wctx := clientv3.WithRequireLeader(ctx)
	ch := c.Watch(wctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithRev(3), clientv3.WithPrevKV())
	r := next(t, ch)
	if len(r.Events) != 1 || string(r.Events[0].Kv.Value) != "2" || r.Events[0].PrevKv == nil || string(r.Events[0].PrevKv.Value) != "1" {
		t.Fatalf("replay from revision 3: %+v", r.Events)
	}
	if _, err := c.Delete(ctx, "/registry/pods/a"); err != nil { // rev 5
		t.Fatal(err)
	}
	r = next(t, ch)
	if len(r.Events) != 1 || r.Events[0].Type != clientv3.EventTypeDelete || r.Events[0].Kv.ModRevision != 5 || string(r.Events[0].PrevKv.Value) != "2" {
		t.Fatalf("delete event: %+v", r.Events)
	}
	if err := c.RequestProgress(wctx); err != nil {
		t.Fatal(err)
	}
	if r = next(t, ch); !r.IsProgressNotify() || r.Header.Revision != 5 {
		t.Fatalf("progress: %+v", r)
	}

	// Asking for history that has been compacted cancels the watch and
	// says where history now starts.
	if _, err := c.Compact(ctx, 4); err != nil {
		t.Fatal(err)
	}
	r = next(t, c.Watch(ctx, "/registry/pods/", clientv3.WithPrefix(), clientv3.WithRev(2)))
	if r.CompactRevision != 4 || r.Err() != rpctypes.ErrCompacted {
		t.Fatalf("compacted watch: %+v err %v", r, r.Err())
	}
}

// Leases the way kube-apiserver uses them for events: grant, attach,
// expire. Plus keep-alive, revoke and status.
func TestLeaseAndStatus(t *testing.T) {
	b, c, ctx := oneNode(t)
	// The owner turns expired leases into revokes in the log.
	go func() {
		for ls := range b.st.Lessor().ExpiredLeasesC() {
			for _, l := range ls {
				b.Propose(ctx, &sm.Command{LeaseRevoke: &pb.LeaseRevokeRequest{ID: int64(l.ID)}})
			}
		}
	}()
	short, err := c.Grant(ctx, 1)
	if err != nil || short.ID == 0 {
		t.Fatalf("grant: %v %+v", err, short)
	}
	long, err := c.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	c.Put(ctx, "/registry/events/short", "e", clientv3.WithLease(short.ID))
	c.Put(ctx, "/registry/events/long", "e", clientv3.WithLease(long.ID))
	if ka, err := c.KeepAliveOnce(ctx, long.ID); err != nil || ka.TTL != 60 {
		t.Fatalf("keep-alive: %v %+v", err, ka)
	}
	ch := c.Watch(ctx, "/registry/events/short")
	if r := next(t, ch); len(r.Events) != 1 || r.Events[0].Type != clientv3.EventTypeDelete {
		t.Fatalf("expected the expired lease to delete its key: %+v", r.Events)
	}
	if _, err := c.Revoke(ctx, long.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.Get(ctx, "/registry/events/", clientv3.WithPrefix()); g.Count != 0 {
		t.Fatalf("%d keys left after their leases ended", g.Count)
	}
	if _, err := c.Put(ctx, "/k", "v", clientv3.WithLease(long.ID)); err != rpctypes.ErrLeaseNotFound {
		t.Fatalf("put with revoked lease: %v", err)
	}
	st, err := c.Status(ctx, c.Endpoints()[0])
	if err != nil || st.Version != sm.EtcdVersion || st.DbSize == 0 {
		t.Fatalf("status: %v %+v", err, st)
	}
}
