package sm_test

import (
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
)

// casPut is the only write shape kube-apiserver uses: put if the key's
// mod_revision is what the caller last saw (0 means "create").
func casPut(key, value string, mod int64) *sm.Command {
	return &sm.Command{Txn: &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte(key), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL,
			TargetUnion: &pb.Compare_ModRevision{ModRevision: mod}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte(value)}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(key)}}}},
	}}
}

func open(t *testing.T) *sm.Store {
	t.Helper()
	s, err := sm.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustEncode(t *testing.T, c *sm.Command) []byte {
	t.Helper()
	b, err := sm.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Two stores fed the same commands agree on every result and end with the
// same revision and hash. A failed compare is a result, not an error.
func TestSameLogSameState(t *testing.T) {
	a, b := open(t), open(t)
	cmds := []*sm.Command{
		casPut("/registry/pods/x", "v1", 0),
		casPut("/registry/pods/x", "dup", 0), // already exists
		casPut("/registry/pods/x", "v2", 2),
		casPut("/registry/pods/y", "v1", 0),
		{DeleteRange: &pb.DeleteRangeRequest{Key: []byte("/registry/pods/y")}},
		{LeaseGrant: &pb.LeaseGrantRequest{ID: 7, TTL: 60}},
		{Put: &pb.PutRequest{Key: []byte("/registry/events/e"), Value: []byte("ev"), Lease: 7}},
		{LeaseRevoke: &pb.LeaseRevokeRequest{ID: 7}}, // deletes the event
		{Compaction: &pb.CompactionRequest{Revision: 3}},
	}
	wantOK := []bool{true, false, true, true}
	for i, c := range cmds {
		p := mustEncode(t, c)
		ra, rb := a.Apply(uint64(i+1), p), b.Apply(uint64(i+1), p)
		if (ra.Err == nil) != (rb.Err == nil) || ra.Revision != rb.Revision {
			t.Fatalf("command %d diverged: %v rev %d vs %v rev %d", i, ra.Err, ra.Revision, rb.Err, rb.Revision)
		}
		if ra.Err != nil {
			t.Fatalf("command %d: %v", i, ra.Err)
		}
		if i < len(wantOK) {
			if got := ra.Resp.(*pb.TxnResponse).Succeeded; got != wantOK[i] {
				t.Fatalf("command %d succeeded=%v, want %v", i, got, wantOK[i])
			}
		}
	}
	ha, ra, err := a.Hash()
	if err != nil {
		t.Fatal(err)
	}
	hb, rb, _ := b.Hash()
	if ha != hb || ra != rb {
		t.Fatalf("stores differ: hash %d rev %d vs hash %d rev %d", ha, ra, hb, rb)
	}
	if a.Revision() != 7 {
		t.Fatalf("revision = %d, want 7", a.Revision())
	}
}

type clock struct{ now *time.Duration }

func (c clock) Now() time.Duration { return *c.now }

// The whole path: commands go through one node's log, a second node tails
// them into its own store, ownership changes hands, and both stores stay
// identical.
func TestStoresFollowTheLogAcrossTakeover(t *testing.T) {
	lay := disk.Layout{Nodes: 2, EpochSlots: 16, LogSize: 4 << 20, TTL: 10 * time.Second, MaxBatch: 4 * disk.BlockSize}
	dev := disk.NewMemDevice(lay.Total())
	if err := disk.Format(dev, lay); err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	stores := []*sm.Store{open(t), open(t)}
	nodes := make([]*node.Node, 2)
	for i := range nodes {
		st := stores[i]
		n, err := node.New(dev, clock{&now}, lease.Config{Node: i, Incarnation: uint64(i + 1), Drift: 0.1},
			func(index uint64, payload []byte) {
				if r := st.Apply(index, payload); r.Err != nil {
					t.Errorf("node apply at %d: %v", index, r.Err)
				}
			})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	write := func(n *node.Node, c *sm.Command) {
		t.Helper()
		if _, err := n.Append([][]byte{mustEncode(t, c)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := nodes[0].Campaign(); err != nil {
		t.Fatal(err)
	}
	write(nodes[0], casPut("/registry/pods/x", "v1", 0))
	write(nodes[0], casPut("/registry/pods/x", "v2", 2))
	if err := nodes[1].Tail(); err != nil {
		t.Fatal(err)
	}
	if stores[1].Revision() != 3 {
		t.Fatalf("follower revision = %d, want 3", stores[1].Revision())
	}

	// Planned handover, then the new owner writes.
	if err := nodes[0].Release(); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	write(nodes[1], casPut("/registry/pods/x", "v3", 3))
	write(nodes[1], casPut("/registry/pods/y", "v1", 0))
	if err := nodes[0].Tail(); err != nil {
		t.Fatal(err)
	}

	h0, r0, _ := stores[0].Hash()
	h1, r1, _ := stores[1].Hash()
	if h0 != h1 || r0 != r1 || r0 != 5 {
		t.Fatalf("stores differ after takeover: hash %d rev %d vs hash %d rev %d", h0, r0, h1, r1)
	}
}
