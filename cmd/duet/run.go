package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/lease"
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
	"github.com/shpwrck/duet/pkg/wal"
)

type realClock struct{ start time.Time }

func (c realClock) Now() time.Duration { return time.Since(c.start) }

func cmdFormat(args []string) error {
	fs := flag.NewFlagSet("format", flag.ExitOnError)
	device := fs.String("device", "", "file or block device to format (destroys its contents)")
	nodes := fs.Int("nodes", 2, "how many nodes may share the disk")
	slots := fs.Int("epoch-slots", 4096, "how many ownership changes the disk can record")
	logSize := fs.Int64("log-size", 0, "bytes for the log; 0 uses the rest of the device")
	ttl := fs.Duration("ttl", 5*time.Second, "lease length")
	maxBatch := fs.Int("max-batch", 1<<20, "largest single log write in bytes")
	direct := fs.Bool("direct", true, "use direct I/O")
	fs.Parse(args)
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	l := disk.Layout{Nodes: *nodes, EpochSlots: *slots, LogSize: *logSize, TTL: *ttl, MaxBatch: *maxBatch}
	if l.LogSize == 0 {
		l.LogSize = dev.Size() - l.LogStart()
	}
	if err := disk.Format(dev, l); err != nil {
		return err
	}
	fmt.Printf("formatted %s: %d nodes, %d epoch slots, %d MiB log, lease %s, max batch %d KiB\n",
		*device, l.Nodes, l.EpochSlots, l.LogSize>>20, l.TTL, l.MaxBatch>>10)
	return nil
}

type appendReq struct {
	payload []byte
	resp    chan appendResp
}

type appendResp struct {
	index  uint64
	err    error
	result sm.Result // set only when the node runs the etcd state machine
}

type status struct {
	Node    int    `json:"node"`
	Owner   bool   `json:"owner"`
	Epoch   uint64 `json:"epoch"`
	Applied uint64 `json:"applied"`
	Chain   string `json:"chain"`
}

type server struct {
	id      int
	n       *node.Node
	l       disk.Layout
	reqs    chan *appendReq
	carry   *appendReq // a request that did not fit in the previous batch
	backoff time.Time  // do not campaign again before this

	mu    sync.RWMutex
	sums  [][32]byte // sha256 of each applied payload, by index-1
	chain [32]byte   // hash chain over sums; equal chains mean equal logs
	owner bool
	epoch uint64

	// etcd mode only.
	st       *sm.Store
	etcdAddr string
	etcdSrv  *grpc.Server // non-nil only while this node is the owner
	barriers chan chan error
	collect  bool        // true while the loop is inside Append
	results  []sm.Result // results of the batch being appended
}

func (s *server) apply(index uint64, payload []byte) {
	if s.st != nil {
		r := s.st.Apply(index, payload)
		if s.collect {
			s.results = append(s.results, r)
		}
	}
	sum := sha256.Sum256(payload)
	s.mu.Lock()
	s.sums = append(s.sums, sum)
	s.chain = sha256.Sum256(append(s.chain[:], sum[:]...))
	s.mu.Unlock()
}

func (s *server) publish() {
	s.mu.Lock()
	was := s.owner
	s.owner, s.epoch = s.n.IsOwner(), s.n.Epoch()
	now := s.owner
	s.mu.Unlock()
	s.followOwnership(now)
	if now != was {
		if now {
			log.Printf("node %d: became owner of epoch %d", s.id, s.n.Epoch())
		} else {
			log.Printf("node %d: no longer owner", s.id)
		}
	}
}

// maintain is the periodic work: an owner renews, everyone else tails the
// log and, if allowed, stands for ownership.
func (s *server) maintain(campaign bool) {
	if s.n.IsOwner() {
		if err := s.n.Renew(); err != nil {
			log.Printf("node %d: renew: %v", s.id, err)
		}
	} else {
		if err := s.n.Tail(); err != nil {
			log.Printf("node %d: tail: %v", s.id, err)
		}
		if campaign && time.Now().After(s.backoff) {
			err := s.n.Campaign()
			switch {
			case err == nil, errors.Is(err, lease.ErrNotExpired):
			case errors.Is(err, lease.ErrContended), errors.Is(err, lease.ErrLostBallot),
				errors.Is(err, lease.ErrOwnerAlive), errors.Is(err, lease.ErrLeaseLost),
				errors.Is(err, node.ErrLeaseLost):
				// Another node is in the same ballot. Wait a random time so
				// that challengers on the same tick do not collide forever.
				s.backoff = time.Now().Add(time.Duration(rand.Int63n(int64(s.l.TTL/2) + 1)))
			default:
				log.Printf("node %d: campaign: %v", s.id, err)
			}
		}
	}
	s.publish()
}

// write gathers every request that is already waiting into one batch, so
// that many client writes share one disk flush.
func (s *server) write(first *appendReq) {
	batch := []*appendReq{first}
	sizes := []int{len(first.payload)}
gather:
	for len(batch) < 4096 {
		select {
		case r := <-s.reqs:
			if !wal.Fits(s.l, append(sizes, len(r.payload))) {
				s.carry = r // does not fit; it goes first in the next batch
				break gather
			}
			batch, sizes = append(batch, r), append(sizes, len(r.payload))
		default:
			break gather
		}
	}
	payloads := make([][]byte, len(batch))
	for i, r := range batch {
		payloads[i] = r.payload
	}
	s.collect, s.results = true, s.results[:0]
	idx, err := s.n.Append(payloads)
	s.collect = false
	for i, r := range batch {
		resp := appendResp{index: idx + uint64(i), err: err}
		if err == nil && s.st != nil {
			resp.result = s.results[i]
		}
		r.resp <- resp
	}
	s.publish()
}

func (s *server) loop(ctx context.Context, tick time.Duration, campaign bool) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// A planned stop hands over immediately instead of making the
			// others wait for the lease to run out.
			if s.n.IsOwner() {
				if err := s.n.Release(); err != nil {
					log.Printf("node %d: release: %v", s.id, err)
				} else {
					log.Printf("node %d: released ownership", s.id)
				}
			}
			return
		case <-t.C:
			s.maintain(campaign)
		case b := <-s.barriers:
			s.barrier(b)
		case r := <-s.reqs:
			s.write(r)
			for s.carry != nil {
				r, s.carry = s.carry, nil
				s.write(r)
			}
		}
	}
}

func (s *server) handleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, int64(s.l.MaxBatch)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !wal.Fits(s.l, []int{len(payload)}) {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	// Answer at once when this node is not the owner, without queueing
	// behind whatever the node is busy with (a restarted node may be
	// replaying the log), so a client moves on to the next node quickly.
	s.mu.RLock()
	owner := s.owner
	s.mu.RUnlock()
	if !owner {
		http.Error(w, node.ErrNotOwner.Error(), http.StatusServiceUnavailable)
		return
	}
	req := &appendReq{payload: payload, resp: make(chan appendResp, 1)}
	select {
	case s.reqs <- req:
	case <-r.Context().Done():
		return
	}
	resp := <-req.resp
	switch {
	case resp.err == nil:
		fmt.Fprintf(w, "%d\n", resp.index)
	case errors.Is(resp.err, node.ErrNotOwner):
		http.Error(w, resp.err.Error(), http.StatusServiceUnavailable)
	default:
		// Includes a lease lost mid-write: the outcome is unknown to the
		// client, like a timeout.
		http.Error(w, resp.err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	st := status{Node: s.id, Owner: s.owner, Epoch: s.epoch,
		Applied: uint64(len(s.sums)), Chain: hex.EncodeToString(s.chain[:])}
	s.mu.RUnlock()
	json.NewEncoder(w).Encode(st)
}

// handleHashes returns one payload hash per line for indexes from..to.
func (s *server) handleHashes(w http.ResponseWriter, r *http.Request) {
	from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if from < 1 || to > uint64(len(s.sums)) || from > to {
		http.Error(w, "range not applied here", http.StatusNotFound)
		return
	}
	for i := from; i <= to; i++ {
		fmt.Fprintln(w, hex.EncodeToString(s.sums[i-1][:]))
	}
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	device := fs.String("device", "", "shared file or block device")
	id := fs.Int("node", 0, "this node's id, unique among nodes sharing the disk")
	listen := fs.String("listen", "127.0.0.1:7000", "address for the test HTTP interface")
	drift := fs.Float64("drift", 0.1, "bound on relative clock rate error between nodes")
	direct := fs.Bool("direct", true, "use direct I/O (required when nodes are on different machines)")
	campaign := fs.Bool("campaign", true, "stand for ownership; false makes a read-only follower")
	etcdListen := fs.String("etcd", "", "address for the etcd v3 gRPC API; empty leaves it off and the log carries opaque payloads")
	stateDir := fs.String("state-dir", "", "directory for the etcd state machine's files (rebuilt from the log at every start); default is the system temporary directory")
	startDelay := fs.Duration("start-delay", time.Second, "wait before first touching the disk, so a previous process's last write has landed")
	fs.Parse(args)

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	time.Sleep(*startDelay)
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	l, err := disk.Open(dev)
	if err != nil {
		return err
	}
	s := &server{id: *id, l: l, reqs: make(chan *appendReq, 8192)}
	if *etcdListen != "" {
		if s.st, err = sm.Open(*stateDir); err != nil {
			return err
		}
		defer s.st.Close()
		s.barriers = make(chan chan error, 8192)
		s.etcdAddr = *etcdListen
	}
	s.n, err = node.New(dev, realClock{time.Now()},
		lease.Config{Node: *id, Incarnation: uint64(time.Now().UnixNano()), Drift: *drift}, s.apply)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	mux := http.NewServeMux()
	mux.HandleFunc("/append", s.handleAppend)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/hashes", s.handleHashes)
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("node %d: %v", *id, err)
		}
	}()
	log.Printf("node %d: serving on %s, lease %s, direct=%v", *id, *listen, l.TTL, *direct)
	s.loop(ctx, l.TTL/5, *campaign)
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}

func cmdDump(args []string) error {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	device := fs.String("device", "", "shared file or block device")
	direct := fs.Bool("direct", true, "use direct I/O")
	fs.Parse(args)
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	s := &server{}
	n, err := node.New(dev, realClock{time.Now()}, lease.Config{Node: 0, Incarnation: 1, Drift: 0.1}, s.apply)
	if err != nil {
		return err
	}
	if err := n.Tail(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "records=%d epoch=%d chain=%s\n", len(s.sums), n.Epoch(), hex.EncodeToString(s.chain[:]))
	return nil
}
