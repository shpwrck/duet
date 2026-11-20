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
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	ckptSize := fs.Int64("checkpoint-size", 0, "bytes for one checkpoint of the etcd state; every node gets two such slots; 0 means no checkpoints")
	maxBatch := fs.Int("max-batch", 1<<20, "largest single log write in bytes")
	direct := fs.Bool("direct", true, "use direct I/O")
	fs.Parse(args)
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	l := disk.Layout{Nodes: *nodes, EpochSlots: *slots, LogSize: *logSize, TTL: *ttl, MaxBatch: *maxBatch, CkptSize: *ckptSize}
	if l.LogSize == 0 {
		l.LogSize = (dev.Size() - l.LogStart() - int64(2*l.Nodes)*l.CkptSize) &^ (disk.BlockSize - 1)
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
	Checkpoint uint64  `json:"checkpoint"` // index of this node's last checkpoint
	Restored   uint64  `json:"restored"`   // index of the checkpoint this node started from
	Batches    uint64  `json:"batches"`    // log writes since start
	Records    uint64  `json:"records"`    // records in those writes
	AppendMs   uint64  `json:"append_ms"`  // time spent in them
	Laps       float64 `json:"laps"`       // how many times round the log this node's position is
	ApplyMs    uint64  `json:"apply_ms"`   // spent applying to the state machine, on its own goroutine
	Node       int     `json:"node"`
	Owner      bool    `json:"owner"`
	Epoch      uint64  `json:"epoch"`
	Applied    uint64  `json:"applied"`
	Chain      string  `json:"chain"`
}

type server struct {
	id      int
	n       *node.Node
	l       disk.Layout
	reqs    chan *appendReq
	carry   *appendReq // a request that did not fit in the previous batch
	backoff time.Time  // do not campaign again before this

	mu        sync.RWMutex
	sums      [][32]byte // sha256 of each applied payload, by index-1
	chain     [32]byte   // hash chain over sums; equal chains mean equal logs
	owner     bool
	epoch     uint64
	ownerNode int // who this node believes owns the log; -1 if unknown

	base uint64 // records before this index came from a checkpoint

	// Write-path counters, for finding where time goes.
	statBatches, statRecords, statAppendNs, statApplyNs atomic.Uint64

	// etcd mode only.
	dev           disk.Device
	ckptEvery     uint64
	ckptLast      uint64       // index at which the last checkpoint was started
	ckptLastOff   int64        // and the log offset there
	ckptPrevOff   atomic.Int64 // log offset of the last checkpoint known to be on disk
	trimQ         chan int64   // offsets to publish as the head, for the loop
	lapped        bool         // the log has been reused past this node's position
	cursorOff     atomic.Int64
	ckptBusy      atomic.Bool
	ckptDone      atomic.Uint64
	st            *sm.Store
	watchProgress time.Duration
	front         *front
	fwd           *forwarder
	barriers      chan chan error
	pending       []*appendReq // requests of the batch being appended, not yet handed to the applier
	applyQ        chan applyTask
	applyDone     chan struct{}
}

// apply is called by the node, on the loop goroutine, for every record in
// log order. In etcd mode it only queues the record: the state machine is
// driven by its own goroutine, so that the loop can start the next disk
// write while the previous batch is still being applied.
func (s *server) apply(index uint64, payload []byte) {
	if s.st != nil {
		t := applyTask{index: index, payload: payload}
		if s.pending != nil {
			t.req, s.pending = s.pending[0], s.pending[1:]
		}
		s.applyQ <- t
	}
	sum := sha256.Sum256(payload)
	s.mu.Lock()
	s.sums = append(s.sums, sum)
	s.chain = sha256.Sum256(append(s.chain[:], sum[:]...))
	s.mu.Unlock()
}

// applyTask is one step for the state machine goroutine: a record to
// apply, with the request waiting for its result if there is one, or a
// function to run once everything queued before it has been applied.
type applyTask struct {
	index   uint64
	payload []byte
	req     *appendReq
	fn      func()
}

// applier owns the state machine. Records reach it in log order and only
// after they are acknowledged, so the store never holds a write that
// could be lost, and a write is answered only once it has been applied.
func (s *server) applier() {
	defer close(s.applyDone)
	for t := range s.applyQ {
		if t.fn != nil {
			t.fn()
			continue
		}
		began := time.Now()
		r := s.st.Apply(t.index, t.payload)
		s.statApplyNs.Add(uint64(time.Since(began)))
		if t.req != nil {
			t.req.resp <- appendResp{index: t.index, result: r}
		}
	}
}

func (s *server) publish() {
	s.mu.Lock()
	was := s.owner
	s.owner, s.epoch, s.ownerNode = s.n.IsOwner(), s.n.Epoch(), s.n.Owner()
	if c := s.n.Cursor(); c.Offset > 0 {
		s.cursorOff.Store(c.Offset)
	}
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
		if err := s.n.Tail(); errors.Is(err, node.ErrLapped) {
			s.lapped = true
			return
		} else if err != nil {
			log.Printf("node %d: tail: %v", s.id, err)
		}
		if campaign && time.Now().After(s.backoff) {
			err := s.n.Campaign()
			switch {
			case errors.Is(err, node.ErrLapped):
				s.lapped = true
				return
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
	// In etcd mode the applier answers each request after applying it;
	// apply hands it the requests in the order of this batch.
	if s.st != nil {
		s.pending = batch
	}
	began := time.Now()
	idx, err := s.n.Append(payloads)
	s.pending = nil
	s.statBatches.Add(1)
	s.statRecords.Add(uint64(len(batch)))
	s.statAppendNs.Add(uint64(time.Since(began)))
	if err != nil || s.st == nil {
		for i, r := range batch {
			r.resp <- appendResp{index: idx + uint64(i), err: err}
		}
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
			if s.lapped {
				return
			}
			s.maybeCheckpoint()
		case b := <-s.barriers:
			s.barrier(b)
		case off := <-s.trimQ:
			// Free space only as it is needed: keep the last half lap of
			// the log readable, so that a standby a little behind, or one
			// that restarts, is not overtaken by the head.
			if keep := s.n.Cursor().Offset - s.l.LogSize/2; off > keep {
				off = keep
			}
			if off <= s.l.LogStart() {
				continue
			}
			if err := s.n.Trim(off); err != nil {
				log.Printf("node %d: publishing head %d: %v", s.id, off, err)
			}
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
		Checkpoint: s.ckptDone.Load(), Restored: s.base,
		Laps:    float64(s.cursorOff.Load()-s.l.LogStart()) / float64(s.l.LogSize),
		Batches: s.statBatches.Load(), Records: s.statRecords.Load(),
		AppendMs: s.statAppendNs.Load() / 1e6, ApplyMs: s.statApplyNs.Load() / 1e6,
		Applied: s.base + uint64(len(s.sums)), Chain: hex.EncodeToString(s.chain[:])}
	s.mu.RUnlock()
	json.NewEncoder(w).Encode(st)
}

// handleHashes returns one payload hash per line for indexes from..to.
func (s *server) handleHashes(w http.ResponseWriter, r *http.Request) {
	from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if from <= s.base || to > s.base+uint64(len(s.sums)) || from > to {
		http.Error(w, "range not applied here", http.StatusNotFound)
		return
	}
	for i := from; i <= to; i++ {
		fmt.Fprintln(w, hex.EncodeToString(s.sums[i-1-s.base][:]))
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
	etcdPeers := fs.String("etcd-peers", "", "comma-separated etcd API addresses of all nodes, in node id order; lets a node that does not own the log forward calls to the one that does")
	stateDir := fs.String("state-dir", "", "directory for the etcd state machine's files (rebuilt from the log at every start); default is the system temporary directory")
	ckptEvery := fs.Uint64("checkpoint-every", 100000, "write a checkpoint of the etcd state after this many records; 0 never. Needs a disk formatted with -checkpoint-size")
	watchProgress := fs.Duration("watch-progress-interval", 10*time.Minute, "how often idle watchers that asked for progress notifications get one (etcd's --watch-progress-notify-interval; kubeadm sets 5s)")
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
	s := &server{id: *id, l: l, ownerNode: -1, reqs: make(chan *appendReq, 8192)}
	var cursor node.Cursor
	if *etcdListen != "" {
		s.dev, s.ckptEvery = dev, *ckptEvery
		if cursor, err = s.restore(*stateDir); err != nil {
			return err
		}
		defer s.st.Close()
		s.applyQ, s.applyDone = make(chan applyTask, 1<<16), make(chan struct{})
		go s.applier()
		s.barriers = make(chan chan error, 8192)
		s.trimQ = make(chan int64, 4)
		s.watchProgress = *watchProgress
	}
	s.n, err = node.New(dev, realClock{time.Now()},
		lease.Config{Node: *id, Incarnation: uint64(time.Now().UnixNano()), Drift: *drift}, s.apply)
	if err != nil {
		return err
	}
	if err := s.n.Seek(cursor); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	mux := http.NewServeMux()
	mux.HandleFunc("/append", s.handleAppend)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/hashes", s.handleHashes)
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("node %d: %v", *id, err)
		}
	}()
	if s.st != nil {
		var peers []string
		if *etcdPeers != "" {
			peers = strings.Split(*etcdPeers, ",")
		}
		if err := s.startEtcd(*etcdListen, peers); err != nil {
			return err
		}
		go s.expireLeases(ctx)
	}
	log.Printf("node %d: serving on %s, lease %s, direct=%v", *id, *listen, l.TTL, *direct)
	s.loop(ctx, l.TTL/5, *campaign)
	if s.applyQ != nil {
		close(s.applyQ) // let queued records finish before the store closes
		<-s.applyDone
	}
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	if s.lapped {
		// Exit so that whatever supervises this process starts it again:
		// a fresh start loads the newest checkpoint and, as a new
		// incarnation, retires this one's old epochs.
		return errors.New("the log has been reused past this node's position; restart to restore from a checkpoint")
	}
	return nil
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
