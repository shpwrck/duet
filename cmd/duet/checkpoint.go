package main

import (
	"errors"
	"io"
	"log"
	"os"
	"time"

	"github.com/shpwrck/duet/pkg/ckpt"
	"github.com/shpwrck/duet/pkg/node"
	"github.com/shpwrck/duet/pkg/sm"
	"github.com/shpwrck/duet/pkg/trim"
)

// restore opens the state machine from the newest intact checkpoint on the
// disk, written by any node, and returns where the log continues. With no
// usable checkpoint the store starts empty and the whole log is replayed.
func (s *server) restore(stateDir string) (node.Cursor, error) {
	// A checkpoint behind the published head is useless: the log between
	// the two may have been reused. If the only good one is being replaced
	// right now, a newer one is seconds away, so wait for it.
	for attempt := 0; ; attempt++ {
		head, err := trim.Head(s.dev, s.l)
		if err != nil {
			return node.Cursor{}, err
		}
		metas, err := ckpt.All(s.dev, s.l)
		if err != nil {
			return node.Cursor{}, err
		}
		for _, m := range metas {
			if m.Cursor.Next != m.Applied+1 || (head > s.l.LogStart() && m.Cursor.Offset < head) {
				continue
			}
			st, err := s.restoreFrom(stateDir, m)
			if err != nil {
				log.Printf("node %d: checkpoint at index %d by node %d unusable: %v", s.id, m.Applied, m.Node, err)
				continue
			}
			s.st, s.base, s.ckptLast, s.ckptLastOff = st, m.Applied, m.Applied, m.Cursor.Offset
			log.Printf("node %d: restored checkpoint at index %d (%d KiB, written by node %d)", s.id, m.Applied, m.Size>>10, m.Node)
			return m.Cursor, nil
		}
		if head <= s.l.LogStart() {
			break // nothing has been trimmed: the whole log can be replayed
		}
		if attempt >= 30 {
			return node.Cursor{}, errors.New("the log has been trimmed and no usable checkpoint covers it")
		}
		time.Sleep(time.Second)
	}
	var err error
	s.st, err = sm.Open(stateDir)
	return node.Cursor{}, err
}

func (s *server) restoreFrom(stateDir string, m ckpt.Meta) (*sm.Store, error) {
	// Verify the whole checkpoint before building a store from it.
	tmp, err := os.CreateTemp(stateDir, "duet-ckpt-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if err := ckpt.Read(s.dev, s.l, m, tmp); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return sm.Restore(stateDir, tmp, m.Applied)
}

// maybeCheckpoint starts a checkpoint when enough records have been
// applied since the last one. The loop decides and records the cursor; the
// state machine goroutine takes the snapshot when it reaches that point;
// the copy to disk then runs in the background.
func (s *server) maybeCheckpoint() {
	if s.st == nil || s.l.CkptSize == 0 || s.ckptEvery == 0 || s.ckptBusy.Load() {
		return
	}
	cur := s.n.Cursor()
	if cur.Next == 0 {
		return
	}
	applied := cur.Next - 1
	// Checkpoint after enough records, or once a quarter of the log has
	// been written since the last one, so that space is freed in time.
	if applied == s.ckptLast || (applied < s.ckptLast+s.ckptEvery && cur.Offset-s.ckptLastOff < s.l.LogSize/4) {
		return
	}
	s.ckptLast, s.ckptLastOff = applied, cur.Offset
	s.ckptBusy.Store(true)
	// The snapshot is taken by the state machine goroutine once it has
	// applied everything up to the cursor, which is everything queued
	// before this task.
	s.applyQ <- applyTask{fn: func() { s.checkpoint(applied, cur) }}
}

func (s *server) checkpoint(applied uint64, cur node.Cursor) {
	snap := s.st.Snapshot()
	if snap.Applied != applied {
		log.Printf("node %d: checkpoint skipped: store is at index %d, cursor says %d", s.id, snap.Applied, applied)
		snap.Close()
		s.ckptBusy.Store(false)
		return
	}
	go func() {
		defer s.ckptBusy.Store(false)
		defer snap.Close()
		start := time.Now()
		pr, pw := io.Pipe()
		copied := make(chan struct{})
		go func() {
			defer close(copied)
			_, err := snap.WriteTo(pw)
			pw.CloseWithError(err)
		}()
		_, err := ckpt.Write(s.dev, s.l, s.id, ckpt.Meta{Applied: applied, Cursor: cur, Size: snap.Size}, pr)
		// The copier may still be inside the snapshot, for instance when
		// the write gave up early. Stop it and wait before the snapshot is
		// closed under it.
		pr.CloseWithError(errors.New("checkpoint writer stopped"))
		<-copied
		if err != nil {
			log.Printf("node %d: checkpoint at index %d failed: %v", s.id, applied, err)
			return
		}
		s.ckptDone.Store(applied)
		// Two checkpoints of this node are now on disk. Vouch for the log
		// up to the older one: a reader overtaken by that head still finds
		// a checkpoint beyond it, also while the older slot is being
		// replaced next time. Publishing is the loop's job.
		if prev := s.ckptPrevOff.Swap(cur.Offset); prev > 0 {
			select {
			case s.trimQ <- prev:
			default:
			}
		}
		log.Printf("node %d: checkpoint at index %d (%d KiB) in %s", s.id, applied, snap.Size>>10, time.Since(start).Round(time.Millisecond))
	}()
}
