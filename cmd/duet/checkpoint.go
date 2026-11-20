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
)

// restore opens the state machine from the newest intact checkpoint on the
// disk, written by any node, and returns where the log continues. With no
// usable checkpoint the store starts empty and the whole log is replayed.
func (s *server) restore(stateDir string) (node.Cursor, error) {
	metas, err := ckpt.All(s.dev, s.l)
	if err != nil {
		return node.Cursor{}, err
	}
	for _, m := range metas {
		if m.Cursor.Next != m.Applied+1 {
			continue
		}
		st, err := s.restoreFrom(stateDir, m)
		if err != nil {
			log.Printf("node %d: checkpoint at index %d by node %d unusable: %v", s.id, m.Applied, m.Node, err)
			continue
		}
		s.st, s.base, s.ckptLast = st, m.Applied, m.Applied
		log.Printf("node %d: restored checkpoint at index %d (%d KiB, written by node %d)", s.id, m.Applied, m.Size>>10, m.Node)
		return m.Cursor, nil
	}
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
	if applied < s.ckptLast+s.ckptEvery {
		return
	}
	s.ckptLast = applied
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
		go func() {
			_, err := snap.WriteTo(pw)
			pw.CloseWithError(err)
		}()
		_, err := ckpt.Write(s.dev, s.l, s.id, ckpt.Meta{Applied: applied, Cursor: cur, Size: snap.Size}, pr)
		pr.CloseWithError(errors.New("checkpoint writer stopped"))
		if err != nil {
			log.Printf("node %d: checkpoint at index %d failed: %v", s.id, applied, err)
			return
		}
		s.ckptDone.Store(applied)
		log.Printf("node %d: checkpoint at index %d (%d KiB) in %s", s.id, applied, snap.Size>>10, time.Since(start).Round(time.Millisecond))
	}()
}
