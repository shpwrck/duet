package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ack struct {
	index uint64
	sum   string
	at    time.Time
	took  time.Duration
}

// cmdLoad writes through whichever node is the owner, following it when it
// changes, and records every acknowledged write. Its summary includes the
// longest stretch with no acknowledged write, which is the recovery time a
// client would have seen.
func cmdLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated base URLs of nodes")
	duration := fs.Duration("duration", 10*time.Second, "how long to write")
	workers := fs.Int("workers", 8, "concurrent writers")
	size := fs.Int("size", 256, "payload bytes")
	acksPath := fs.String("acks", "acks.txt", "file to record acknowledged writes in")
	fs.Parse(args)
	eps := strings.Split(*endpoints, ",")
	client := &http.Client{Timeout: 2 * time.Second}

	out, err := os.Create(*acksPath)
	if err != nil {
		return err
	}
	defer out.Close()
	bw := bufio.NewWriter(out)
	defer bw.Flush()

	var leader atomic.Int64
	acks := make(chan ack, 4096)
	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			pad := strings.Repeat("x", *size)
			for seq := 0; time.Now().Before(deadline); seq++ {
				payload := []byte(fmt.Sprintf("w%d-%d-%d-%s", w, seq, time.Now().UnixNano(), pad))[:max(*size, 32)]
				sum := sha256.Sum256(payload)
				ep := int(leader.Load())
				start := time.Now()
				resp, err := client.Post(eps[ep%len(eps)]+"/append", "application/octet-stream", bytes.NewReader(payload))
				if err != nil {
					leader.CompareAndSwap(int64(ep), int64(ep+1))
					time.Sleep(20 * time.Millisecond)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					leader.CompareAndSwap(int64(ep), int64(ep+1))
					time.Sleep(20 * time.Millisecond)
					continue
				}
				idx, err := strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
				if err != nil {
					continue
				}
				acks <- ack{index: idx, sum: hex.EncodeToString(sum[:]), at: time.Now(), took: time.Since(start)}
			}
		}(w)
	}
	go func() { wg.Wait(); close(acks) }()

	begin := time.Now()
	last, gap, gapAt := begin, time.Duration(0), time.Duration(0)
	var took []time.Duration
	n := 0
	for a := range acks {
		fmt.Fprintf(bw, "%d %s\n", a.index, a.sum)
		if d := a.at.Sub(last); d > gap {
			gap, gapAt = d, last.Sub(begin)
		}
		if a.at.After(last) {
			last = a.at
		}
		took = append(took, a.took)
		n++
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	pct := func(p float64) float64 {
		if len(took) == 0 {
			return 0
		}
		return float64(took[int(float64(len(took)-1)*p)].Microseconds()) / 1000
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"acknowledged":       n,
		"seconds":            time.Since(begin).Seconds(),
		"writes_per_second":  float64(n) / time.Since(begin).Seconds(),
		"latency_ms_p50":     pct(0.50),
		"latency_ms_p99":     pct(0.99),
		"longest_gap_ms":     gap.Milliseconds(),
		"longest_gap_at_sec": gapAt.Seconds(),
	})
}

func getStatus(client *http.Client, ep string) (status, error) {
	var st status
	resp, err := client.Get(ep + "/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// cmdVerify checks that every node has applied the same log and that every
// acknowledged write is in it, at the index it was acknowledged with.
func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated base URLs of nodes")
	acksPath := fs.String("acks", "acks.txt", "file written by load")
	wait := fs.Duration("wait", 30*time.Second, "how long to wait for nodes to converge")
	fs.Parse(args)
	eps := strings.Split(*endpoints, ",")
	client := &http.Client{Timeout: 5 * time.Second}

	want := map[uint64]string{}
	var maxIdx uint64
	f, err := os.Open(*acksPath)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var idx uint64
		var sum string
		if _, err := fmt.Sscanf(sc.Text(), "%d %s", &idx, &sum); err != nil {
			continue
		}
		if prev, ok := want[idx]; ok && prev != sum {
			return fmt.Errorf("index %d was acknowledged for two different writes", idx)
		}
		want[idx] = sum
		maxIdx = max(maxIdx, idx)
	}
	f.Close()

	// Wait until every node has applied the same number of records.
	var sts []status
	for deadline := time.Now().Add(*wait); ; time.Sleep(200 * time.Millisecond) {
		sts = sts[:0]
		same := true
		for _, ep := range eps {
			st, err := getStatus(client, ep)
			if err != nil {
				return fmt.Errorf("%s: %w", ep, err)
			}
			sts = append(sts, st)
			same = same && st.Applied == sts[0].Applied && st.Applied >= maxIdx
		}
		if same {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("nodes did not converge: %+v", sts)
		}
	}
	for _, st := range sts[1:] {
		if st.Chain != sts[0].Chain {
			return fmt.Errorf("nodes %d and %d applied different logs of the same length", sts[0].Node, st.Node)
		}
	}
	if maxIdx > 0 {
		resp, err := client.Get(fmt.Sprintf("%s/hashes?from=1&to=%d", eps[0], maxIdx))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("fetching hashes: %s", resp.Status)
		}
		sc := bufio.NewScanner(resp.Body)
		for idx := uint64(1); sc.Scan(); idx++ {
			if sum, ok := want[idx]; ok && sum != sc.Text() {
				return fmt.Errorf("acknowledged write at index %d is not in the log", idx)
			}
			delete(want, idx)
		}
		if len(want) != 0 {
			return fmt.Errorf("%d acknowledged writes are missing from the log", len(want))
		}
	}
	fmt.Printf("OK: %d nodes agree on %d records; all %d acknowledged writes present\n",
		len(sts), sts[0].Applied, len(want)+int(countAcks(*acksPath)))
	return nil
}

func countAcks(path string) uint64 {
	b, _ := os.ReadFile(path)
	return uint64(bytes.Count(b, []byte("\n")))
}
