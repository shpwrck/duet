package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// cmdEtcdBench measures the calls a kube-apiserver makes most: point reads
// that must be current, paged lists of a prefix, and the delay between a
// write being acknowledged and a watcher seeing it. It works against any
// etcd v3 endpoint, so the same command measures duet and etcd.
func cmdEtcdBench(args []string) error {
	fs := flag.NewFlagSet("etcdbench", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated etcd endpoints")
	keys := fs.Int("keys", 20000, "objects to load first")
	size := fs.Int("value-size", 4096, "bytes per object")
	workers := fs.Int("workers", 32, "concurrent clients")
	duration := fs.Duration("duration", 15*time.Second, "length of each phase")
	watchers := fs.Int("watchers", 50, "watch streams on the prefix")
	page := fs.Int64("page", 500, "list page size")
	fs.Parse(args)
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx := context.Background()
	val := strings.Repeat("x", *size)
	name := func(i int) string { return fmt.Sprintf("/bench/obj/%08d", i) }

	// Load.
	t0 := time.Now()
	var next atomic.Int64
	var failed atomic.Int64
	par(*workers, func(int) {
		for {
			i := int(next.Add(1)) - 1
			if i >= *keys {
				return
			}
			c, cancel := context.WithTimeout(ctx, 10*time.Second)
			if _, err := cli.Put(c, name(i), val); err != nil {
				failed.Add(1)
			}
			cancel()
		}
	})
	fmt.Printf("| load | %d objects of %d bytes | %.0f writes/s | %d failed |\n", *keys, *size, float64(*keys)/time.Since(t0).Seconds(), failed.Load())

	// Point reads that must be current.
	lat := timed(*workers, *duration, func(rng *rand.Rand) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := cli.Get(c, name(rng.Intn(*keys)))
		return err
	})
	fmt.Printf("| get, current | %d clients | %s |\n", *workers, lat)

	// Point reads that may be slightly stale (what the apiserver never asks for, shown for scale).
	lat = timed(*workers, *duration, func(rng *rand.Rand) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := cli.Get(c, name(rng.Intn(*keys)), clientv3.WithSerializable())
		return err
	})
	fmt.Printf("| get, may be stale | %d clients | %s |\n", *workers, lat)

	// Full paged list of the prefix, 4 at a time.
	lat = timed(4, *duration, func(*rand.Rand) error {
		key, rev := "/bench/obj/", int64(0)
		end := clientv3.GetPrefixRangeEnd(key)
		for {
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			r, err := cli.Get(c, key, clientv3.WithRange(end), clientv3.WithLimit(*page), clientv3.WithRev(rev))
			cancel()
			if err != nil {
				return err
			}
			if !r.More || len(r.Kvs) == 0 {
				return nil
			}
			rev = r.Header.Revision
			key = string(r.Kvs[len(r.Kvs)-1].Key) + "\x00"
		}
	})
	fmt.Printf("| list all %d, pages of %d | 4 clients | %s |\n", *keys, *page, lat)

	// Mixed: 90%% current reads, 10%% writes.
	lat = timed(*workers, *duration, func(rng *rand.Rand) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if rng.Intn(10) == 0 {
			_, err := cli.Put(c, name(rng.Intn(*keys)), val)
			return err
		}
		_, err := cli.Get(c, name(rng.Intn(*keys)))
		return err
	})
	fmt.Printf("| 90%% get, 10%% put | %d clients | %s |\n", *workers, lat)

	// Watch delay: writers stamp the send time in the value; watchers
	// record how long after that the event reached them.
	wctx, stop := context.WithCancel(ctx)
	var mu sync.Mutex
	var delays []time.Duration
	var wg sync.WaitGroup
	for w := 0; w < *watchers; w++ {
		wc, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 5 * time.Second})
		if err != nil {
			stop()
			return err
		}
		defer wc.Close()
		ch := wc.Watch(wctx, "/bench/watch/", clientv3.WithPrefix())
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for r := range ch {
				now := time.Now().UnixNano()
				for _, ev := range r.Events {
					if len(ev.Kv.Value) < 20 {
						continue
					}
					sent, _ := strconv.ParseInt(string(ev.Kv.Value[:20]), 10, 64)
					mine = append(mine, time.Duration(now-sent))
				}
			}
			mu.Lock()
			delays = append(delays, mine...)
			mu.Unlock()
		}()
	}
	time.Sleep(time.Second)
	pad := strings.Repeat("x", max(*size-20, 0))
	wl := timed(8, *duration, func(rng *rand.Rand) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := cli.Put(c, fmt.Sprintf("/bench/watch/%04d", rng.Intn(1000)), fmt.Sprintf("%020d", time.Now().UnixNano())+pad)
		return err
	})
	time.Sleep(time.Second)
	stop()
	wg.Wait()
	fmt.Printf("| put while %d watch | 8 clients | %s |\n", *watchers, wl)
	fmt.Printf("| send of put to watch event | %d watchers | %d events, %s |\n", *watchers, len(delays), pcts(delays))
	return nil
}

func par(n int, f func(int)) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); f(i) }(i)
	}
	wg.Wait()
}

// timed runs f from n clients for d and reports rate and latency.
func timed(n int, d time.Duration, f func(*rand.Rand) error) string {
	var mu sync.Mutex
	var all []time.Duration
	var errs int
	end := time.Now().Add(d)
	t0 := time.Now()
	par(n, func(i int) {
		rng := rand.New(rand.NewSource(int64(i) + 1))
		var mine []time.Duration
		bad := 0
		for time.Now().Before(end) {
			s := time.Now()
			if err := f(rng); err != nil {
				bad++
				continue
			}
			mine = append(mine, time.Since(s))
		}
		mu.Lock()
		all = append(all, mine...)
		errs += bad
		mu.Unlock()
	})
	return fmt.Sprintf("%.0f/s, %s, %d errors", float64(len(all))/time.Since(t0).Seconds(), pcts(all), errs)
}

func pcts(d []time.Duration) string {
	if len(d) == 0 {
		return "no samples"
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	q := func(p float64) string {
		return fmt.Sprintf("%.1f", float64(d[int(p*float64(len(d)-1))])/1e6)
	}
	return fmt.Sprintf("p50 %s ms, p99 %s ms, max %s ms", q(0.5), q(0.99), q(1))
}
