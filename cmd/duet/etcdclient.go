package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// cmdEtcdLoad writes through the etcd v3 API with the stock etcd client,
// so the same load can be put on duet and on an etcd cluster. Every write
// is a create guarded by "the key does not exist", which is the write
// kube-apiserver issues. It records each acknowledged key and reports the
// longest stretch with no acknowledged write.
func cmdEtcdLoad(args []string) error {
	fs := flag.NewFlagSet("etcdload", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated etcd endpoints")
	duration := fs.Duration("duration", 30*time.Second, "how long to write")
	workers := fs.Int("workers", 8, "concurrent clients")
	size := fs.Int("value-size", 1024, "bytes per value")
	timeout := fs.Duration("timeout", 3*time.Second, "per-request timeout")
	acksPath := fs.String("acks", "acks.txt", "file to record acknowledged keys in")
	fs.Parse(args)

	acks, err := os.Create(*acksPath)
	if err != nil {
		return err
	}
	defer acks.Close()
	out := bufio.NewWriter(acks)
	value := strings.Repeat("x", *size)
	run := fmt.Sprint(time.Now().UnixNano()) // keeps runs on one store apart

	var mu sync.Mutex
	var lat []time.Duration
	var errs int
	begin := time.Now()
	last, gap, gapAt := begin, time.Duration(0), time.Duration(0)
	stop := begin.Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 5 * time.Second})
			if err != nil {
				mu.Lock()
				errs++
				mu.Unlock()
				return
			}
			defer cli.Close()
			for i := 0; time.Now().Before(stop); i++ {
				key := fmt.Sprintf("/load/%s/%d/%d", run, w, i)
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), *timeout)
				resp, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
					Then(clientv3.OpPut(key, value)).Commit()
				cancel()
				now := time.Now()
				mu.Lock()
				if err != nil || !resp.Succeeded {
					errs++
					mu.Unlock()
					time.Sleep(20 * time.Millisecond)
					continue
				}
				if d := now.Sub(last); d > gap {
					gap, gapAt = d, last.Sub(begin)
				}
				last = now
				lat = append(lat, now.Sub(start))
				fmt.Fprintln(out, key)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if err := out.Flush(); err != nil {
		return err
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"acknowledged":       len(lat),
		"errors":             errs,
		"writes_per_second":  float64(len(lat)) / time.Since(begin).Seconds(),
		"latency_ms_p50":     pct(0.50),
		"latency_ms_p99":     pct(0.99),
		"longest_gap_ms":     gap.Milliseconds(),
		"longest_gap_at_sec": gapAt.Seconds(),
	})
}

// cmdEtcdVerify checks that every key etcdload recorded as acknowledged
// can be read back.
func cmdEtcdVerify(args []string) error {
	fs := flag.NewFlagSet("etcdverify", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated etcd endpoints")
	acksPath := fs.String("acks", "acks.txt", "file written by etcdload")
	wait := fs.Duration("wait", 60*time.Second, "how long to keep trying to reach the store")
	fs.Parse(args)

	f, err := os.Open(*acksPath)
	if err != nil {
		return err
	}
	defer f.Close()
	want := map[string]bool{}
	prefix := ""
	for sc := bufio.NewScanner(f); sc.Scan(); {
		want[sc.Text()] = true
		if prefix == "" {
			prefix = sc.Text()[:strings.Index(sc.Text()[len("/load/"):], "/")+len("/load/")+1]
		}
	}
	if len(want) == 0 {
		return fmt.Errorf("no acknowledged writes recorded")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer cli.Close()
	found := 0
	key, end := prefix, clientv3.GetPrefixRangeEnd(prefix)
	for deadline := time.Now().Add(*wait); ; {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithKeysOnly(), clientv3.WithLimit(5000))
		cancel()
		if err != nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("could not read the store: %w", err)
			}
			time.Sleep(time.Second)
			continue
		}
		for _, kv := range resp.Kvs {
			if want[string(kv.Key)] {
				found++
				delete(want, string(kv.Key))
			}
		}
		if !resp.More {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	if len(want) > 0 {
		for k := range want {
			return fmt.Errorf("%d acknowledged writes are missing, for example %s", len(want), k)
		}
	}
	fmt.Printf("OK: all %d acknowledged writes present\n", found)
	return nil
}
