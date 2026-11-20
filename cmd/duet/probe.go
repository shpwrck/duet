package main

import (
	"encoding/json"
	"flag"
	"os"
	"sort"
	"time"

	"github.com/shpwrck/duet/pkg/disk"
)

// cmdDiskProbe times small synchronous writes to a device, the operation
// every duet write is made of. Run it on each node before formatting: the
// same shared disk has measured twice as slow from one node as from the
// other, differently on different days, and nothing else in a run can be
// read without knowing which case it was.
//
// It overwrites one block at the given offset. Do not point it at a disk
// that is in use.
func cmdDiskProbe(args []string) error {
	fs := flag.NewFlagSet("diskprobe", flag.ExitOnError)
	device := fs.String("device", "", "file or block device")
	offset := fs.Int64("offset", 1<<30, "where to write; one block is overwritten")
	count := fs.Int("count", 500, "writes to time")
	direct := fs.Bool("direct", true, "use direct I/O")
	fs.Parse(args)
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	buf := make([]byte, disk.BlockSize)
	lat := make([]time.Duration, 0, *count)
	for i := 0; i < *count; i++ {
		buf[0] = byte(i)
		start := time.Now()
		if _, err := dev.WriteAt(buf, *offset); err != nil {
			return err
		}
		if err := dev.Sync(); err != nil {
			return err
		}
		lat = append(lat, time.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	ms := func(p float64) float64 { return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000 }
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"writes": len(lat), "write_sync_ms_p50": ms(0.5), "write_sync_ms_p99": ms(0.99),
	})
}
