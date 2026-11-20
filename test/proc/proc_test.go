// Package proc runs real duet processes against one disk image and injects
// faults with signals. Unlike the simulation in pkg/node, this exercises
// the real clock, direct I/O, the renewal loop and group commit. All
// processes still share one kernel; separate machines need a lab.
package proc

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type proc struct {
	id  int
	url string
	cmd *exec.Cmd
}

type cluster struct {
	t      *testing.T
	bin    string
	device string
	direct string
	nodes  []*proc
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (c *cluster) start(id int, addr string) *proc {
	c.t.Helper()
	cmd := exec.Command(c.bin, "run", "-device", c.device, "-node", fmt.Sprint(id),
		"-listen", addr, "-start-delay", "200ms", c.direct)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	return &proc{id: id, url: "http://" + addr, cmd: cmd}
}

func (c *cluster) owner() *proc {
	client := &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		for _, p := range c.nodes {
			resp, err := client.Get(p.url + "/status")
			if err != nil {
				continue
			}
			var st struct{ Owner bool }
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.Owner {
				return p
			}
		}
	}
	c.t.Fatal("no owner within 10s")
	return nil
}

func (c *cluster) endpoints() string {
	urls := make([]string, len(c.nodes))
	for i, p := range c.nodes {
		urls[i] = p.url
	}
	return strings.Join(urls, ",")
}

func TestPauseAndKillOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes; skipped with -short")
	}
	dir := t.TempDir()
	c := &cluster{t: t, bin: filepath.Join(dir, "duet"), device: filepath.Join(dir, "disk.img"), direct: "-direct=true"}
	if out, err := exec.Command("go", "build", "-o", c.bin, "github.com/shpwrck/duet/cmd/duet").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	if err := os.WriteFile(c.device, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(c.device, 256<<20); err != nil {
		t.Fatal(err)
	}
	format := func() ([]byte, error) {
		return exec.Command(c.bin, "format", "-device", c.device, "-nodes", "3", "-ttl", "1s",
			"-epoch-slots", "256", "-max-batch", "262144", c.direct).CombinedOutput()
	}
	if out, err := format(); err != nil {
		t.Logf("direct I/O unavailable, falling back to buffered: %s", out)
		c.direct = "-direct=false"
		if out, err := format(); err != nil {
			t.Fatalf("format: %v\n%s", err, out)
		}
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	for i, a := range addrs {
		c.nodes = append(c.nodes, c.start(i, a))
	}
	defer func() {
		for _, p := range c.nodes {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	}()
	c.owner() // wait for the first owner

	acks := filepath.Join(dir, "acks.txt")
	load := exec.Command(c.bin, "load", "-endpoints", c.endpoints(), "-duration", "14s", "-acks", acks)
	load.Stderr = os.Stderr
	out, err := load.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := load.Start(); err != nil {
		t.Fatal(err)
	}

	// Fault 1: freeze the owner for three lease periods, then let it run.
	time.Sleep(3 * time.Second)
	frozen := c.owner()
	t.Logf("freezing owner, node %d, for 3s, at %s", frozen.id, time.Now().Format("15:04:05.000"))
	frozen.cmd.Process.Signal(syscall.SIGSTOP)
	time.Sleep(3 * time.Second)
	frozen.cmd.Process.Signal(syscall.SIGCONT)

	// Fault 2: kill the current owner outright and restart it.
	time.Sleep(2 * time.Second)
	victim := c.owner()
	t.Logf("killing owner, node %d, at %s", victim.id, time.Now().Format("15:04:05.000"))
	victim.cmd.Process.Kill()
	victim.cmd.Wait()
	time.Sleep(time.Second)
	c.nodes[victim.id] = c.start(victim.id, addrs[victim.id])

	var summary struct {
		Acknowledged   int     `json:"acknowledged"`
		WritesPerSec   float64 `json:"writes_per_second"`
		LongestGapMs   int     `json:"longest_gap_ms"`
		LatencyP99Ms   float64 `json:"latency_ms_p99"`
		LongestGapAtSe float64 `json:"longest_gap_at_sec"`
	}
	if err := json.NewDecoder(out).Decode(&summary); err != nil {
		t.Fatalf("load summary: %v", err)
	}
	if err := load.Wait(); err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Logf("load: %d acknowledged, %.0f writes/s, p99 %.1f ms, longest gap %d ms at %.1fs",
		summary.Acknowledged, summary.WritesPerSec, summary.LatencyP99Ms, summary.LongestGapMs, summary.LongestGapAtSe)
	if summary.Acknowledged == 0 {
		t.Fatal("nothing was acknowledged")
	}

	verify, err := exec.Command(c.bin, "verify", "-endpoints", c.endpoints(), "-acks", acks).CombinedOutput()
	t.Logf("verify: %s", strings.TrimSpace(string(verify)))
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	// With a 1s lease, takeover should take well under 5s. This is a loose
	// bound to catch a stuck failover, not a performance claim.
	if summary.LongestGapMs > 5000 {
		t.Fatalf("longest gap without an acknowledged write was %d ms", summary.LongestGapMs)
	}
}
