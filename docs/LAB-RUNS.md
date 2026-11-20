# Lab run log

Every Google Cloud lab run, in order. The owner approved up to 20 runs on
19 Nov 2026; this file is the count.

| # | Started (UTC) | Run | Outcome |
| --- | --- | --- | --- |
| 1 | 2026-11-20 04:17 | [44673361599](https://github.com/shpwrck/duet/actions/runs/44673361599) | Passed first time. All six scenarios verified: 1,387,881 acknowledged writes, none missing, both nodes agree. Found two flaws in the test itself: the load client reconnected constantly above 2 writers, and stopping a VM shuts it down gracefully, so "vm_stop" measured a planned handover. |
| 2 | 2026-11-20 04:39 | [44675861074](https://github.com/shpwrck/duet/actions/runs/44675861074) | Failed before creating anything: the `GCP_PROJECT` repository variable had a trailing line break. The workflow now strips whitespace from all three. |
| 3 | 2026-11-20 04:40 | [44675996672](https://github.com/shpwrck/duet/actions/runs/44675996672) | **Found a real flaw.** Five scenarios verified, including 24,727 writes/s with 128 writers. After a hypervisor suspend and resume of the owner VM, the other node took over correctly in 6.1 s, but the resumed node never rejoined: its clock had stood still, so by its own clock the lease had not expired, and it accepted its own in-flight write past the cutoff. Reproduced in simulation and fixed: acknowledgement now depends on a disk read after the write, not on the clock. The hard-reset scenario then showed a 52 s outage only because the stuck node could not take over. |
| 4 | 2026-11-20 05:14 | [44679682852](https://github.com/shpwrck/duet/actions/runs/44679682852) | Passed with the fix. All seven scenarios verified, 1,587,723 acknowledged writes. The suspended VM now rejoins; hard reset recovers in 6.1 s (was 52 s). **The fix costs throughput:** 13,252 writes/s with 128 writers (was 24,727), p99 13.3 ms (was 8.2 ms), because every batch now pays a disk read after its write. Unexplained: with 8 writers, p50 is about 7.5 ms in three scenarios and 4.7 ms in the other three. The likely cause is which zone's node owns the log, not yet confirmed. |
| 5 | 2026-11-20 10:17 | [44717997195](https://github.com/shpwrck/duet/actions/runs/44717997195) | All 17 scenarios ran; the run is marked failed because one verification step could not SSH to the client. **Raw log:** all seven verified; 27,465 writes/s with 128 writers, which contradicts run 4's 13,252 on the same code, so run 4's "the fix halves throughput" does not hold and run-to-run variation is about 2x. Latency depends on which node owns the log (p50 about 3.7 ms on one, 7.7 ms on the other); not explained. **etcd API, duet (5 s lease) against three-member etcd:** 8 clients 1,781 vs 1,616 writes/s; 128 clients 6,791 vs 8,755; planned stop 0.55 s vs 0.04 s without writes; leader killed 6.6 s vs 3.0 s; worst zone lost for 40 s: duet 5.9 s without writes, etcd 45.6 s. Every duet scenario verified. etcd's first four verified; its zone-loss verification is the step that did not run. The script now retries SSH. |
| 6 | 2026-11-20 11:04 | [44724515999](https://github.com/shpwrck/duet/actions/runs/44724515999) | Passed: all 17 scenarios verified, etcd's zone loss included. duet with a 2 s lease this time. **Raw log:** 26,450 writes/s with 128 writers; takeover after a kill 2.5 s, after freeze, suspend or reset 4.0 s. **etcd API, duet against three-member etcd:** 8 clients 1,791 vs 1,482 writes/s; 128 clients 6,771 vs 8,320; planned stop 0.3 s vs 3.0 s without writes; leader killed 2.6 s vs 1.1 s; worst zone lost for 40 s: duet 2.5 s without writes, etcd 46.2 s. Latency again depends on the owner: p50 3.9 ms on node 0, 7.6 ms on node 1. In runs 4 to 6 the slow owner is the one in the load generator's zone; not explained. |

## duet against etcd, runs 5 and 6

Same load through the stock etcd client: creates guarded the way
kube-apiserver guards them, 1 KiB values. duet: two nodes, one per zone,
on one regional multi-writer disk. etcd v3.7.4: three members at default
timings, two in one zone and one in the other, each on its own disk. The
load generator is in the zone with one etcd member. n4-standard-2 VMs.

| | duet, run 5 (5 s lease) | duet, run 6 (2 s lease) | etcd, run 5 | etcd, run 6 |
| --- | --- | --- | --- | --- |
| 8 clients, writes/s | 1,781 | 1,791 | 1,616 | 1,482 |
| 8 clients, p50 / p99 ms | 4.2 / 11.9 | 4.2 / 10.4 | 4.7 / 10.8 | 5.0 / 12.1 |
| 128 clients, writes/s | 6,791 | 6,771 | 8,755 | 8,320 |
| 128 clients, p50 / p99 ms | 10.0 / 86.1 | 10.4 / 83.5 | 11.5 / 59.8 | 12.3 / 58.0 |
| Planned stop of leader, longest gap | 0.55 s | 0.29 s | 0.04 s | 3.02 s |
| Leader killed, longest gap | 6.6 s | 2.6 s | 3.0 s | 1.1 s |
| Worst zone lost for 40 s, longest gap | 5.9 s | 2.5 s | 45.6 s | 46.2 s |
| Acknowledged writes lost | 0 | 0 | 0 (zone loss not checked) | 0 |

What this does and does not show:

- **Zone loss is the difference.** etcd with its majority in the lost
  zone accepts no writes until that zone returns. duet carries on after
  one lease period. If etcd's minority zone is lost instead, etcd carries
  on too; an operator cannot choose which zone fails.
- **etcd is faster under heavy load here**, by about a quarter at 128
  clients, with a better tail. duet's log alone takes 26,000 writes a
  second on the same disks, so the limit is in duet's etcd layer, which
  applies writes on one loop and forwards half the clients through the
  other node. Not yet profiled.
- **At light load they are level**, duet slightly ahead.
- **A killed leader costs duet about one lease plus a second.** With a 2 s
  lease that is within etcd's own range across the two runs (1.1 s and
  3.0 s).
- etcd's planned-stop result differs 75-fold between the two runs, which
  is a reminder that two runs are two runs.

