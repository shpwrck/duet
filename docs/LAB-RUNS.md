# Lab run log

Every Google Cloud lab run, in order. The owner approved up to 20 runs on
19 Nov 2026; this file is the count.

| # | Started (UTC) | Run | Outcome |
| --- | --- | --- | --- |
| 1 | 2026-11-20 04:17 | [44673361599](https://github.com/shpwrck/duet/actions/runs/44673361599) | Passed first time. All six scenarios verified: 1,387,881 acknowledged writes, none missing, both nodes agree. Found two flaws in the test itself: the load client reconnected constantly above 2 writers, and stopping a VM shuts it down gracefully, so "vm_stop" measured a planned handover. |
| 2 | 2026-11-20 04:39 | [44675861074](https://github.com/shpwrck/duet/actions/runs/44675861074) | Failed before creating anything: the `GCP_PROJECT` repository variable had a trailing line break. The workflow now strips whitespace from all three. |
| 3 | 2026-11-20 04:40 | [44675996672](https://github.com/shpwrck/duet/actions/runs/44675996672) | **Found a real flaw.** Five scenarios verified, including 24,727 writes/s with 128 writers. After a hypervisor suspend and resume of the owner VM, the other node took over correctly in 6.1 s, but the resumed node never rejoined: its clock had stood still, so by its own clock the lease had not expired, and it accepted its own in-flight write past the cutoff. Reproduced in simulation and fixed: acknowledgement now depends on a disk read after the write, not on the clock. The hard-reset scenario then showed a 52 s outage only because the stuck node could not take over. |
| 4 | 2026-11-20 05:14 | [44679682852](https://github.com/shpwrck/duet/actions/runs/44679682852) | Passed with the fix. All seven scenarios verified, 1,587,723 acknowledged writes. The suspended VM now rejoins; hard reset recovers in 6.1 s (was 52 s). **The fix costs throughput:** 13,252 writes/s with 128 writers (was 24,727), p99 13.3 ms (was 8.2 ms), because every batch now pays a disk read after its write. Unexplained: with 8 writers, p50 is about 7.5 ms in three scenarios and 4.7 ms in the other three. The likely cause is which zone's node owns the log, not yet confirmed. |
