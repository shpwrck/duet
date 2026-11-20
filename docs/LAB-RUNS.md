# Lab run log

Every Google Cloud lab run, in order. The owner approved up to 20 runs on
19 Nov 2026; this file is the count.

| # | Started (UTC) | Run | Outcome |
| --- | --- | --- | --- |
| 1 | 2026-11-20 04:17 | [44673361599](https://github.com/shpwrck/duet/actions/runs/44673361599) | Passed first time. All six scenarios verified: 1,387,881 acknowledged writes, none missing, both nodes agree. Found two flaws in the test itself: the load client reconnected constantly above 2 writers, and stopping a VM shuts it down gracefully, so "vm_stop" measured a planned handover. |
