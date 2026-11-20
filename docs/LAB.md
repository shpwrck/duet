# Labs

Three places the protocol runs, in increasing order of realism. None of
them is vSAN; results from each are labelled with where they came from.

| Lab | What it is | Status |
| --- | --- | --- |
| Simulation (`pkg/node`) | All nodes in one process on an ideal in-memory disk, interleaved at single block operations | Runs in CI |
| Processes (`test/proc`) | Real `duet` processes with direct I/O, a real clock and signals, on one disk image and one kernel | Runs locally |
| Google Cloud (`deploy/lab-gcp`) | Two nodes in two zones on one regionally replicated multi-writer disk | Run seven times; see [LAB-RUNS.md](LAB-RUNS.md) |

## Google Cloud lab

| vSAN piece | Stand-in | Faithful in | Not faithful in |
| --- | --- | --- | --- |
| Multi-writer VMDK mirrored across two hosts | Hyperdisk Balanced High Availability in multi-writer mode, replicated across two zones | Two kernels, no shared page cache, synchronously replicated network block storage, two failure domains | Different storage system with its own semantics and latency |
| vSAN witness | Google's own regional disk control plane | The tiebreak lives in the storage layer, not in duet | Not visible or testable |
| Host failure | Stopping the owner's VM | The node and its zone attachment disappear | No storage rebuild to observe |
| VM stun | Freezing the process for four lease periods | The pause case exactly | Not a hypervisor-level stun |
| Clustered VMDK reservations | NVMe persistent reservations on the same disk | Register, acquire, release | Google does not support preempt-and-abort, so the reservation backend must manage with preempt alone or acquire after release |

`run.sh` has two parts. The first drives duet's log directly: baseline,
throughput (128 writers), planned handover, owner frozen, owner killed,
owner's VM suspended and resumed, owner's VM hard reset. The second puts
the same load through the etcd API on duet and on a three-member etcd
cluster: baseline, throughput, planned stop of the leader, leader killed,
and every member in the worst zone killed for 40 s. Each scenario records
acknowledged writes, throughput, latency, the longest stretch with no
acknowledged write, and whether any acknowledged write is missing. The
script first times the shared disk from each node, because that has
differed two-fold between nodes and between runs.

### Running it

One-time, by the project owner, in Cloud Shell:

```
git clone https://github.com/shpwrck/duet && cd duet/deploy/lab-gcp
./setup.sh YOUR_PROJECT_ID
```

Set the three values it prints as repository variables. Then start the
`lab-gcp` workflow from the Actions tab and type the confirmation phrase.
The workflow builds, creates the lab, runs the scenarios, uploads
`results.json` and destroys everything, including when a step fails.

### What it creates and what it costs

- Three `n4-standard-2` VMs (two nodes, one client), no public addresses.
- One 20 GB regional Hyperdisk Balanced High Availability volume.
- Three 10 GB boot disks, one network, one subnet, two firewall rules.

A run takes well under an hour. The VMs list at about $0.09 an hour each
in us-central1 (third-party price trackers, November 2026); disk charges at
this size and duration have not been priced but are small next to that.
Budget alerts in Google Cloud warn and do not cap, so the safeguards are:
teardown always runs, the VMs delete themselves after two hours, and the
workflow fails loudly if any labelled resource is left.

### Still on a KVM host or iSCSI target

Stock etcd under Pacemaker with disk-based fencing (the milestone 1
baseline) can run on the same Google Cloud lab and is the next addition.

## What only vSAN can answer

Everything in the "To verify" table of [VCF-GAPS.md](VCF-GAPS.md). These
labs exercise the protocol; they cannot confirm what the protocol assumes
about a vSAN disk.
