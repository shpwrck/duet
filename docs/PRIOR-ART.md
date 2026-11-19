# Prior art

What exists, what each gets right, and why it does not solve this problem.
Researched November 2026.

## Shims and replacements for etcd

| System | What it is | Gets right | Shortfall here | What duet does instead |
| --- | --- | --- | --- | --- |
| [Kine](https://github.com/k3s-io/kine) | etcd API shim over SQLite, Postgres, MySQL, NATS, T4 | Shows kube-apiserver needs only a subset of etcd | HA is the backing database's problem. On SQL, one poll loop feeds every watch, and compaction has deleted rows the poller had not read ([PR 759](https://github.com/k3s-io/kine/pull/759)). Revisions come from auto-increment IDs and can have gaps. Dropped 16.5% of writes on Postgres at concurrency 1000 in the [k0smotron benchmark](https://blog.k0sproject.io/posts/k0smotron-big-bang-benchmark/). | Owns its log. Dense revisions from log order. Watches pushed from the apply loop. Compaction as a logged command. |
| [GKE on Spanner](https://cloud.google.com/blog/products/containers-kubernetes/gke-65k-nodes-and-counting) | etcd API over a Spanner-based store | Unmodified Kubernetes at 130,000 nodes | Closed. Needs Spanner. | Uses storage the platform already has. Open. |
| [EKS on Journal](https://aws.amazon.com/blogs/containers/under-the-hood-amazon-eks-ultra-scale-clusters/) | etcd with Raft removed; ordering and durability in AWS's internal Journal | Shows etcd's state machine is safe without Raft; replicas freed from quorum; partitioning | Journal is proprietary and the modified etcd is unreleased | Same shape, with a shared disk as the journal |
| [MemoryDB](https://www.amazon.science/publications/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database) | Redis with durability in the same Journal | Publishes the key idea: leadership by conditional append, replicas that never talk to each other | Needs a conditional-append service | Ownership decided by Disk Paxos on the disk |
| [T4](https://t4db.github.io/t4/faq/) | etcd-compatible store with S3 as the recovery log | No quorum requirement; leadership by a conditional write | Depends on object storage | No external service |
| [tigerstore](https://github.com/djosh34/tigerstore) | New etcd replacement on the etcd Raft library | Careful public research on what kube-apiserver calls and how to test a replacement | Standard Raft, three nodes | Different problem; its test research is reused |

## Two-node designs

| System | Tiebreaker | Shortfall here |
| --- | --- | --- |
| [OpenShift two-node with arbiter](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html-single/installing_a_two_node_openshift_cluster/index) | A third node running a full etcd member | Adds a machine that stores all data and votes on every write |
| [OpenShift two-node with fencing](https://www.redhat.com/en/blog/two-node-openshift-fencing-improves-reliability-edge) | Power-off through the peer's BMC, driven by Pacemaker | Needs BMCs and the RHEL HA stack; control plane VMs have no BMC |
| [etcd witness proposal](https://github.com/etcd-io/etcd/issues/20696) | A witness member that holds no data | Still a third party; a proposal, not a release |
| Pacemaker with disk fencing (SBD) and stock etcd | Poison pill on a shared disk plus a watchdog | Works with existing software. Cold standby, safety depends on a watchdog firing in time, and it puts a cluster manager in every control plane VM. **This is the baseline duet has to beat.** |

## Platform primitives in VCF 9

| Primitive | Use to duet | Limit |
| --- | --- | --- |
| vSAN quorum | Replicates the disk; on a partition the losing side loses access and its VMs are stopped | Needs the vSAN witness, which the cluster already has |
| Multi-writer VMDK | The shared disk. [Supported on vSAN for Linux HA clusters](https://techdocs.broadcom.com/us/en/vmware-cis/vsan/vsan/8-0/vsan-administration/expanding-and-managing-a-vsan-cluster/clustering-applications-on-vsan.html). | No storage-level locking, so the protocol must make a second writer harmless |
| Clustered VMDK with SCSI-3 reservations | Storage-enforced single writer; the stronger option | [On vSAN, supported for Windows failover clustering only](https://knowledge.broadcom.com/external/article/433580/redhat-clustering-suite-and-suse-linux-c.html) |
| vSphere HA, Fault Tolerance | Baselines with no new code | HA restarts take minutes; FT support for a control plane VM is unverified |

## Theory

- Gafni and Lamport, *Disk Paxos* (2003): consensus among processes that
  share only disks. The ownership ballot is this algorithm with one disk.
- Lamport, Malkhi and Zhou, [*Vertical Paxos and Primary-Backup Replication*](https://www.microsoft.com/en-us/research/publication/vertical-paxos-and-primary-backup-replication/) (2009): a
  small configuration service lets data live on f+1 replicas. Here the
  shared disk is that service.
- sanlock: a production implementation of disk leases with Paxos, used by
  oVirt, which pairs its leases with a watchdog. duet's lease uses a
  write-then-read handshake on both sides so that safety does not depend on
  a watchdog firing.
