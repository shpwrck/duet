# VCF gaps

What is unverified or missing in VMware Cloud Foundation for duet to ship
as part of VKS. Each row is a question to answer or a request to make.

## To verify (milestone 0)

| Question | Why it matters | How |
| --- | --- | --- |
| Are multi-writer or clustered VMDKs supported on a two-host vSAN cluster? | The minimum deployment. Documentation confirms stretched clusters; no explicit two-host statement found. | vSAN product team, then test |
| On a multi-writer vSAN disk, is a read from one host guaranteed to see a write that returned on another? | Assumption A1 | Test with two VMs on different hosts |
| Does a flush on a multi-writer vSAN disk mean the data is on both hosts? | Assumption A3 | vSAN engineering, then fault test |
| What is flush latency and its tail on such a disk? | Sets lease length and the throughput ceiling | Measure |
| What happens to in-flight I/O when a host is partitioned or loses quorum? | Assumptions A5 and A7 | Fault test |
| Which SCSI-3 reservation commands does vSAN accept from a Linux guest? | Decides how far away the storage-enforced backend is | `sg_persist` matrix |
| How many VMs can share one disk? | Number of warm readers | Documentation, then test |
| What do VM stun, snapshot and vMotion do to a VM mid-write? | The pause case | Test |

## To request

| Area | Gap | Request |
| --- | --- | --- |
| vSAN | SCSI-3 reservations are supported for Windows clustering only | Support the reservation commands Linux fencing uses (register, reserve, preempt-and-abort) |
| VKS | Control plane size must be one or three | Allow two (or any number) when the datastore is external |
| VKS, VM Service, Cluster API | No way found to attach a shared disk to control plane VMs | Declare a shared disk on the control plane and keep it across rolling node replacement |
| VKS | Datastore is stacked etcd | An external-datastore option for control planes |
| vSphere | Shared-disk VMs restrict snapshots, storage vMotion and similar | Confirm which restrictions apply and whether VKS lifecycle operations depend on them |
| vSAN | A multi-writer disk accepts a write from any attached VM, however stale | Let a disk reject writes from a writer whose epoch is out of date, as the journals behind EKS, MemoryDB and Aurora do. duet could then reuse log space without asking vCenter whether a lost node is really gone |
| VKS, Cluster API | duet needs to know when a control plane VM is gone for good | A hook on machine deletion that runs once vCenter has removed the VM |

## Not a gap

The vSAN witness. It already exists in every two-host vSAN cluster and is
the only tiebreaker duet relies on.
