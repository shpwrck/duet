# Lab stand-ins

Until a two-host vSAN cluster is available, open-source pieces stand in for
it. Results from these are labelled as stand-in results; they say nothing
about vSAN.

**Nothing on this page has been built or run yet.** It is the plan.

| vSAN piece | Stand-in | Faithful in | Not faithful in |
| --- | --- | --- | --- |
| Multi-writer VMDK shared by two VMs | One raw disk image attached to two KVM guests as shareable, with host caching off | Two kernels, no shared page cache, real block I/O | One host, so no replication, no partition between hosts, different latency |
| Clustered VMDK with SCSI-3 reservations | A Linux LIO iSCSI target exporting one LUN to both guests | Real persistent reservations, including preempt-and-abort | The target is a third machine in the lab; in production its role is vSAN's |
| vSAN partition handling | Blocking one guest's access to the disk | The losing side loses I/O | vSAN also stops the VMs; here that is scripted |
| Host failure | Killing a guest | Process and kernel death | No storage rebuild |
| VM stun | Pausing a guest for longer than the lease | The pause case exactly | |

## Order of work

1. `duetd` able to run the existing protocol against a real block device
   with direct I/O and a real clock.
2. Two KVM guests on a shared raw disk: repeat the simulation's scenarios
   with real processes (pause, kill, slow I/O).
3. Baseline on the same two guests: stock etcd under Pacemaker with
   disk-based fencing. Measure recovery time. This is the milestone 1 gate.
4. iSCSI target: the reservation backend, and stock etcd with reservation
   fencing as a second baseline.

## What only vSAN can answer

Everything in the "To verify" table of [VCF-GAPS.md](VCF-GAPS.md). The
stand-ins exercise the protocol; they cannot confirm the assumptions the
protocol makes about the disk.
