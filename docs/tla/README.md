# TLA+ model of ownership

`Duet.tla` models the part of duet that decides who may write and which
records count, one block read or write per step, following `pkg/lease` and
the reader in `pkg/node`. Its header says what it leaves out.

**It has not been run through the TLC model checker yet.** Treat it as a
precise description, not as a checked one.

It has already been useful. Writing the protocol down one block read at a
time turned up two faults that every test had passed over. Both come from
the same place: Disk Paxos never erases a ballot block, and duet does,
when a node withdraws a ballot.

1. The owner's check after a write, and a reader's after a read, looked at
   the next epoch slot before the ballot blocks. A node stalled between
   the two could find the blocks wiped clean again and acknowledge, or
   apply, a record that a new epoch had cut off. The slot is now read
   last.
2. A node that chose which epoch to stand for and then stalled could run
   a ballot for an epoch already decided, see no block that said so,
   choose itself, and carry on as owner although the table named another
   node. A ballot now consults the table after its survey and goes by the
   table at the end.

Each is a deterministic test in `pkg/node/stalled_test.go`. Neither was
found by 6,000 random schedules, nor, for the second, by 20,000 schedules
of the newer `TestRandomInterleavings`, which can stall any node anywhere.

To check the model, with `tla2tools.jar` from the TLA+ project:

    java -cp tla2tools.jar tlc2.TLC -config Fixed.cfg Duet.tla         # the code as it is; expect no violation
    java -cp tla2tools.jar tlc2.TLC -config SlotFirst.cfg Duet.tla     # fault 1 put back; expect AckedSurvive to fail
    java -cp tla2tools.jar tlc2.TLC -config NoTableCheck.cfg Duet.tla  # fault 2 put back; expect OneOwner to fail

If the two faulty configurations do not produce violations, the model is
too weak to see faults that are known to exist, and a clean result from `Fixed.cfg`
means little. The bounds in the `.cfg` files may need raising for that
(the known schedule needs three nodes and two ballots by one of them).
