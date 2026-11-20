# TLA+ model of ownership

`Duet.tla` models the part of duet that decides who may write and which
records count, one block read or write per step, following `pkg/lease` and
the reader in `pkg/node`. Its header says what it leaves out.

**It has not been run through the TLC model checker yet.** Treat it as a
precise description, not as a checked one.

It has already been useful. Writing it down, with the owner's check as
separate reads, showed that the order of those reads mattered and that the
code had them the wrong way round: a node stalled between reading the next
epoch slot and reading the ballot blocks could acknowledge (or, as a
reader, apply) a record that a new epoch had cut off. That schedule is now
a test, `TestStalledBetweenTheReadsOfTheCheck` in `pkg/node`, and the code
reads the slot last.

To check it, with `tla2tools.jar` from the TLA+ project:

    java -cp tla2tools.jar tlc2.TLC -config Fixed.cfg Duet.tla      # the code as it is; expect no violation
    java -cp tla2tools.jar tlc2.TLC -config SlotFirst.cfg Duet.tla  # the old order; expect AckedSurvive to fail

If `SlotFirst.cfg` does not produce a violation, the model is too weak to
see the fault that is known to exist, and a clean result from `Fixed.cfg`
means little. The bounds in the `.cfg` files may need raising for that
(the known schedule needs three nodes and two ballots by one of them).
