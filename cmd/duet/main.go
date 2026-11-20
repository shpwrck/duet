// Command duet runs and inspects a duet node.
//
//	duet format  -device PATH ...     write a fresh superblock
//	duet run     -device PATH -node N run one node
//	duet dump    -device PATH         replay the log and print a summary
//	duet load    -endpoints ...       write through running nodes, record acks
//	duet verify  -endpoints ... -acks check nodes agree and no ack was lost
//
// The HTTP interface served by "run" exists only so the protocol can be
// exercised by real processes on a real disk. It is not the etcd API and
// will be replaced by it.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "format":
		err = cmdFormat(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "dump":
		err = cmdDump(os.Args[2:])
	case "load":
		err = cmdLoad(os.Args[2:])
	case "verify":
		err = cmdVerify(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "duet:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: duet {format|run|dump|load|verify} [flags]")
	os.Exit(2)
}
