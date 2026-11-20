package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/shpwrck/duet/pkg/disk"
	"github.com/shpwrck/duet/pkg/trim"
)

// cmdRetire declares a node gone for good.
//
// A node that owned the log and was replaced may have one write still to
// come, and the space that write is aimed at is not reused until the node
// shows it has stopped. A node that restarts shows it by itself. A node
// that never runs again cannot, and the log eventually fills. This command
// is the statement, from outside, that it never will.
//
// It is only safe if it is true. Run it once the machine is known to be
// powered off or destroyed, for example after vCenter confirms the VM is
// gone. If the node is in fact alive and writes, acknowledged data can be
// destroyed.
func cmdRetire(args []string) error {
	fs := flag.NewFlagSet("retire", flag.ExitOnError)
	device := fs.String("device", "", "shared file or block device")
	id := fs.Int("node", -1, "id of the node that is gone for good")
	sure := fs.Bool("confirmed-gone", false, "required: the node's machine is powered off or destroyed and will not come back under this identity")
	direct := fs.Bool("direct", true, "use direct I/O")
	fs.Parse(args)
	if !*sure {
		return errors.New("retire: refusing without -confirmed-gone; if the node is still alive this can destroy acknowledged data")
	}
	dev, err := disk.OpenFile(*device, 0, *direct)
	if err != nil {
		return err
	}
	defer dev.Close()
	l, err := disk.Open(dev)
	if err != nil {
		return err
	}
	if *id < 0 || *id >= l.Nodes {
		return fmt.Errorf("retire: node must be between 0 and %d", l.Nodes-1)
	}
	if err := trim.DeclareDead(dev, l, *id); err != nil {
		return err
	}
	fmt.Printf("node %d declared gone; the log space it was holding can be reused\n", *id)
	return nil
}
