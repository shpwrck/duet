//go:build linux

package disk

import "syscall"

const oDirect = syscall.O_DIRECT
