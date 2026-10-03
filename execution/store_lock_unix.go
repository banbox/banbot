//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package execution

import (
	"golang.org/x/sys/unix"
	"os"
)

func lockStoreFile(file *os.File) error   { return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB) }
func unlockStoreFile(file *os.File) error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }
