//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package biz

import (
	"golang.org/x/sys/unix"
	"os"
)

func lockHubJournal(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
func unlockHubJournal(file *os.File) error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }
