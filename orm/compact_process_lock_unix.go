//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package orm

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func tryCompactProcessOSLock(file *os.File, exclusive bool) (bool, error) {
	mode := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		mode = unix.LOCK_EX | unix.LOCK_NB
	}
	err := unix.Flock(int(file.Fd()), mode)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	return err == nil, err
}

func unlockCompactProcessOSLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
