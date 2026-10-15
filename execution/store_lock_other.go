//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package execution

import (
	"errors"
	"os"
)

func lockStoreFile(*os.File) error {
	return errors.New("execution: account process lease unavailable on this platform")
}
func unlockStoreFile(*os.File) error { return nil }
