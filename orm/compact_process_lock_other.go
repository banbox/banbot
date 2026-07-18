//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package orm

import (
	"fmt"
	"os"
)

func tryCompactProcessOSLock(_ *os.File, _ bool) (bool, error) {
	return false, fmt.Errorf("cross-process compact locks are unsupported on this platform")
}

func unlockCompactProcessOSLock(_ *os.File) error {
	return nil
}
