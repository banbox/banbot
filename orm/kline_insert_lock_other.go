//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package orm

import (
	"fmt"
	"os"
)

func tryKlineInsertOSLock(_ *os.File) (bool, error) {
	return false, fmt.Errorf("cross-process kline insert locks are unsupported on this platform")
}

func unlockKlineInsertOSLock(_ *os.File) error {
	return nil
}
