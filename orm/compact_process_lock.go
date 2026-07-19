package orm

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/banbox/banbot/config"
)

const compactProcessLockPollInterval = 10 * time.Millisecond

type compactProcessLockMode uint8

const (
	compactProcessLockShared compactProcessLockMode = iota
	compactProcessLockExclusive
)

var (
	compactProcessLockRoots  sync.Map
	compactProcessLockRootFn = compactProcessLockRoot
)

func compactProcessLockRoot() string {
	dataDir := config.GetDataDirSafe()
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, "locks", "compact")
}

func ensureCompactProcessLockRoot(root string) error {
	if root == "" {
		return nil
	}
	if _, ok := compactProcessLockRoots.Load(root); ok {
		return nil
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	compactProcessLockRoots.Store(root, struct{}{})
	return nil
}

func tryAcquireCompactProcessLock(root, table string, mode compactProcessLockMode) (func() error, bool, error) {
	if root == "" {
		return func() error { return nil }, true, nil
	}
	if err := ensureCompactProcessLockRoot(root); err != nil {
		return nil, false, err
	}
	file, err := os.OpenFile(filepath.Join(root, table+".lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, false, err
	}
	locked, err := tryCompactProcessOSLock(file, mode == compactProcessLockExclusive)
	if err != nil || !locked {
		_ = file.Close()
		return nil, false, err
	}
	release := func() error {
		unlockErr := unlockCompactProcessOSLock(file)
		closeErr := file.Close()
		if unlockErr != nil {
			return unlockErr
		}
		return closeErr
	}
	return release, true, nil
}

func acquireCompactProcessSharedLock(ctx context.Context, root, table string) (func() error, error) {
	for {
		release, acquired, err := tryAcquireCompactProcessLock(root, table, compactProcessLockShared)
		if err != nil {
			return nil, err
		}
		if acquired {
			return release, nil
		}
		timer := time.NewTimer(compactProcessLockPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func tryAcquireCompactProcessExclusiveLock(root, table string) (func() error, bool, error) {
	return tryAcquireCompactProcessLock(root, table, compactProcessLockExclusive)
}
