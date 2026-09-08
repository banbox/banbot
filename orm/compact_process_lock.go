package orm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// compactProcessLockRoot retains the legacy-config root for existing callers.
// Identity-aware callers should use CompactProcessLockRootForIdentity.
func compactProcessLockRoot() string {
	dataDir := config.GetDataDirSafe()
	identity := CanonicalStorageIdentityForType("", databaseURL(), dataDir, databaseType())
	if identity == "" {
		if dataDir == "" {
			return ""
		}
		identity = "data-dir:" + absoluteStoragePath(dataDir)
	}
	return compactProcessLockRootForIdentity(identity)
}

func compactProcessLockRootForIdentity(identity string) string {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(identity))
	return filepath.Join(os.TempDir(), "banbot", "compact", hex.EncodeToString(digest[:]))
}

// CompactProcessLockRootForIdentity maps an explicit storage identity to the
// process-lock root used by QuestDB compact and rewrite leases. Callers that
// already own an explicit Runtime identity should use this instead of the
// legacy-config-derived root.
func CompactProcessLockRootForIdentity(identity string) string {
	return compactProcessLockRootForIdentity(identity)
}

func compactProcessLockRootForAllocator(allocator *SIDAllocator) string {
	if allocator != nil && !allocator.legacyConfig {
		return allocator.sharedReservationRoot()
	}
	return compactProcessLockRootFn()
}

func databaseURL() string {
	if config.Database == nil {
		return ""
	}
	return config.Database.Url
}

func databaseType() string {
	if config.Database == nil {
		return ""
	}
	return config.Database.DbType
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
		closeErr := file.Close()
		return nil, false, errors.Join(err, closeErr)
	}
	release := func() error {
		unlockErr := unlockCompactProcessOSLock(file)
		closeErr := file.Close()
		if unlockErr != nil {
			return errors.Join(unlockErr, closeErr)
		}
		return closeErr
	}
	return release, true, nil
}

func acquireCompactProcessSharedLock(ctx context.Context, root, table string) (func() error, error) {
	return acquireCompactProcessLock(ctx, root, table, compactProcessLockShared)
}

func acquireCompactProcessExclusiveLock(ctx context.Context, root, table string) (func() error, error) {
	return acquireCompactProcessLock(ctx, root, table, compactProcessLockExclusive)
}

func acquireCompactProcessLock(ctx context.Context, root, table string, mode compactProcessLockMode) (func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		release, acquired, err := tryAcquireCompactProcessLock(root, table, mode)
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
