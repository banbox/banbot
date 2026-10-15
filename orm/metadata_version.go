package orm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// reserveMetadataVersions reserves a contiguous microsecond range before any
// INSERT. The storage identity's OS lease and durable highwater coordinate
// writers on the same host, including restarts and WAL-invisible reservations.
// Writers on different hosts require an external single-writer owner; this is
// deliberately not a distributed clock. Existing replay timestamps stay intact.
func (q *Queries) reserveMetadataVersions(ctx context.Context, table string, count int, floor time.Time) (time.Time, error) {
	root := q.processLockRoot()
	if q.storage == nil && q.symbols != nil {
		root = compactProcessLockRootForAllocator(q.symbols.sidAllocator())
	}
	return q.reserveMetadataVersionsAtRoot(ctx, root, table, count, floor)
}

func (q *Queries) reserveMetadataVersionsAtRoot(ctx context.Context, root, table string, count int, floor time.Time) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return reserveQuestMetadataVersions(ctx, root, table, count, floor, func() (time.Time, error) {
		// An upgrade must stop old writers first. Drain their pending WAL before
		// treating max(ts) as the seed; new writers share the highwater lease.
		ok, err := compactWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
			_, applied, suspended, err := queryCompactWalFallback(ctx, q.db, table)
			if err != nil {
				return false, err
			}
			if suspended {
				return false, fmt.Errorf("metadata seed WAL is suspended: table=%s", table)
			}
			return applied, nil
		})
		if err != nil {
			return time.Time{}, err
		}
		if !ok {
			return time.Time{}, fmt.Errorf("metadata seed WAL visibility timeout: table=%s", table)
		}
		var micros int64
		sql := fmt.Sprintf("SELECT coalesce(cast(max(ts) as long), 0) FROM %s", quoteIdent(table))
		if err := q.db.QueryRow(ctx, sql).Scan(&micros); err != nil {
			return time.Time{}, fmt.Errorf("seed metadata version for %s: %w", table, err)
		}
		return time.UnixMicro(micros).UTC(), nil
	})
}

func reserveQuestMetadataVersions(ctx context.Context, root, table string, count int, floor time.Time, seed func() (time.Time, error)) (start time.Time, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if strings.TrimSpace(root) == "" {
		return time.Time{}, fmt.Errorf("metadata version reservation requires a storage identity")
	}
	if _, ok := compactTables[table]; !ok || count <= 0 {
		return time.Time{}, fmt.Errorf("invalid metadata version reservation: table=%q count=%d", table, count)
	}
	if err := ensureExSymbolRecoveryDir(root); err != nil {
		return time.Time{}, err
	}
	release, err := acquireCompactProcessExclusiveLock(ctx, root, "metadata_"+table)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { retErr = errors.Join(retErr, release()) }()
	path := filepath.Join(root, "metadata_"+table+".version")
	payload, err := os.ReadFile(path)
	var highwater int64
	if err == nil {
		highwater, err = strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid metadata highwater %s: %w", path, err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if seed != nil {
			observed, seedErr := seed()
			if seedErr != nil {
				return time.Time{}, seedErr
			}
			if !observed.IsZero() {
				highwater = observed.UnixMicro()
			}
		}
	} else {
		return time.Time{}, err
	}
	next := normalizeQuestTimestamp(time.Now().UTC()).UnixMicro()
	if !floor.IsZero() && floor.UnixMicro() > highwater {
		highwater = floor.UnixMicro()
	}
	if highwater == math.MaxInt64 {
		return time.Time{}, fmt.Errorf("metadata highwater exhausted for %s", table)
	}
	if next <= highwater {
		next = highwater + 1
	}
	if next > math.MaxInt64-int64(count-1) {
		return time.Time{}, fmt.Errorf("metadata version batch overflows for %s", table)
	}
	file, err := os.CreateTemp(root, "metadata-version-*")
	if err != nil {
		return time.Time{}, err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	writeErr := writeAndSyncExSymbolRecoveryFile(file, []byte(strconv.FormatInt(next+int64(count-1), 10)+"\n"))
	if err = errors.Join(writeErr, file.Close()); err != nil {
		return time.Time{}, err
	}
	if err := publishRecoveryFile(tmp, path); err != nil {
		return time.Time{}, err
	}
	if err := syncExSymbolRecoveryDir(root); err != nil {
		// Keep the published highwater on failure. A retry reserves beyond it,
		// even when the associated INSERT was never sent.
		return time.Time{}, err
	}
	return time.UnixMicro(next).UTC(), nil
}
