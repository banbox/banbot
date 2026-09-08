package orm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
)

const exSymbolRecoveryVersion = 1

var (
	syncExSymbolRecoveryFile = func(file *os.File) error { return file.Sync() }
	syncExSymbolRecoveryDir  = syncDirectory
)

const (
	exSymbolRecoveryMarkerPrefix = "exsymbol-"
	exSymbolRecoveryMarkerSuffix = ".pending.json"

	// The shared ledger lives below the canonical storage lock root, rather
	// than below a runtime's recovery root. Its name and temporary-file prefix
	// stay outside readPendingExSymbolMarkers so the two durability domains do
	// not get mistaken for duplicate runtime markers.
	sidReservationLockName   = "exsymbol_sid"
	sidReservationMarkerName = "sid-reservation.pending.json"
	sidReservationTempPrefix = "sid-reservation-"
)

type exSymbolRecoveryRow struct {
	ID       int32     `json:"id"`
	Exchange string    `json:"exchange"`
	ExgReal  string    `json:"exg_real,omitempty"`
	Market   string    `json:"market"`
	Symbol   string    `json:"symbol"`
	WriteTS  time.Time `json:"write_ts,omitempty"`
	Inserted *bool     `json:"inserted,omitempty"`
	Combined bool      `json:"combined,omitempty"`
	ListMs   int64     `json:"list_ms,omitempty"`
	DelistMs int64     `json:"delist_ms,omitempty"`
	AggRules string    `json:"agg_rules,omitempty"`
}

type exSymbolRecoveryMarker struct {
	Version   int                   `json:"version"`
	CreatedAt time.Time             `json:"created_at"`
	Namespace string                `json:"namespace,omitempty"`
	Rows      []exSymbolRecoveryRow `json:"rows"`
}

func pendingExSymbolRows(arg []AddSymbolsParams, ids []int32) []exSymbolRecoveryRow {
	rows := make([]exSymbolRecoveryRow, len(arg))
	for i, item := range arg {
		rows[i] = exSymbolRecoveryRow{
			ID:       ids[i],
			Exchange: item.Exchange,
			ExgReal:  item.ExgReal,
			Market:   item.Market,
			Symbol:   item.Symbol,
			Combined: item.Combined,
			ListMs:   item.ListMs,
			DelistMs: item.DelistMs,
			AggRules: item.AggRules,
		}
	}
	return rows
}

// BindExSymbolRecoveryDir binds durable recovery markers to one SymbolState.
// Recovery files contain Runtime identity, so their directory is state-owned
// rather than allocator-owned and may differ between sibling runtimes.
func BindExSymbolRecoveryDir(state *SymbolState, dataDir string) error {
	if state == nil {
		return fmt.Errorf("cannot bind exsymbol recovery directory: symbol state is nil")
	}
	dataDir = filepath.Clean(strings.TrimSpace(dataDir))
	if dataDir == "" || dataDir == "." {
		return fmt.Errorf("cannot bind exsymbol recovery directory: data directory is not configured")
	}
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolve exsymbol recovery directory: %w", err)
	}
	root := filepath.Join(dataDir, "recovery")
	state.recoveryMu.Lock()
	if current := state.recoveryRoot; current != "" && current != root {
		state.recoveryMu.Unlock()
		return fmt.Errorf("cannot rebind exsymbol recovery directory from %s to %s", current, root)
	}
	state.recoveryRoot = root
	state.recoveryMu.Unlock()
	allocator := state.sidAllocator()
	allocator.bindStorage(dataDir)
	allocator.registerRecoveryRoot(root)
	return nil
}

func storageIdentityDigest(identity string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(identity)))
	return hex.EncodeToString(digest[:])
}

func exSymbolRecoveryRoot(state *SymbolState, legacy bool) (string, error) {
	if !legacy {
		if state == nil {
			return "", fmt.Errorf("cannot persist exsymbol recovery marker: explicit symbol state is nil")
		}
		state.recoveryMu.RLock()
		root := state.recoveryRoot
		state.recoveryMu.RUnlock()
		if root == "" {
			return "", fmt.Errorf("cannot persist exsymbol recovery marker: explicit symbol state has no recovery directory")
		}
		state.sidAllocator().registerRecoveryRoot(root)
		return root, nil
	}
	dataDir := config.GetDataDirSafe()
	if dataDir == "" {
		return "", fmt.Errorf("cannot persist exsymbol recovery marker: data directory is not configured")
	}
	root := filepath.Join(dataDir, "recovery")
	allocator := state.sidAllocator()
	allocator.bindStorage(dataDir)
	allocator.registerRecoveryRoot(root)
	return root, nil
}

func writePendingExSymbolMarker(root string, rows []exSymbolRecoveryRow) (string, error) {
	return writePendingExSymbolMarkerForNamespace(root, "", rows)
}

func writePendingExSymbolMarkerForNamespace(root, namespace string, rows []exSymbolRecoveryRow) (string, error) {
	marker := exSymbolRecoveryMarker{
		Version:   exSymbolRecoveryVersion,
		CreatedAt: time.Now().UTC(),
		Namespace: namespace,
		Rows:      rows,
	}
	return persistExSymbolRecoveryMarker(root, "", marker)
}

// persistExSymbolRecoveryMarker publishes a marker atomically. A non-empty
// target updates an existing marker; the published file is intentionally kept
// when the final directory sync fails so recovery remains discoverable.
func persistExSymbolRecoveryMarker(root, target string, marker exSymbolRecoveryMarker) (string, error) {
	return persistExSymbolRecoveryMarkerWithTempPrefix(root, target, marker,
		fmt.Sprintf("%s%d-", exSymbolRecoveryMarkerPrefix, os.Getpid()))
}

func persistExSymbolRecoveryMarkerWithTempPrefix(root, target string, marker exSymbolRecoveryMarker, tempPrefix string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("create exsymbol recovery directory: path is empty")
	}
	if !validExSymbolRecoveryMarker(marker) {
		return "", fmt.Errorf("validate exsymbol recovery marker: invalid marker")
	}
	if err := ensureExSymbolRecoveryDir(root); err != nil {
		return "", err
	}
	payload, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode exsymbol recovery marker: %w", err)
	}
	file, err := os.CreateTemp(root, fmt.Sprintf("%s%d-", tempPrefix, os.Getpid()))
	if err != nil {
		return "", fmt.Errorf("create exsymbol recovery marker: %w", err)
	}
	tmpPath := file.Name()
	writeErr := writeAndSyncExSymbolRecoveryFile(file, payload)
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("write exsymbol recovery marker: %w", writeErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("close exsymbol recovery marker: %w", closeErr)
	}
	if target == "" {
		target = tmpPath + exSymbolRecoveryMarkerSuffix
	}
	if err := os.Rename(tmpPath, target); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("publish exsymbol recovery marker: %w", err)
	}
	if err := syncExSymbolRecoveryDir(root); err != nil {
		return target, fmt.Errorf("sync published exsymbol recovery marker directory %s: %w", root, err)
	}
	return target, nil
}

func writeAndSyncExSymbolRecoveryFile(file *os.File, payload []byte) error {
	written, err := file.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	return syncExSymbolRecoveryFile(file)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for fsync: %w", path, err)
	}
	if err := dir.Sync(); err != nil {
		return errors.Join(fmt.Errorf("fsync directory %s: %w", path, err), dir.Close())
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close directory %s after fsync: %w", path, err)
	}
	return nil
}

func ensureExSymbolRecoveryDir(root string) error {
	root = filepath.Clean(root)
	missing := make([]string, 0, 1)
	for path := root; ; path = filepath.Dir(path) {
		info, err := os.Stat(path)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("create exsymbol recovery directory: %s is not a directory", path)
			}
			break
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("inspect exsymbol recovery directory %s: %w", path, err)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return fmt.Errorf("create exsymbol recovery directory: no existing parent for %s", root)
		}
		missing = append(missing, path)
	}
	if len(missing) == 0 {
		return nil
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("create exsymbol recovery directory: %w", err)
	}
	// Sync each newly-created directory's parent from the filesystem root down
	// so the recovery directory entry is durable even when its parent was new.
	for i := len(missing) - 1; i >= 0; i-- {
		parent := filepath.Dir(missing[i])
		if err := syncExSymbolRecoveryDir(parent); err != nil {
			return fmt.Errorf("sync created exsymbol recovery directory parent %s: %w", parent, err)
		}
	}
	return nil
}

func readPendingExSymbolMarker(path string) (exSymbolRecoveryMarker, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return exSymbolRecoveryMarker{}, fmt.Errorf("read exsymbol recovery marker %s: %w", path, err)
	}
	var marker exSymbolRecoveryMarker
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return exSymbolRecoveryMarker{}, fmt.Errorf("decode exsymbol recovery marker %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return exSymbolRecoveryMarker{}, fmt.Errorf("decode exsymbol recovery marker %s: trailing JSON value", path)
		}
		return exSymbolRecoveryMarker{}, fmt.Errorf("decode exsymbol recovery marker %s: %w", path, err)
	}
	if !validExSymbolRecoveryMarker(marker) {
		return exSymbolRecoveryMarker{}, fmt.Errorf("invalid exsymbol recovery marker %s", path)
	}
	return marker, nil
}

type pendingExSymbolMarker struct {
	path   string
	marker exSymbolRecoveryMarker
}

// readPendingExSymbolMarkers fails closed for every recovery-looking entry.
// A leftover temp file means a process may have stopped between writing and
// publishing a marker, so allocation must stop until an operator resolves it.
func readPendingExSymbolMarkers(root string) ([]pendingExSymbolMarker, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read exsymbol recovery directory: %w", err)
	}
	markers := make([]pendingExSymbolMarker, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), exSymbolRecoveryMarkerPrefix) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if !strings.HasSuffix(entry.Name(), exSymbolRecoveryMarkerSuffix) {
			return nil, fmt.Errorf("incomplete exsymbol recovery marker %s", path)
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("invalid exsymbol recovery marker %s: not a regular file", path)
		}
		marker, err := readPendingExSymbolMarker(path)
		if err != nil {
			return nil, err
		}
		markers = append(markers, pendingExSymbolMarker{path: path, marker: marker})
	}
	return markers, nil
}

// sidReservationRootForAllocator returns the same-host coordination root
// derived from storage identity. Runtime recovery markers remain durable under
// each runtime's recovery root; this root is only a compatibility fence for
// single-writer QuestDB deployments, never the cross-host SID authority.
func sidReservationRootForAllocator(allocator *SIDAllocator) string {
	if allocator == nil {
		return ""
	}
	if root := allocator.sharedReservationRoot(); root != "" {
		return root
	}
	namespace := strings.TrimSpace(allocator.Namespace())
	if namespace == "" {
		return ""
	}
	return CompactProcessLockRootForIdentity(namespace)
}

// acquireLocalSIDReservationLease is the compatibility fence for legacy
// callers, database test doubles, and QuestDB deployments without a shared
// SID registry. It is deliberately same-host only; multi-writer QuestDB
// deployments must bind a SymbolSIDRegistry to the runtime allocator.
func acquireLocalSIDReservationLease(ctx context.Context, allocator *SIDAllocator) (func() error, error) {
	if IsQuestDB && (allocator == nil || strings.TrimSpace(allocator.Namespace()) == "") {
		return nil, errs.NewMsg(core.ErrBadConfig,
			"QuestDB SID allocation requires a canonical shared storage identity; configure database.url or storage namespace")
	}
	root := sidReservationRootForAllocator(allocator)
	if root == "" && !IsQuestDB && compactProcessLockRootFn != nil {
		// Keep legacy allocators on the existing process-lock boundary. Explicit
		// namespaces use the identity-derived root above.
		root = compactProcessLockRootFn()
	}
	if root == "" {
		if IsQuestDB {
			return nil, errs.NewMsg(core.ErrBadConfig,
				"QuestDB SID allocation requires a shared coordination root for the canonical storage identity")
		}
		return func() error { return nil }, nil
	}
	if err := ensureExSymbolRecoveryDir(root); err != nil {
		return nil, fmt.Errorf("prepare SID reservation directory: %w", err)
	}
	// The lock root may have been created by an earlier process-lock open. Sync
	// its parent on every lease attempt so a prior failed directory sync cannot
	// silently become a durable lease boundary.
	if err := syncExSymbolRecoveryDir(filepath.Dir(root)); err != nil {
		return nil, fmt.Errorf("sync SID reservation directory parent %s: %w", filepath.Dir(root), err)
	}
	return acquireCompactProcessExclusiveLock(ctx, root, sidReservationLockName)
}

// acquireSIDReservationLease is kept for package-local compatibility tests.
// Registry-backed QuestDB writes use the shared database authority and do not
// use this local compatibility fence.
func acquireSIDReservationLease(ctx context.Context, allocator *SIDAllocator) (func() error, error) {
	return acquireLocalSIDReservationLease(ctx, allocator)
}

// readSharedSIDReservationMarker reads the one durable pending ledger for a
// canonical storage namespace. The lock is acquired by the caller before this
// function is used for allocation or reconciliation.
func readSharedSIDReservationMarker(allocator *SIDAllocator) (string, exSymbolRecoveryMarker, error) {
	root := sidReservationRootForAllocator(allocator)
	if root == "" {
		return "", exSymbolRecoveryMarker{}, nil
	}
	namespace := strings.TrimSpace(allocator.Namespace())
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return "", exSymbolRecoveryMarker{}, nil
	}
	if err != nil {
		return "", exSymbolRecoveryMarker{}, fmt.Errorf("read shared SID reservation directory %s: %w", root, err)
	}
	var markerPath string
	var marker exSymbolRecoveryMarker
	for _, entry := range entries {
		name := entry.Name()
		if name != sidReservationMarkerName && strings.HasPrefix(name, "sid-reservation") {
			return "", exSymbolRecoveryMarker{}, fmt.Errorf("incomplete shared SID reservation marker %s", filepath.Join(root, name))
		}
		if name != sidReservationMarkerName {
			continue
		}
		path := filepath.Join(root, name)
		if !entry.Type().IsRegular() {
			return "", exSymbolRecoveryMarker{}, fmt.Errorf("invalid shared SID reservation marker %s: not a regular file", path)
		}
		marker, err = readPendingExSymbolMarker(path)
		if err != nil {
			return "", exSymbolRecoveryMarker{}, err
		}
		markerPath = path
	}
	if markerPath != "" {
		if namespace == "" || marker.Namespace != namespace {
			return "", exSymbolRecoveryMarker{}, fmt.Errorf("shared SID reservation marker %s belongs to storage namespace %q, current namespace is %q", markerPath, marker.Namespace, namespace)
		}
		return markerPath, marker, nil
	}
	return "", exSymbolRecoveryMarker{}, nil
}

func sharedSIDReservationRows(allocator *SIDAllocator) ([]exSymbolRecoveryRow, error) {
	_, marker, err := readSharedSIDReservationMarker(allocator)
	if err != nil {
		return nil, err
	}
	if len(marker.Rows) == 0 {
		return nil, nil
	}
	reservations := make([]sidReservation, 0, len(marker.Rows))
	for _, row := range marker.Rows {
		reservations = append(reservations, sidReservation{
			key: exSymbolKey(row.Exchange, row.Market, row.Symbol),
			id:  row.ID,
		})
	}
	if err := allocator.reservePendingSIDBatch(reservations); err != nil {
		return nil, fmt.Errorf("reserve shared SID reservation marker: %w", err)
	}
	return append([]exSymbolRecoveryRow(nil), marker.Rows...), nil
}

// reconcileSharedSIDReservations promotes visible rows and removes the shared
// ledger only after every row has been verified. A visible SID with different
// metadata is a hard conflict: continuing could allocate another logical
// symbol against the same durable ID.
func reconcileSharedSIDReservations(ctx context.Context, q *Queries, state *SymbolState, allocator *SIDAllocator) error {
	path, marker, err := readSharedSIDReservationMarker(allocator)
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	if q == nil || q.db == nil {
		return fmt.Errorf("reconcile shared SID reservation marker %s: database handle is nil", path)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reservations := make([]sidReservation, 0, len(marker.Rows))
	for _, row := range marker.Rows {
		reservations = append(reservations, sidReservation{
			key: exSymbolKey(row.Exchange, row.Market, row.Symbol),
			id:  row.ID,
		})
	}
	if err := allocator.reservePendingSIDBatch(reservations); err != nil {
		return fmt.Errorf("reserve shared SID reservation marker: %w", err)
	}
	allVisible := true
	for _, expected := range marker.Rows {
		item, err := questExsymbolBySID(ctx, q, expected.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			allVisible = false
			continue
		}
		if err != nil {
			return fmt.Errorf("reconcile shared SID reservation marker %s sid %d: %w", path, expected.ID, err)
		}
		if !expected.matches(item) {
			return fmt.Errorf("reconcile shared SID reservation marker %s sid %d: metadata conflict", path, expected.ID)
		}
		if state.identitySet && (item.Exchange != state.identityExchange || item.Market != state.identityMarket) {
			// The shared ledger is storage-scoped, so a foreign runtime must
			// still verify WAL visibility before allowing the ledger to clear.
			if err := allocator.markSIDConfirmed(exSymbolKey(item.Exchange, item.Market, item.Symbol), item.ID); err != nil {
				return fmt.Errorf("confirm shared SID reservation marker %s sid %d: %w", path, item.ID, err)
			}
			continue
		}
		if err := state.validateReservedSymbol(item); err != nil {
			return fmt.Errorf("reconcile shared SID reservation marker %s sid %d: %w", path, item.ID, err)
		}
		if err := state.cacheExSymbolChecked(item); err != nil {
			return fmt.Errorf("cache shared SID reservation marker %s sid %d: %w", path, item.ID, err)
		}
		if err := allocator.markSIDConfirmed(exSymbolKey(item.Exchange, item.Market, item.Symbol), item.ID); err != nil {
			return fmt.Errorf("confirm shared SID reservation marker %s sid %d: %w", path, item.ID, err)
		}
	}
	if allVisible {
		if err := removePendingExSymbolMarker(path); err != nil {
			return fmt.Errorf("remove shared SID reservation marker %s: %w", path, err)
		}
	}
	return nil
}

func publishSharedSIDReservations(allocator *SIDAllocator, rows []exSymbolRecoveryRow) (string, error) {
	if allocator == nil || len(rows) == 0 {
		return "", nil
	}
	root := sidReservationRootForAllocator(allocator)
	if root == "" {
		return "", nil
	}
	path, marker, err := readSharedSIDReservationMarker(allocator)
	if err != nil {
		return "", err
	}
	if path == "" {
		path = filepath.Join(root, sidReservationMarkerName)
		marker = exSymbolRecoveryMarker{
			Version:   exSymbolRecoveryVersion,
			CreatedAt: time.Now().UTC(),
			Namespace: strings.TrimSpace(allocator.Namespace()),
		}
	}
	merged, overlap, err := mergeExSymbolRecoveryRows(marker.Rows, rows)
	if err != nil {
		return "", fmt.Errorf("merge shared SID reservation marker: %w", err)
	}
	if overlap && sameExSymbolRecoveryRows(marker.Rows, merged) {
		if err := syncExistingExSymbolRecoveryFile(path); err != nil {
			return path, fmt.Errorf("sync existing shared SID reservation marker %s: %w", path, err)
		}
		if err := syncExSymbolRecoveryDir(root); err != nil {
			return path, fmt.Errorf("sync existing shared SID reservation directory %s: %w", root, err)
		}
		return path, nil
	}
	marker.Rows = merged
	marker.Namespace = strings.TrimSpace(allocator.Namespace())
	marker.CreatedAt = time.Now().UTC()
	if !validExSymbolRecoveryMarker(marker) {
		return "", fmt.Errorf("validate shared SID reservation marker: invalid marker")
	}
	return persistExSymbolRecoveryMarkerWithTempPrefix(root, path, marker,
		fmt.Sprintf("%s%d-", sidReservationTempPrefix, os.Getpid()))
}

func removeSharedSIDReservations(allocator *SIDAllocator, rows []exSymbolRecoveryRow) error {
	if allocator == nil || len(rows) == 0 || sidReservationRootForAllocator(allocator) == "" {
		return nil
	}
	path, marker, err := readSharedSIDReservationMarker(allocator)
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	requested := make(map[string]exSymbolRecoveryRow, len(rows))
	for _, row := range rows {
		requested[exSymbolKey(row.Exchange, row.Market, row.Symbol)] = row
	}
	remaining := make([]exSymbolRecoveryRow, 0, len(marker.Rows))
	removed := 0
	for _, current := range marker.Rows {
		key := exSymbolKey(current.Exchange, current.Market, current.Symbol)
		want, ok := requested[key]
		if !ok {
			remaining = append(remaining, current)
			continue
		}
		if !sameExSymbolRecoveryRow(current, want) {
			return fmt.Errorf("shared SID reservation marker %s has conflicting metadata for %s", path, key)
		}
		removed++
	}
	if removed == 0 {
		return fmt.Errorf("shared SID reservation marker %s does not contain requested rows", path)
	}
	if len(remaining) == 0 {
		return removePendingExSymbolMarker(path)
	}
	marker.Rows = remaining
	marker.CreatedAt = time.Now().UTC()
	_, err = persistExSymbolRecoveryMarkerWithTempPrefix(filepath.Dir(path), path, marker,
		fmt.Sprintf("%s%d-", sidReservationTempPrefix, os.Getpid()))
	return err
}

// pendingExSymbolMarkerSIDs returns the unresolved reservations published by
// this recovery root. The allocator is process/database scoped, while marker
// ownership is root scoped; callers use this distinction to reject a retry
// from a different root before it can issue a duplicate INSERT.
func pendingExSymbolMarkerSIDs(root, namespace string) (map[string]int32, error) {
	markers, err := readPendingExSymbolMarkers(root)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int32)
	for _, pending := range markers {
		if pending.marker.Namespace != "" && pending.marker.Namespace != namespace {
			return nil, fmt.Errorf("exsymbol recovery marker %s belongs to storage namespace %q, current namespace is %q", pending.path, pending.marker.Namespace, namespace)
		}
		for _, row := range pending.marker.Rows {
			key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
			if current, ok := result[key]; ok && current != row.ID {
				return nil, fmt.Errorf("exsymbol recovery markers reserve logical symbol %s as both sid %d and sid %d", key, current, row.ID)
			}
			result[key] = row.ID
		}
	}
	return result, nil
}

func recoveryRootsForAllocator(allocator *SIDAllocator, current string) []string {
	roots := make([]string, 0, 1)
	seen := make(map[string]struct{})
	if allocator != nil {
		for _, root := range allocator.recoveryRootSnapshot() {
			if root == "" {
				continue
			}
			if _, ok := seen[root]; ok {
				continue
			}
			seen[root] = struct{}{}
			roots = append(roots, root)
		}
	}
	if current != "" {
		if _, ok := seen[current]; !ok {
			roots = append(roots, current)
		}
	}
	return roots
}

func findPendingExSymbolMarkerAcrossRoots(roots []string, namespace string, rows []exSymbolRecoveryRow) (string, error) {
	for _, root := range roots {
		path, err := findPendingExSymbolMarker(root, namespace, rows)
		if err != nil {
			return "", err
		}
		if path != "" {
			return path, nil
		}
	}
	return "", nil
}

// findPendingExSymbolMarker returns an existing marker only when it describes
// exactly the retry batch. Reusing it keeps an unresolved write idempotent and
// prevents repeated retries from accumulating duplicate markers.
func findPendingExSymbolMarker(root, namespace string, rows []exSymbolRecoveryRow) (string, error) {
	markers, err := readPendingExSymbolMarkers(root)
	if err != nil {
		return "", err
	}
	selected := make([]bool, len(markers))
	primary := -1
	for i, pending := range markers {
		if pending.marker.Namespace == namespace && recoveryRowsOverlap(pending.marker.Rows, rows) {
			primary = i
			selected[i] = true
			break
		}
	}
	if primary < 0 {
		return "", nil
	}

	// Merge every transitively-overlapping marker. A retry can span two
	// earlier batches (for example A then B, retried as A+B); updating only one
	// file would leave duplicate rows and make reconciliation fail closed.
	merged := append([]exSymbolRecoveryRow(nil), markers[primary].marker.Rows...)
	var mergeErr error
	merged, _, mergeErr = mergeExSymbolRecoveryRows(merged, rows)
	if mergeErr != nil {
		return "", fmt.Errorf("update exsymbol recovery marker %s: %w", markers[primary].path, mergeErr)
	}
	for {
		changed := false
		for i, pending := range markers {
			if selected[i] || pending.marker.Namespace != namespace {
				continue
			}
			candidate, overlap, err := mergeExSymbolRecoveryRows(merged, pending.marker.Rows)
			if err != nil {
				return "", fmt.Errorf("update exsymbol recovery marker %s: %w", pending.path, err)
			}
			if !overlap {
				continue
			}
			selected[i] = true
			merged = candidate
			changed = true
		}
		if !changed {
			break
		}
	}

	primaryMarker := markers[primary]
	if !sameExSymbolRecoveryRows(primaryMarker.marker.Rows, merged) {
		updated := primaryMarker.marker
		updated.Rows = merged
		if _, err := persistExSymbolRecoveryMarker(filepath.Dir(primaryMarker.path), primaryMarker.path, updated); err != nil {
			return "", err
		}
	} else {
		if err := syncExistingExSymbolRecoveryFile(primaryMarker.path); err != nil {
			return primaryMarker.path, fmt.Errorf("sync existing exsymbol recovery marker %s: %w", primaryMarker.path, err)
		}
		if err := syncExSymbolRecoveryDir(filepath.Dir(primaryMarker.path)); err != nil {
			return primaryMarker.path, fmt.Errorf("sync existing exsymbol recovery marker directory %s: %w", filepath.Dir(primaryMarker.path), err)
		}
	}
	for i, pending := range markers {
		if i == primary || !selected[i] {
			continue
		}
		if err := removePendingExSymbolMarker(pending.path); err != nil {
			return "", err
		}
	}
	return primaryMarker.path, nil
}

func recoveryRowsOverlap(a, b []exSymbolRecoveryRow) bool {
	keys := make(map[string]struct{}, len(a))
	sids := make(map[int32]struct{}, len(a))
	for _, row := range a {
		keys[exSymbolKey(row.Exchange, row.Market, row.Symbol)] = struct{}{}
		sids[row.ID] = struct{}{}
	}
	for _, row := range b {
		if _, ok := keys[exSymbolKey(row.Exchange, row.Market, row.Symbol)]; ok {
			return true
		}
		if _, ok := sids[row.ID]; ok {
			return true
		}
	}
	return false
}

func mergeExSymbolRecoveryRows(existing, requested []exSymbolRecoveryRow) ([]exSymbolRecoveryRow, bool, error) {
	merged := append([]exSymbolRecoveryRow(nil), existing...)
	byKey := make(map[string]exSymbolRecoveryRow, len(existing))
	bySID := make(map[int32]exSymbolRecoveryRow, len(existing))
	for _, row := range existing {
		byKey[exSymbolKey(row.Exchange, row.Market, row.Symbol)] = row
		bySID[row.ID] = row
	}
	overlap := false
	for _, row := range requested {
		key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
		if current, ok := byKey[key]; ok {
			overlap = true
			if !sameExSymbolRecoveryRow(current, row) {
				return nil, true, fmt.Errorf("logical symbol %s has conflicting metadata", key)
			}
			continue
		}
		if current, ok := bySID[row.ID]; ok {
			overlap = true
			return nil, true, fmt.Errorf("sid %d is already assigned to logical symbol %s", row.ID, exSymbolKey(current.Exchange, current.Market, current.Symbol))
		}
		merged = append(merged, row)
		byKey[key] = row
		bySID[row.ID] = row
	}
	return merged, overlap, nil
}

func reconcilePendingExSymbolMarkers(ctx context.Context, q *Queries, state *SymbolState, root string) error {
	if state == nil {
		return fmt.Errorf("reconcile exsymbol recovery markers: symbol state is nil")
	}
	allocator := state.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	return reconcilePendingExSymbolMarkersLocked(ctx, q, state, root)
}

func waitForExSymbolRecoveryRows(ctx context.Context, q *Queries, rows []exSymbolRecoveryRow) (bool, error) {
	if len(rows) == 0 {
		return true, nil
	}
	if q == nil || q.db == nil {
		return false, fmt.Errorf("wait for exsymbol recovery rows: database handle is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return questWaitForCondition(ctx, questReadAfterWriteTimeout, questReadAfterWritePollInterval, func() (bool, error) {
		for _, expected := range rows {
			item, err := questExsymbolBySID(ctx, q, expected.ID)
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if !expected.matches(item) {
				return false, fmt.Errorf("exsymbol recovery sid %d: metadata conflict", expected.ID)
			}
		}
		return true, nil
	})
}

func replayMissingExSymbolRows(ctx context.Context, q *Queries, marker exSymbolRecoveryMarker, rows []exSymbolRecoveryRow) error {
	if len(rows) == 0 {
		return nil
	}
	if q == nil || q.db == nil {
		return fmt.Errorf("replay exsymbol recovery rows: database handle is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fallbackTS := marker.CreatedAt.UTC()
	if fallbackTS.IsZero() {
		fallbackTS = time.Now().UTC()
	}
	for i, row := range rows {
		writeTS := row.WriteTS
		if writeTS.IsZero() {
			// Old markers have no write timestamp. Keep their replay timestamp
			// stable across recovery passes so repeated replays remain dedupable.
			offset := i
			for markerIndex, markerRow := range marker.Rows {
				if sameExSymbolRecoveryRow(markerRow, row) {
					offset = markerIndex
					break
				}
			}
			writeTS = fallbackTS.Add(time.Duration(offset) * time.Microsecond)
		}
		if _, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q
  (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
			row.ID, writeTS, row.Exchange, row.ExgReal, row.Market, row.Symbol,
			row.Combined, row.ListMs, row.DelistMs, row.AggRules); err != nil {
			return fmt.Errorf("replay exsymbol recovery sid %d: %w", row.ID, err)
		}
	}
	return nil
}

func markPendingExSymbolRowInserted(path string, row exSymbolRecoveryRow) error {
	if path == "" {
		return nil
	}
	marker, err := readPendingExSymbolMarker(path)
	if err != nil {
		return err
	}
	key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
	for i, current := range marker.Rows {
		if exSymbolKey(current.Exchange, current.Market, current.Symbol) != key {
			continue
		}
		if !sameExSymbolRecoveryRow(current, row) {
			return fmt.Errorf("exsymbol recovery marker %s has conflicting metadata for %s", path, key)
		}
		if current.Inserted != nil && *current.Inserted {
			return nil
		}
		inserted := true
		current.Inserted = &inserted
		marker.Rows[i] = current
		_, err := persistExSymbolRecoveryMarkerWithTempPrefix(filepath.Dir(path), path, marker,
			fmt.Sprintf("%s%d-", exSymbolRecoveryMarkerPrefix, os.Getpid()))
		return err
	}
	return fmt.Errorf("exsymbol recovery marker %s does not contain %s", path, key)
}

func exSymbolRecoveryRowNeedsReplay(row exSymbolRecoveryRow) bool {
	return row.Inserted == nil || !*row.Inserted
}

func reconcilePendingExSymbolMarkersLocked(ctx context.Context, q *Queries, state *SymbolState, root string) error {
	markers, err := readPendingExSymbolMarkers(root)
	if err != nil {
		return err
	}
	allocator := state.sidAllocator()
	namespace := allocator.Namespace()
	for _, pending := range markers {
		if pending.marker.Namespace != "" && pending.marker.Namespace != namespace {
			return fmt.Errorf("exsymbol recovery marker %s belongs to storage namespace %q, current namespace is %q", pending.path, pending.marker.Namespace, namespace)
		}
	}
	seenKeys := make(map[string]string)
	seenSIDs := make(map[int32]string)
	reservations := make([]sidReservation, 0)
	for _, pending := range markers {
		for _, expected := range pending.marker.Rows {
			key := exSymbolKey(expected.Exchange, expected.Market, expected.Symbol)
			if previous, ok := seenKeys[key]; ok {
				return fmt.Errorf("reconcile exsymbol recovery marker %s: logical symbol %s is also present in %s", pending.path, key, previous)
			}
			if previous, ok := seenSIDs[expected.ID]; ok {
				return fmt.Errorf("reconcile exsymbol recovery marker %s: sid %d is also present in %s", pending.path, expected.ID, previous)
			}
			seenKeys[key] = pending.path
			seenSIDs[expected.ID] = pending.path
			if err := state.validateReservedSymbol(expected.exSymbol()); err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s: %w", pending.path, err)
			}
			reservations = append(reservations, sidReservation{
				key: key,
				id:  expected.ID,
			})
		}
	}
	// Reserve every marker identity before checking WAL visibility. An
	// unresolved row is still an in-flight SID assignment and must not be
	// allocated again by a retry or sibling runtime.
	if err := allocator.reservePendingSIDBatch(reservations); err != nil {
		return fmt.Errorf("reserve exsymbol recovery marker SIDs: %w", err)
	}
	if q == nil || q.db == nil {
		return fmt.Errorf("reconcile exsymbol recovery markers: database handle is nil")
	}
	for _, pending := range markers {
		path, marker := pending.path, pending.marker
		confirmed := true
		foreign := false
		visible := make([]*ExSymbol, 0, len(marker.Rows))
		unresolved := make([]exSymbolRecoveryRow, 0, len(marker.Rows))
		replayable := make([]exSymbolRecoveryRow, 0, len(marker.Rows))
		for _, expected := range marker.Rows {
			if state.identitySet && (expected.Exchange != state.identityExchange || expected.Market != state.identityMarket) {
				confirmed = false
				foreign = true
				continue
			}
			item, err := questExsymbolBySID(ctx, q, expected.ID)
			if errors.Is(err, pgx.ErrNoRows) {
				confirmed = false
				unresolved = append(unresolved, expected)
				if exSymbolRecoveryRowNeedsReplay(expected) {
					replayable = append(replayable, expected)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: %w", path, expected.ID, err)
			}
			if !expected.matches(item) {
				return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: metadata conflict", path, expected.ID)
			}
			visible = append(visible, item)
		}
		if len(unresolved) > 0 {
			ready, err := waitForExSymbolRecoveryRows(ctx, q, unresolved)
			if err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s: wait for missing rows: %w", path, err)
			}
			if !ready && len(replayable) > 0 {
				if err := replayMissingExSymbolRows(ctx, q, marker, replayable); err != nil {
					return err
				}
				ready, err = waitForExSymbolRecoveryRows(ctx, q, unresolved)
				if err != nil {
					return fmt.Errorf("reconcile exsymbol recovery marker %s: wait after replay: %w", path, err)
				}
			}
			if ready {
				missingVisible := true
				for _, expected := range unresolved {
					item, err := questExsymbolBySID(ctx, q, expected.ID)
					if errors.Is(err, pgx.ErrNoRows) {
						missingVisible = false
						continue
					}
					if err != nil {
						return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: %w", path, expected.ID, err)
					}
					if !expected.matches(item) {
						return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: metadata conflict", path, expected.ID)
					}
					visible = append(visible, item)
				}
				if missingVisible && !foreign {
					confirmed = true
				}
			}
		}
		for _, item := range visible {
			if err := state.validateReservedSymbol(item); err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: %w", path, item.ID, err)
			}
			if err := allocator.markSIDConfirmed(exSymbolKey(item.Exchange, item.Market, item.Symbol), item.ID); err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: %w", path, item.ID, err)
			}
			if err := state.cacheExSymbolChecked(item); err != nil {
				return fmt.Errorf("reconcile exsymbol recovery marker %s sid %d: %w", path, item.ID, err)
			}
		}
		if confirmed {
			if err := removePendingExSymbolMarker(path); err != nil {
				return err
			}
			if err := removeSharedSIDReservations(allocator, marker.Rows); err != nil {
				return fmt.Errorf("remove shared exsymbol recovery rows: %w", err)
			}
		}
	}
	return nil
}

func validExSymbolRecoveryMarker(marker exSymbolRecoveryMarker) bool {
	if marker.Version != exSymbolRecoveryVersion || marker.CreatedAt.IsZero() || len(marker.Rows) == 0 {
		return false
	}
	byKey := make(map[string]exSymbolRecoveryRow, len(marker.Rows))
	bySID := make(map[int32]string, len(marker.Rows))
	for _, row := range marker.Rows {
		if row.ID <= 0 || row.Exchange == "" || row.Market == "" || row.Symbol == "" {
			return false
		}
		key := exSymbolKey(row.Exchange, row.Market, row.Symbol)
		if _, ok := byKey[key]; ok {
			return false
		}
		if _, ok := bySID[row.ID]; ok {
			return false
		}
		byKey[key] = row
		bySID[row.ID] = key
	}
	return true
}

func (r exSymbolRecoveryRow) exSymbol() *ExSymbol {
	return &ExSymbol{
		ID:       r.ID,
		Exchange: r.Exchange,
		ExgReal:  r.ExgReal,
		Market:   r.Market,
		Symbol:   r.Symbol,
		Combined: r.Combined,
		ListMs:   r.ListMs,
		DelistMs: r.DelistMs,
		AggRules: r.AggRules,
	}
}

func sameExSymbolRecoveryRow(a, b exSymbolRecoveryRow) bool {
	return a.ID == b.ID && a.Exchange == b.Exchange && a.ExgReal == b.ExgReal && a.Market == b.Market &&
		a.Symbol == b.Symbol && a.Combined == b.Combined && a.ListMs == b.ListMs && a.DelistMs == b.DelistMs &&
		a.AggRules == b.AggRules
}

func sameExSymbolRecoveryRows(a, b []exSymbolRecoveryRow) bool {
	if len(a) != len(b) {
		return false
	}
	byKey := make(map[string]exSymbolRecoveryRow, len(a))
	for _, row := range a {
		byKey[exSymbolKey(row.Exchange, row.Market, row.Symbol)] = row
	}
	if len(byKey) != len(a) {
		return false
	}
	for _, row := range b {
		previous, ok := byKey[exSymbolKey(row.Exchange, row.Market, row.Symbol)]
		if !ok || !sameExSymbolRecoveryRow(previous, row) {
			return false
		}
	}
	return true
}

func (r exSymbolRecoveryRow) matches(item *ExSymbol) bool {
	return item != nil && r.ID == item.ID && r.Exchange == item.Exchange && r.ExgReal == item.ExgReal &&
		r.Market == item.Market && r.Symbol == item.Symbol && r.Combined == item.Combined &&
		r.ListMs == item.ListMs && r.DelistMs == item.DelistMs && r.AggRules == item.AggRules
}

func removePendingExSymbolMarker(path string) error {
	if path == "" {
		return nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read exsymbol recovery marker %s before removal: %w", path, err)
	}
	if err := syncExistingExSymbolRecoveryFile(path); err != nil {
		return fmt.Errorf("sync exsymbol recovery marker %s before removal: %w", path, err)
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("remove exsymbol recovery marker %s: %w", path, err)
	}
	if err := syncExSymbolRecoveryDir(filepath.Dir(path)); err != nil {
		if restoreErr := restoreExSymbolRecoveryMarker(path, payload); restoreErr != nil {
			return fmt.Errorf("sync removed exsymbol recovery marker directory %s: %w (restore marker: %v)", filepath.Dir(path), err, restoreErr)
		}
		return fmt.Errorf("sync removed exsymbol recovery marker directory %s: %w", filepath.Dir(path), err)
	}
	return nil
}

// removePendingExSymbolMarkerRows removes only rows that have been verified.
// A marker may contain other unresolved rows after overlapping retries were
// merged; deleting the whole file would discard their recovery information.
func removePendingExSymbolMarkerRows(path string, rows []exSymbolRecoveryRow) error {
	if path == "" || len(rows) == 0 {
		return nil
	}
	marker, err := readPendingExSymbolMarker(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	requested := make(map[string]exSymbolRecoveryRow, len(rows))
	for _, row := range rows {
		requested[exSymbolKey(row.Exchange, row.Market, row.Symbol)] = row
	}
	remaining := make([]exSymbolRecoveryRow, 0, len(marker.Rows))
	removed := 0
	for _, current := range marker.Rows {
		key := exSymbolKey(current.Exchange, current.Market, current.Symbol)
		want, ok := requested[key]
		if !ok {
			remaining = append(remaining, current)
			continue
		}
		if !sameExSymbolRecoveryRow(current, want) {
			return fmt.Errorf("exsymbol recovery marker %s has conflicting metadata for %s", path, key)
		}
		removed++
	}
	if removed == 0 {
		return fmt.Errorf("exsymbol recovery marker %s does not contain requested rows", path)
	}
	if len(remaining) == 0 {
		return removePendingExSymbolMarker(path)
	}
	marker.Rows = remaining
	marker.CreatedAt = time.Now().UTC()
	_, err = persistExSymbolRecoveryMarkerWithTempPrefix(filepath.Dir(path), path, marker,
		fmt.Sprintf("%s%d-", exSymbolRecoveryMarkerPrefix, os.Getpid()))
	return err
}

func syncExistingExSymbolRecoveryFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	syncErr := syncExSymbolRecoveryFile(file)
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func restoreExSymbolRecoveryMarker(path string, payload []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("recreate exsymbol recovery marker: %w", err)
	}
	if err := writeAndSyncExSymbolRecoveryFile(file, payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("rewrite exsymbol recovery marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close restored exsymbol recovery marker: %w", err)
	}
	if err := syncExSymbolRecoveryDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync restored exsymbol recovery marker directory: %w", err)
	}
	return nil
}
