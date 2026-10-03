package config

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
)

// Cleanup plan: keep the existing in-memory importer and overlay semantics;
// add one shared file-write boundary, prove the full candidate before writes,
// and regression-test failures, competing edits, and interrupted retries.
var configWriteLocks sync.Map

func normalizedConfigPath(path string) (string, error) {
	if strings.HasPrefix(path, "$") || strings.HasPrefix(path, "@") {
		dir := DataDir
		if dir == "" {
			dir = ResolveDataDir("")
		}
		if dir == "" {
			return "", fmt.Errorf("DataDir is required to resolve %s", path)
		}
		path = filepath.Join(dir, strings.TrimLeft(path, "$@\\/"))
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		abs = resolved
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func lockConfigPaths(paths []string) func() {
	keys := slices.Clone(paths)
	if runtime.GOOS == "windows" {
		for i := range keys {
			keys[i] = strings.ToLower(keys[i])
		}
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	locks := make([]*sync.Mutex, 0, len(keys))
	for _, key := range keys {
		value, _ := configWriteLocks.LoadOrStore(key, &sync.Mutex{})
		lock := value.(*sync.Mutex)
		lock.Lock()
		locks = append(locks, lock)
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}
}

func configPathKey(path string) string {
	if runtime.GOOS == "windows" {
		return strings.ToLower(path)
	}
	return path
}

// LoadUnifiedConfigs migrates file inputs and rereads the committed v2 chain.
// ParseUnifiedConfigs remains the read-only inspection API.
func LoadUnifiedConfigs(paths []string, showLog bool) (*UnifiedConfig, *errs.Error) {
	return loadUnifiedSources(paths, nil, nil, showLog, nil)
}

// migrationHook is local fault injection; callers cannot bypass validation.
type migrationHook func(stage, path string) error

func loadUnifiedSources(paths []string, inline [][]byte, inlineNames []string, showLog bool, hook migrationHook, metadata ...*loadedConfigMetadata) (*UnifiedConfig, *errs.Error) {
	fail := func(err error) (*UnifiedConfig, *errs.Error) { return nil, errs.New(core.ErrBadConfig, err) }
	resolved := make([]string, len(paths))
	for i, path := range paths {
		var err error
		resolved[i], err = normalizedConfigPath(path)
		if err != nil {
			return fail(err)
		}
	}
	unlock := lockConfigPaths(resolved)
	defer unlock()
	raws := make([][]byte, len(paths))
	candidates := make([][]byte, len(paths))
	imports := make([]*YAMLImport, len(paths))
	modes := make([]os.FileMode, len(paths))
	var fileLocks []*os.File
	defer func() {
		for _, lock := range fileLocks {
			releaseConfigFileLock(lock)
		}
	}()
	for i, path := range resolved {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fail(fmt.Errorf("%s: %w", path, err))
		}
		candidate, err := ImportV1YAML(raw, path)
		if err != nil {
			return fail(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fail(err)
		}
		if !info.Mode().IsRegular() {
			return fail(fmt.Errorf("%s: configuration must be a regular file", path))
		}
		raws[i], candidates[i], imports[i], modes[i] = raw, candidate.YAML, candidate, info.Mode().Perm()
	}
	// Acquire OS locks in the same order across processes and only once for
	// duplicate paths. A dead process releases its lock without stale recovery.
	var changedPaths []string
	seenPaths := make(map[string]bool)
	for i, path := range resolved {
		if imports[i].Changed && !seenPaths[configPathKey(path)] {
			changedPaths = append(changedPaths, path)
			seenPaths[configPathKey(path)] = true
		}
	}
	slices.SortFunc(changedPaths, func(a, b string) int { return strings.Compare(configPathKey(a), configPathKey(b)) })
	changedPaths = slices.Compact(changedPaths)
	for _, path := range changedPaths {
		info, err := os.Stat(path)
		if err != nil {
			return fail(err)
		}
		dirInfo, err := os.Stat(filepath.Dir(path))
		if err != nil {
			return fail(err)
		}
		if info.Mode().Perm()&0222 == 0 || dirInfo.Mode().Perm()&0222 == 0 {
			return fail(fmt.Errorf("%s: read-only configuration or directory; migration requires write access", path))
		}
		lock, err := acquireConfigFileLock(path)
		if err != nil {
			return fail(fmt.Errorf("%s: exclusive migration lock: %w", path, err))
		}
		fileLocks = append(fileLocks, lock)
	}
	for i, path := range resolved {
		if err := verifySource(path, raws[i]); err != nil {
			return fail(err)
		}
	}
	names := append(slices.Clone(resolved), inlineNames...)
	before, err := parseUnifiedLayers(append(slices.Clone(raws), inline...), names)
	if err != nil {
		return nil, err
	}
	after, err := parseUnifiedLayers(append(slices.Clone(candidates), inline...), names)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before, after) {
		return fail(fmt.Errorf("configuration candidate chain is not equivalent"))
	}
	for _, meta := range metadata {
		if meta.preflight != nil {
			if err := meta.preflight(after); err != nil {
				return fail(err)
			}
		}
	}
	// Each prefix must also be equivalent; per-layer importer already proves
	// syntax values and prevents a later overlay from hiding reinterpretation.
	committed := make(map[string]bool)
	for i, path := range resolved {
		if !imports[i].Changed {
			continue
		}
		if committed[configPathKey(path)] {
			continue
		}
		if err := invokeMigrationHook(hook, "before-backup", path); err != nil {
			return fail(err)
		}
		if err := verifySource(path, raws[i]); err != nil {
			return fail(err)
		}
		backup, err := backupConfig(path, raws[i], modes[i])
		if err != nil {
			return fail(fmt.Errorf("%s: backup: %w", path, err))
		}
		log.Info("Configuration migration backup: " + backup)
		if err := invokeMigrationHook(hook, "before-replace", path); err != nil {
			return fail(err)
		}
		if err := replaceConfig(path, raws[i], candidates[i], modes[i]); err != nil {
			return fail(err)
		}
		committed[configPathKey(path)] = true
		if err := invokeMigrationHook(hook, "after-replace", path); err != nil {
			return fail(err)
		}
	}
	for i, path := range resolved {
		if showLog {
			log.Info("Using " + path)
		}
		if err := invokeMigrationHook(hook, "before-reread", path); err != nil {
			return fail(err)
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return fail(fmt.Errorf("%s: reread: %w", path, readErr))
		}
		if sha256.Sum256(raw) != sha256.Sum256(candidates[i]) {
			return fail(fmt.Errorf("%s: configuration changed during migration reread; backups retained", path))
		}
		raws[i] = raw
	}
	result, err := parseUnifiedLayers(append(raws, inline...), names, metadata...)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(result, after) {
		return fail(fmt.Errorf("configuration reread chain differs from candidate; backups retained"))
	}
	return result, nil
}

func invokeMigrationHook(hook migrationHook, stage, path string) error {
	if hook != nil {
		if err := hook(stage, path); err != nil {
			return fmt.Errorf("%s: %s: %w", path, stage, err)
		}
	}
	return nil
}

func verifySource(path string, expected []byte) error {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) && expected == nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: source reread: %w", path, err)
	}
	if expected == nil {
		return fmt.Errorf("%s: source edit conflict; expected a new file", path)
	}
	if sha256.Sum256(raw) != sha256.Sum256(expected) {
		return fmt.Errorf("%s: source edit conflict; configuration not overwritten", path)
	}
	return nil
}

func backupConfig(path string, raw []byte, mode os.FileMode) (string, error) {
	file, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".bak."+time.Now().UTC().Format("20060102T150405")+".*")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := writeConfigFile(file, raw, mode); err != nil {
		return name, err
	}
	if err := preserveConfigPermissions(path, name); err != nil {
		return name, err
	}
	actual, err := os.ReadFile(name)
	if err != nil {
		return name, err
	}
	if !bytes.Equal(actual, raw) {
		return name, fmt.Errorf("backup verification failed")
	}
	return name, nil
}

func writeConfigFile(file *os.File, raw []byte, mode os.FileMode) error {
	defer file.Close()
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func replaceConfig(path string, expected, candidate []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".migrate.*")
	if err != nil {
		return fmt.Errorf("%s: prepare replacement: %w", path, err)
	}
	name := file.Name()
	defer os.Remove(name)
	if err := writeConfigFile(file, candidate, mode); err != nil {
		return fmt.Errorf("%s: write replacement: %w", path, err)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		if err := preserveConfigPermissions(path, name); err != nil {
			return fmt.Errorf("%s: preserve permissions: %w", path, err)
		}
	}
	actual, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, candidate) {
		return fmt.Errorf("%s: replacement verification failed", path)
	}
	if err := verifySource(path, expected); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("%s: atomic replacement: %w", path, err)
	}
	return syncConfigDir(filepath.Dir(path))
}

// WriteConfigAtomic is the shared application-edit boundary. expected must be
// the bytes originally read by the editor, so stale updates fail explicitly.
func WriteConfigAtomic(path string, expected, candidate []byte) *errs.Error {
	resolved, err := normalizedConfigPath(path)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	unlock := lockConfigPaths([]string{resolved})
	defer unlock()
	lock, err := acquireConfigFileLock(resolved)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	defer releaseConfigFileLock(lock)
	info, err := os.Stat(resolved)
	mode := os.FileMode(0600)
	if err != nil && !(os.IsNotExist(err) && expected == nil) {
		return errs.New(core.ErrBadConfig, err)
	}
	if info != nil {
		mode = info.Mode().Perm()
	}
	if err := replaceConfig(resolved, expected, candidate, mode); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	return nil
}
