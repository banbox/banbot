package config

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
)

// Configuration loads are read-only. Explicit edits share one atomic boundary.
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

// LoadUnifiedConfigs loads a configuration chain without changing source files.
func LoadUnifiedConfigs(paths []string, showLog bool) (*UnifiedConfig, *errs.Error) {
	return ParseUnifiedConfigs(paths, showLog)
}

func loadUnifiedSources(paths []string, inline [][]byte, inlineNames []string, showLog bool, metadata ...*loadedConfigMetadata) (*UnifiedConfig, *errs.Error) {
	raws := make([][]byte, len(paths))
	names := make([]string, len(paths))
	for i, path := range paths {
		resolved, err := normalizedConfigPath(path)
		if err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		raw, err := os.ReadFile(resolved)
		if err != nil {
			return nil, errs.NewFull(core.ErrIOReadFail, err, "Read %s Fail", path)
		}
		raws[i], names[i] = raw, resolved
		if showLog {
			log.Info("Using " + resolved)
		}
	}
	return parseUnifiedLayers(append(raws, inline...), append(names, inlineNames...), metadata...)
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
