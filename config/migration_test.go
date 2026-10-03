package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func migrationFixture(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func configBackups(t *testing.T, path string) []string {
	t.Helper()
	paths, err := filepath.Glob(path + ".bak.*")
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestMigrationExactBackupPermissionsIdempotence(t *testing.T) {
	raw := []byte("# source expressions\r\nrun_policy: [{name: Demo, custom: {empty: [], disabled: false}}]\r\nwallet_amounts: {USDT: 100}\r\n")
	path := migrationFixture(t, "config.yml", string(raw))
	before, err := ParseConfigs([]string{path}, false)
	if err != nil {
		t.Fatal(err)
	}
	u, err := LoadUnifiedConfigs([]string{path}, false)
	if err != nil {
		t.Fatal(err)
	}
	after, err := u.TimeSeriesConfig()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy behavior changed: %v", err)
	}
	backups := configBackups(t, path)
	if len(backups) != 1 {
		t.Fatalf("backups: %v", backups)
	}
	backup, err2 := os.ReadFile(backups[0])
	if err2 != nil || !bytes.Equal(raw, backup) {
		t.Fatal("backup is not exact source bytes")
	}
	for _, item := range []string{path, backups[0]} {
		info, err := os.Stat(item)
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
			t.Fatalf("permissions changed: %v", info.Mode())
		}
	}
	converted, _ := os.ReadFile(path)
	if !bytes.Equal(bytes.Replace(converted, []byte("config_version: 2\r\n"), nil, 1), raw) {
		t.Fatal("migration expanded source")
	}
	if _, err := LoadUnifiedConfigs([]string{path}, false); err != nil {
		t.Fatal(err)
	}
	if len(configBackups(t, path)) != 1 {
		t.Fatal("v2 was backed up again")
	}
}

func TestMigrationValidatesEntireChainBeforeAnyBackup(t *testing.T) {
	first := migrationFixture(t, "first.yml", "run_policy: [{name: Demo}]\n")
	last := migrationFixture(t, "last.yml", "config_version: 2\nrun_policy: [{name: CS, engine: factor}, {name: Other, engine: factor}]\n")
	if _, err := LoadUnifiedConfigs([]string{first, last}, false); err == nil {
		t.Fatal("accepted incomplete shared budgets")
	}
	raw, _ := os.ReadFile(first)
	if string(raw) != "run_policy: [{name: Demo}]\n" || len(configBackups(t, first)) != 0 {
		t.Fatal("invalid chain changed its first file")
	}
}

func TestMigrationFaultsPreserveSourceAndRetry(t *testing.T) {
	for _, stage := range []string{"before-backup", "before-replace", "after-replace", "before-reread"} {
		t.Run(stage, func(t *testing.T) {
			path := migrationFixture(t, "config.yml", "run_policy: [{name: Demo}]\n")
			_, err := loadUnifiedSources([]string{path}, nil, nil, false, func(at, _ string) error {
				if at == stage {
					return errors.New("injected interruption")
				}
				return nil
			})
			if err == nil {
				t.Fatal("fault did not fail load")
			}
			raw, _ := os.ReadFile(path)
			if (stage == "before-backup" || stage == "before-replace") && string(raw) != "run_policy: [{name: Demo}]\n" {
				t.Fatal("failed prepare changed source")
			}
			if _, err := LoadUnifiedConfigs([]string{path}, false); err != nil {
				t.Fatal(err)
			}
			if len(configBackups(t, path)) == 0 {
				t.Fatal("retry lost original backup")
			}
		})
	}
}

func TestMigrationEditConflictsAreNotOverwritten(t *testing.T) {
	for _, stage := range []string{"before-backup", "before-replace", "before-reread"} {
		t.Run(stage, func(t *testing.T) {
			path := migrationFixture(t, "config.yml", "run_policy: [{name: Original}]\n")
			edited := []byte("config_version: 2\nrun_policy: [{name: Editor}]\n")
			_, err := loadUnifiedSources([]string{path}, nil, nil, false, func(at, path string) error {
				if at == stage {
					return os.WriteFile(path, edited, 0600)
				}
				return nil
			})
			if err == nil || (!strings.Contains(err.Error(), "conflict") && !strings.Contains(err.Error(), "reread")) {
				t.Fatalf("missing conflict: %v", err)
			}
			actual, _ := os.ReadFile(path)
			if !bytes.Equal(actual, edited) {
				t.Fatal("external edit was overwritten")
			}
		})
	}
}

func TestMigrationMultiFileInterruptionRetriesOnlyRemainingSources(t *testing.T) {
	first := migrationFixture(t, "first.yml", "run_policy: [{name: Old}]\n")
	last := migrationFixture(t, "last.yml", "run_policy: []\nwallet_amounts: {}\n")
	_, err := loadUnifiedSources([]string{first, last}, nil, nil, false, func(stage, path string) error {
		if stage == "after-replace" && path == first {
			return errors.New("crash")
		}
		return nil
	})
	if err == nil {
		t.Fatal("interruption ignored")
	}
	if len(configBackups(t, first)) != 1 || len(configBackups(t, last)) != 0 {
		t.Fatal("wrong partial commit")
	}
	u, err := LoadUnifiedConfigs([]string{first, last}, false)
	if err != nil {
		t.Fatal(err)
	}
	if u.RunPolicy == nil || len(u.RunPolicy) != 0 {
		t.Fatal("retry collapsed explicit empty list")
	}
	if len(configBackups(t, first)) != 1 || len(configBackups(t, last)) != 1 {
		t.Fatal("retry recreated completed backup")
	}
}

func TestMigrationConcurrencyAliasesAndDuplicatePaths(t *testing.T) {
	path := migrationFixture(t, "config.yml", "run_policy: [{name: Demo}]\n")
	alias := filepath.Join(filepath.Dir(path), ".", filepath.Base(path))
	var wait sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := LoadUnifiedConfigs([]string{alias, path}, false)
			if err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if len(configBackups(t, path)) != 1 {
		t.Fatal("concurrent normalized paths duplicated backups")
	}
}

func TestMigrationReadOnlyAndFailedBackupLeaveSource(t *testing.T) {
	path := migrationFixture(t, "config.yml", "run_policy: [{name: Demo}]\n")
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0600) })
	if _, err := LoadUnifiedConfigs([]string{path}, false); err == nil {
		t.Fatal("migrated read-only source")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "run_policy: [{name: Demo}]\n" || len(configBackups(t, path)) != 0 {
		t.Fatal("read-only source changed")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := loadUnifiedSources([]string{path}, nil, nil, false, func(stage, path string) error {
		if stage == "before-backup" {
			return errors.New("backup storage unavailable")
		}
		return nil
	})
	if err == nil {
		t.Fatal("backup failure ignored")
	}
	raw, _ = os.ReadFile(path)
	if string(raw) != "run_policy: [{name: Demo}]\n" {
		t.Fatal("backup failure changed source")
	}
}

func TestConfigAtomicEditorRejectsStaleBytes(t *testing.T) {
	path := migrationFixture(t, "config.yml", "name: initial\n")
	if err := WriteConfigAtomic(path, []byte("name: initial\n"), []byte("name: edited\n")); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfigAtomic(path, []byte("name: initial\n"), []byte("name: stale\n")); err == nil {
		t.Fatal("stale editor overwrote newer data")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "name: edited\n" {
		t.Fatal("stale write changed source")
	}
}
