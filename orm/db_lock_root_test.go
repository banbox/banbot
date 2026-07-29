package orm

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
)

func swapLockConfig(t *testing.T, dataDir string, db *config.DatabaseConfig) {
	t.Helper()
	prevDir, prevDB := config.DataDir, config.Database
	t.Setenv("BanDataDir", "") // keep GetDataDirSafe off the ambient env
	config.DataDir, config.Database = dataDir, db
	t.Cleanup(func() { config.DataDir, config.Database = prevDir, prevDB })
}

func TestDBIdentLockRootSameAcrossDataDirs(t *testing.T) {
	url := "postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable"
	swapLockConfig(t, t.TempDir(), &config.DatabaseConfig{Url: url})
	compactA, klineA := compactProcessLockRoot(), klineInsertLockRoot()
	config.DataDir = t.TempDir()
	compactB, klineB := compactProcessLockRoot(), klineInsertLockRoot()
	if compactA == "" || compactA != compactB {
		t.Fatalf("same URL must yield one shared compact root across data dirs: %q vs %q", compactA, compactB)
	}
	if klineA == "" || klineA != klineB {
		t.Fatalf("same URL must yield one shared kline_insert root across data dirs: %q vs %q", klineA, klineB)
	}
	if compactA == klineA {
		t.Fatalf("compact and kline_insert must not share one lock dir: %q", compactA)
	}
	if strings.HasPrefix(compactA, config.DataDir) || strings.HasPrefix(klineA, config.DataDir) {
		t.Fatalf("shared roots must live OUTSIDE the data dir: %q / %q", compactA, klineA)
	}
}

func TestDBIdentLockRootNilDatabaseFallsBackToDataDir(t *testing.T) {
	dir := t.TempDir()
	swapLockConfig(t, dir, nil)
	if got, want := compactProcessLockRoot(), compactProcessLockRootDataDir(); got != want {
		t.Fatalf("nil Database must fall back to the legacy compact root %q, got %q", want, got)
	}
	if got, want := klineInsertLockRoot(), klineInsertLockRootDataDir(); got != want {
		t.Fatalf("nil Database must fall back to the legacy kline root %q, got %q", want, got)
	}
	if got := compactProcessLockRoot(); !strings.HasPrefix(got, dir) {
		t.Fatalf("fallback root %q not under data dir %q", got, dir)
	}
}

func TestDBIdentLockRootEmptyConfigIsNoop(t *testing.T) {
	swapLockConfig(t, "", nil)
	if got := compactProcessLockRoot(); got != "" {
		t.Fatalf("no config at all must keep the no-op empty root, got %q", got)
	}
	if got := klineInsertLockRoot(); got != "" {
		t.Fatalf("no config at all must keep the no-op empty kline root, got %q", got)
	}
}

func TestDBIdentLockRootUnparsableURLFallsBack(t *testing.T) {
	dir := t.TempDir()
	swapLockConfig(t, dir, &config.DatabaseConfig{Url: "://not-a-url"})
	if got := compactProcessLockRoot(); !strings.HasPrefix(got, dir) {
		t.Fatalf("unparsable URL must fall back to the data-dir root, got %q", got)
	}
}

func TestDBIdentLockRootPathLeaksNoSecrets(t *testing.T) {
	swapLockConfig(t, t.TempDir(),
		&config.DatabaseConfig{Url: "postgresql://admin:hunter2@db-secret-host:8812/qdb"})
	root := compactProcessLockRoot()
	for _, frag := range []string{"hunter2", "admin", "db-secret-host", "8812"} {
		if strings.Contains(root, frag) {
			t.Fatalf("lock root %q leaks %q from the database URL", root, frag)
		}
	}
}
