package orm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

const (
	sqliteStartupHelperEnv = "BANBOT_SQLITE_STARTUP_HELPER"
	sqliteStartupDirEnv    = "BANBOT_SQLITE_STARTUP_DIR"
	sqliteStartupAtEnv     = "BANBOT_SQLITE_STARTUP_AT"
)

func TestSQLiteConcurrentFreshStartup(t *testing.T) {
	if os.Getenv(sqliteStartupHelperEnv) == "1" {
		runSQLiteStartupHelper(t)
		return
	}

	const workerCount = 12
	startAt := time.Now().Add(2 * time.Second).UnixNano()
	baseDir := t.TempDir()
	type child struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	children := make([]child, workerCount)
	for i := range children {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSQLiteConcurrentFreshStartup$", "-test.count=1")
		cmd.Env = append(os.Environ(),
			sqliteStartupHelperEnv+"=1",
			sqliteStartupDirEnv+"="+baseDir,
			sqliteStartupAtEnv+"="+strconv.FormatInt(startAt, 10),
		)
		children[i].cmd = cmd
		cmd.Stdout = &children[i].out
		cmd.Stderr = &children[i].out
		if err := cmd.Start(); err != nil {
			for j := 0; j < i; j++ {
				_ = children[j].cmd.Process.Kill()
				_ = children[j].cmd.Wait()
			}
			t.Fatalf("start helper %d: %v", i, err)
		}
	}

	for i := range children {
		if err := children[i].cmd.Wait(); err != nil {
			t.Errorf("helper %d: %v\n%s", i, err, children[i].out.String())
		}
	}
}

func runSQLiteStartupHelper(t *testing.T) {
	t.Helper()
	startAt, err := strconv.ParseInt(os.Getenv(sqliteStartupAtEnv), 10, 64)
	if err != nil {
		t.Fatalf("parse start time: %v", err)
	}
	baseDir := os.Getenv(sqliteStartupDirEnv)
	if baseDir == "" {
		t.Fatal("missing helper database directory")
	}

	const freshDatabaseCount = 24
	for i := range freshDatabaseCount {
		wait := time.Until(time.Unix(0, startAt).Add(time.Duration(i) * 20 * time.Millisecond))
		if wait > 0 {
			time.Sleep(wait)
		}

		path := filepath.Join(baseDir, fmt.Sprintf("fresh-%02d.db", i))
		db, openErr := newDbLite(DbPub, path, true, 2_000)
		if openErr != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), openErr)
		}
		var journalMode string
		queryErr := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
		closeErr := db.Close()
		if queryErr != nil {
			t.Fatalf("query journal mode for %s: %v", filepath.Base(path), queryErr)
		}
		if closeErr != nil {
			t.Fatalf("close %s: %v", filepath.Base(path), closeErr)
		}
		if journalMode != "wal" {
			t.Fatalf("journal mode for %s = %q, want wal", filepath.Base(path), journalMode)
		}
	}
}

type sqliteCodeError int

func (e sqliteCodeError) Error() string { return fmt.Sprintf("sqlite code %d", e) }
func (e sqliteCodeError) Code() int     { return int(e) }

func TestRetrySQLiteBusy(t *testing.T) {
	attempts := 0
	err := retrySQLiteBusy(context.Background(), 0, func() error {
		attempts++
		if attempts == 1 {
			return sqliteCodeError(sqlite3.SQLITE_BUSY)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry busy error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("busy attempts = %d, want 2", attempts)
	}

	nonBusy := errors.New("disk I/O error")
	attempts = 0
	err = retrySQLiteBusy(context.Background(), 0, func() error {
		attempts++
		return nonBusy
	})
	if !errors.Is(err, nonBusy) {
		t.Fatalf("non-busy error = %v, want %v", err, nonBusy)
	}
	if attempts != 1 {
		t.Fatalf("non-busy attempts = %d, want 1", attempts)
	}
}

func TestRetrySQLiteBusyStopsAtDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	busyErr := sqliteCodeError(sqlite3.SQLITE_BUSY | 2<<8)
	err := retrySQLiteBusy(ctx, 0, func() error {
		attempts++
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("deadline error = %v, want %v", err, busyErr)
	}
	if attempts != 1 {
		t.Fatalf("deadline attempts = %d, want 1", attempts)
	}
}
