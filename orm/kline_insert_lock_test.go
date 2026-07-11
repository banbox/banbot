package orm

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	klineLockHelperRoot  = "BANBOT_TEST_KLINE_LOCK_ROOT"
	klineLockHelperTF    = "BANBOT_TEST_KLINE_LOCK_TF"
	klineLockHelperWant  = "BANBOT_TEST_KLINE_LOCK_WANT"
	klineLockHelperCrash = "BANBOT_TEST_KLINE_LOCK_CRASH"
)

func TestKlineInsertFileLockHelper(t *testing.T) {
	root := os.Getenv(klineLockHelperRoot)
	if root == "" {
		t.Skip("subprocess helper")
	}
	tf := os.Getenv(klineLockHelperTF)
	want, err := strconv.ParseBool(os.Getenv(klineLockHelperWant))
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Unix(1_700_000_000, 123_456_000).UTC()
	claimed, err := acquireKlineInsertFileLock(root, 42, tf, ts)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != want {
		t.Fatalf("claim mismatch: want=%v got=%v", want, claimed)
	}
	if claimed && os.Getenv(klineLockHelperCrash) == "1" {
		return
	}
	if claimed {
		if err := releaseKlineInsertFileLock(root, 42, tf, ts); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKlineInsertFileLockReleasedOnProcessExit(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestKlineInsertFileLockHelper$")
	cmd.Env = append(os.Environ(),
		klineLockHelperRoot+"="+root,
		klineLockHelperTF+"=15m",
		klineLockHelperWant+"=true",
		klineLockHelperCrash+"=1",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper failed: %v\n%s", err, out)
	}
	ts := time.Unix(1_700_000_000, 123_456_000).UTC()
	claimed, err := acquireKlineInsertFileLock(root, 42, "15m", ts)
	if err != nil || !claimed {
		t.Fatalf("claim after process exit failed: claimed=%v err=%v", claimed, err)
	}
	if err := releaseKlineInsertFileLock(root, 42, "15m", ts); err != nil {
		t.Fatal(err)
	}
}

func TestKlineInsertFileLockAcrossProcesses(t *testing.T) {
	root := t.TempDir()
	ts := time.Unix(1_700_000_000, 123_456_000).UTC()
	claimed, err := acquireKlineInsertFileLock(root, 42, "15m", ts)
	if err != nil || !claimed {
		t.Fatalf("parent claim failed: claimed=%v err=%v", claimed, err)
	}

	runHelper := func(tf string, want bool) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestKlineInsertFileLockHelper$")
		cmd.Env = append(os.Environ(),
			klineLockHelperRoot+"="+root,
			klineLockHelperTF+"="+tf,
			klineLockHelperWant+"="+strconv.FormatBool(want),
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("helper failed: %v\n%s", err, out)
		}
	}

	runHelper("15m", false)
	runHelper("30m", true)
	if err := releaseKlineInsertFileLock(root, 42, "15m", ts); err != nil {
		t.Fatal(err)
	}
	runHelper("15m", true)
}

func TestWaitForKlineInsertCompletionPollsUntilCovered(t *testing.T) {
	checks := 0
	complete, err := waitForKlineInsertCompletion(context.Background(), time.Millisecond, time.Millisecond,
		func() (bool, error) {
			checks++
			return checks >= 3, nil
		},
		func() (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !complete || checks != 3 {
		t.Fatalf("expected coverage after three checks: complete=%v checks=%d", complete, checks)
	}
}

func TestWaitForKlineInsertCompletionRetriesAfterInactiveGrace(t *testing.T) {
	complete, err := waitForKlineInsertCompletion(context.Background(), time.Millisecond, time.Millisecond,
		func() (bool, error) { return false, nil },
		func() (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("inactive incomplete owner should return for a new claim attempt")
	}
}

type insKlineExecStub struct {
	repairDBStub
	tag pgconn.CommandTag
}

func (s *insKlineExecStub) Exec(_ context.Context, sql string, _ ...interface{}) (pgconn.CommandTag, error) {
	s.execSQL = append(s.execSQL, sql)
	return s.tag, s.execErr
}

func TestAddInsKlinePgConflictDoesNotTakeClaim(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = false
	defer func() { IsQuestDB = oldQuestDB }()

	key := insKlineLockKey(7, "15m")
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()

	db := &insKlineExecStub{tag: pgconn.NewCommandTag("INSERT 0 0")}
	q := New(db)
	claimed, err := q.tryAddInsKlinePg(context.Background(), AddInsKlineParams{Sid: 7, Timeframe: "15m", StartMs: 10, StopMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("conflicting insert must not take claim")
	}
	if len(db.execSQL) != 1 || !strings.Contains(db.execSQL[0], "ON CONFLICT (sid, timeframe) DO NOTHING") {
		t.Fatalf("unexpected claim SQL: %v", db.execSQL)
	}
	insKlineLocksmu.Lock()
	_, locked := insKlineLocks[key]
	insKlineLocksmu.Unlock()
	if locked {
		t.Fatal("losing database claim must release the in-process guard")
	}
}

func TestAddInsKlineQuestFileConflictSkipsDatabase(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldDataDir := config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	defer func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
	}()

	key := insKlineLockKey(8, "15m")
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()

	ownerTS := normalizeQuestTimestamp(time.Now().Add(-time.Second))
	root := klineInsertLockRoot()
	claimed, err := acquireKlineInsertFileLock(root, 8, "15m", ownerTS)
	if err != nil || !claimed {
		t.Fatalf("owner claim failed: claimed=%v err=%v", claimed, err)
	}
	defer releaseKlineInsertFileLock(root, 8, "15m", ownerTS)

	db := &repairDBStub{}
	q := New(db)
	ts, err := q.AddInsKline(context.Background(), AddInsKlineParams{Sid: 8, Timeframe: "15m", StartMs: 10, StopMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	if !ts.IsZero() {
		t.Fatalf("conflicting file claim must lose: %s", ts)
	}
	if len(db.execSQL) != 0 {
		t.Fatalf("loser must not write a recovery marker: %v", db.execSQL)
	}
}

func TestDelInsKlineFailureReleasesLeaseButKeepsMarker(t *testing.T) {
	oldQuestDB := IsQuestDB
	oldDataDir := config.DataDir
	IsQuestDB = true
	config.DataDir = t.TempDir()
	defer func() {
		IsQuestDB = oldQuestDB
		config.DataDir = oldDataDir
	}()

	ts := normalizeQuestTimestamp(time.Now())
	root := klineInsertLockRoot()
	claimed, err := acquireKlineInsertFileLock(root, 9, "15m", ts)
	if err != nil || !claimed {
		t.Fatalf("claim failed: claimed=%v err=%v", claimed, err)
	}
	db := &repairDBStub{execErr: errors.New("delete marker failed")}
	q := New(db)
	if err := q.DelInsKline(context.Background(), 9, "15m", ts); err == nil {
		t.Fatal("expected marker deletion error")
	}
	active, err := klineInsertFileLockActive(root, 9, "15m")
	if err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("failed owner must release its lease so recovery can proceed")
	}
}
