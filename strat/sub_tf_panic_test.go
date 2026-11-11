package strat

import (
	"testing"
	"time"

	"github.com/banbox/banbot/orm"
)

func infoJobsLockFree(t *testing.T, st *State) {
	t.Helper()
	done := make(chan struct{})
	go func() { lockInfoJobsWrite(st); unlockInfoJobsWrite(st); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("info-jobs write lock still held (Runtime.Close would deadlock on it)")
	}
}

func pruneFixture(subKey string) (*State, map[string]map[string]map[string]*StratJob) {
	st := NewState()
	job := &StratJob{Strat: &TradeStrat{Name: "s"}, Symbol: &orm.ExSymbol{Symbol: "BTC/USDT"}, TimeFrame: "1h"}
	st.accInfoJobs = map[string]map[string]map[string]*StratJob{"acc": {subKey: {"j": job}}}
	return st, st.accInfoJobs
}

// issue 129: a custom-source subscription without a kline period used to panic (TFToSecs) while the info-jobs lock
// was held, and the caller's deferred Runtime.Close then waited on that lock forever. Now it is an error and the
// lock is free afterwards.
func TestPruneAccInfoJobsInvalidTfIsErrorAndUnlocks(t *testing.T) {
	st, _ := pruneFixture("sm:1:")
	err := pruneAccInfoJobs(st, nil, nil, map[*StratJob]bool{}, make(Warms), map[string]bool{}, map[string]int{}, true)
	if err == nil {
		t.Fatal("empty timeframe subscription must be an error")
	}
	infoJobsLockFree(t, st)
}

// a panic inside the locked section (nil tfSecs map write) must not leave the lock held either
func TestPruneAccInfoJobsPanicUnlocks(t *testing.T) {
	st, _ := pruneFixture("sm:1:1h")
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected the nil map write to panic")
			}
		}()
		_ = pruneAccInfoJobs(st, nil, nil, map[*StratJob]bool{}, make(Warms), map[string]bool{}, nil, true)
	}()
	infoJobsLockFree(t, st)
}

func TestEnsureSubTfSecsNoPanic(t *testing.T) {
	m := map[string]int{}
	if err := ensureSubTfSecs(m, "sm", "sm:1:", ""); err == nil {
		t.Fatal("empty timeframe must be an error")
	}
	if err := ensureSubTfSecs(m, "x", "x:1:zz", "zz"); err == nil {
		t.Fatal("invalid timeframe must be an error")
	}
	if err := ensureSubTfSecs(m, "kline", "kline:1:1h", "1h"); err != nil || m["1h"] != 3600 {
		t.Fatalf("1h: %v %v", err, m)
	}
}
