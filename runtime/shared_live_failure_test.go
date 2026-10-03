package runtime

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestFactorLiveFailureRetainsFreezePersistenceErrorAndClosesAdmission(t *testing.T) {
	f := newSharedTriggerFixture(t)
	sibling, err := f.process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", f.opts.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TRIGGER fail_freeze BEFORE UPDATE OF frozen ON exec_account BEGIN SELECT RAISE(ABORT,'freeze disk failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	primary := errors.New("realtime source failed")
	failures := make(chan error, 2)
	f.rt.failFactorLive(nil, nil, failures, primary)
	err = <-failures
	if !errors.Is(err, primary) || !strings.Contains(err.Error(), "freeze disk failure") {
		t.Fatalf("lost primary/freeze evidence: %v", err)
	}
	if f.rt.EnterCallback() {
		f.rt.LeaveCallback()
		t.Fatal("failure left callback admission open")
	}
	if err := sibling.SharedExecution().Send("unsubmitted", 102); err == nil || !strings.Contains(err.Error(), "reconcil") {
		t.Fatalf("failure left shared execution ready: %v", err)
	}
}
