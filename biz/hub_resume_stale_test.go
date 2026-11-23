package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// staleOwnerPort is a Hub that refuses every create as stale and counts sends.
type staleOwnerPort struct {
	execution.HubPort
	sends int
	err   error
}

func (p *staleOwnerPort) ApplyPendingCommand(context.Context, execution.AccountKey, execution.PendingCommand) (int64, error) {
	p.sends++
	return 0, p.err
}

func resumeMgr(t *testing.T, port execution.HubPort) (*HubOrderMgr, *observer.ObservedLogs) {
	t.Helper()
	m := reviewMgr(t)
	m.config.Client = port
	obsCore, logs := observer.New(zapcore.DebugLevel)
	m.deps.Core = &core.State{Logger: zap.New(obsCore)}
	return m, logs
}

func frozenCreate(created int64) *hubFrozenCommand {
	return &hubFrozenCommand{
		OrderID: 42,
		Pending: &execution.PendingCommand{ID: "ts/s/1/1", Lot: "lot-x", Action: execution.PendingCreate,
			Metadata: &execution.TimeSeriesMetadata{OrderID: 42, TimeFrame: "1m", CreatedMS: created}},
	}
}

// A frozen create resent on restart after the owner's stale threshold is
// dropped with the existing warning, the robot keeps running, and the resend
// itself is logged once per command with its age.
func TestHubResumeStaleCreateIsDroppedAndResendLogged(t *testing.T) {
	port := &staleOwnerPort{err: &execution.RejectedError{Code: execution.RejectStaleEntry, Err: errors.New("stale")}}
	m, logs := resumeMgr(t, port)
	cmd := frozenCreate(time.Now().Add(-10 * time.Minute).UnixMilli())
	m.journal.Commands = map[string]*hubFrozenCommand{"c1": cmd}

	if err := m.resumeCommands(); err != nil {
		t.Fatal("a stale-entry refusal must not stop the robot:", err)
	}
	if port.sends != 1 || !cmd.Done || !cmd.Rejected {
		t.Fatalf("sends=%d done=%v rejected=%v", port.sends, cmd.Done, cmd.Rejected)
	}
	if m.LastError() != nil {
		t.Fatal(m.LastError())
	}
	resend := logs.FilterMessage("hub resends a frozen command").All()
	if len(resend) != 1 || resend[0].Level != zapcore.InfoLevel {
		t.Fatalf("want exactly one INFO resend line, got %d", len(resend))
	}
	f := resend[0].ContextMap()
	if f["order_id"] != int64(42) || f["lot"] != "lot-x" || f["kind"] != "create" {
		t.Fatalf("resend line fields: %v", f)
	}
	if age, ok := f["age"].(time.Duration); !ok || age < 9*time.Minute || age > 11*time.Minute {
		t.Fatalf("resend line age: %v", f["age"])
	}
	warn := logs.FilterMessage("hub rejected an entry; it is dropped, not retried").All()
	if len(warn) != 1 || warn[0].Level != zapcore.WarnLevel || warn[0].ContextMap()["reason"] != execution.RejectStaleEntry {
		t.Fatalf("dropped-entry warning missing: %v", warn)
	}
	// A later resume neither resends nor logs again.
	if err := m.resumeCommands(); err != nil || port.sends != 1 || len(logs.FilterMessage("hub resends a frozen command").All()) != 1 {
		t.Fatal("dropped command was resent", err, port.sends)
	}
}

// Without time-series metadata the line still names the command; Done and
// Imported commands are not logged because they are not resent.
func TestHubResumeLogsOnlyResentCommands(t *testing.T) {
	port := &staleOwnerPort{}
	m, logs := resumeMgr(t, port)
	noMeta := &hubFrozenCommand{OrderID: 5, Pending: &execution.PendingCommand{ID: "c2", Lot: "lot-y", Action: execution.PendingSetProtections}}
	m.journal.Commands = map[string]*hubFrozenCommand{"c2": noMeta,
		"c3": {OrderID: 6, Done: true, Pending: &execution.PendingCommand{ID: "c3"}},
		"c4": {OrderID: 7, Imported: true, Pending: &execution.PendingCommand{ID: "c4"}}}
	if err := m.resumeCommands(); err != nil {
		t.Fatal(err)
	}
	lines := logs.FilterMessage("hub resends a frozen command").All()
	if len(lines) != 1 || port.sends != 1 {
		t.Fatalf("lines=%d sends=%d", len(lines), port.sends)
	}
	f := lines[0].ContextMap()
	if _, has := f["age"]; has || f["kind"] != string(execution.PendingSetProtections) || f["order_id"] != int64(5) {
		t.Fatalf("fields: %v", f)
	}
}
