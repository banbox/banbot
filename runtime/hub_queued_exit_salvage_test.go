package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	goruntime "runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// salvagePort fails ApplyTimeSeriesExit and/or the projection with a hard
// (non-rejection) error while the flags are set.
type salvagePort struct {
	execution.HubPort
	failExit, failProj atomic.Bool
}

func (p *salvagePort) ApplyTimeSeriesExit(ctx context.Context, a execution.AccountKey, c execution.TimeSeriesExitCommand) ([]execution.TimeSeriesExitResult, error) {
	if p.failExit.Load() {
		return nil, errors.New("test: owner unreachable")
	}
	return p.HubPort.ApplyTimeSeriesExit(ctx, a, c)
}

func (p *salvagePort) TimeSeriesProjection(ctx context.Context, a execution.AccountKey, r execution.TimeSeriesProjectionRequest) (execution.TimeSeriesProjectionPage, error) {
	// Only the poll made from ProcessOrders fails: the 100ms background poll
	// failing would stop the robot (Core.Stop) and cancel the shared runtime.
	if p.failProj.Load() && inProcessOrders() {
		return execution.TimeSeriesProjectionPage{}, errors.New("test: projection unreachable")
	}
	return p.HubPort.TimeSeriesProjection(ctx, a, r)
}

func inProcessOrders() bool {
	buf := make([]byte, 16384)
	return strings.Contains(string(buf[:goruntime.Stack(buf, false)]), "HubOrderMgr).ProcessOrders")
}

type salvageRig struct {
	deps biz.RuntimeDeps
	f    *hubTSFixture
	port *salvagePort
	mgr  *biz.HubOrderMgr
	job  *strat.StratJob
	logs *observer.ObservedLogs
	a, b *ormo.InOutOrder
}

type journalProbe struct {
	Commands map[string]struct {
		Done bool
		Exit *execution.TimeSeriesExitCommand
	}
}

func newSalvageRig(t *testing.T) *salvageRig {
	t.Helper()
	f := newHubTSFixture(t, nil)
	obsCore, logs := observer.New(zap.WarnLevel)
	f.manager.CleanUp()
	deps := f.rt.BizDeps()
	f.rt.Core.Logger = zap.New(obsCore)
	port := &salvagePort{HubPort: f.client}
	bridge := *f.bridge
	bridge.Client = port
	f.bridge = &bridge
	mgr, err := biz.NewHubOrderMgr(deps, f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.CleanUp() })
	f.manager = mgr
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "1m", Account: "default", CloseLong: true, CloseShort: true}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	if err = mgr.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	r := &salvageRig{deps: deps, f: f, port: port, mgr: mgr, job: job, logs: logs}
	var e *errs.Error
	if r.a, e = mgr.EnterOrder(job.Symbol, "1m", &strat.EnterReq{CommandID: "lot-a", Amount: 1}); e != nil || r.a == nil {
		t.Fatal(r.a, e)
	}
	if r.b, e = mgr.EnterOrder(job.Symbol, "1m", &strat.EnterReq{CommandID: "lot-b", Amount: 1}); e != nil || r.b == nil {
		t.Fatal(r.b, e)
	}
	return r
}

func (r *salvageRig) journal(t *testing.T) journalProbe {
	t.Helper()
	body, err := os.ReadFile(r.f.bridge.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var j journalProbe
	if err = json.Unmarshal(body, &j); err != nil {
		t.Fatal(err)
	}
	return j
}

// exits returns the journaled exit commands keyed by exit CommandID.
func (r *salvageRig) exits(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, c := range r.journal(t).Commands {
		if c.Exit != nil {
			out[c.Exit.ID] = c.Done
		}
	}
	return out
}

func (r *salvageRig) restart(t *testing.T, port execution.HubPort) *biz.HubOrderMgr {
	t.Helper()
	r.mgr.CleanUp()
	bridge := *r.f.bridge
	bridge.Client = port
	m, err := biz.NewHubOrderMgr(r.deps, &bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.CleanUp() })
	return m
}

func (r *salvageRig) openFilled(t *testing.T) int64 { return hubHeld(t, r.f) }

// A frozen exit whose resend hard-fails blocks the next exit; the exit the
// strategy already queued for another lot must still be journaled, and a restart
// with a healthy owner sends both and closes the lots.
func TestHubProcessOrdersFailureJournalsQueuedExit(t *testing.T) {
	r := newSalvageRig(t)
	r.port.failExit.Store(true)
	if _, e := r.mgr.ExitOrder(r.a, &strat.ExitReq{CommandID: "x-a", Tag: "t"}); e == nil {
		t.Fatal("exit must fail while the owner is unreachable")
	}
	if done, ok := r.exits(t)["x-a"]; !ok || done {
		t.Fatal("first exit not frozen as pending", r.exits(t))
	}
	r.job.Exits = append(r.job.Exits, &strat.ExitReq{OrderID: r.b.ID, CommandID: "x-b", Tag: "t"})
	if !r.job.BeginOrderProcessing() {
		t.Fatal("processing flag held")
	}
	r.job.EndOrderProcessing()
	if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
		t.Fatal("ProcessOrders must return the original error")
	}
	got := r.exits(t)
	if done, ok := got["x-b"]; !ok || done {
		t.Fatal("queued exit lost", got)
	}
	if len(r.job.Exits) != 0 || !r.job.BeginOrderProcessing() {
		t.Fatal("job queue or processing state inconsistent")
	}
	r.job.EndOrderProcessing()

	healthy := r.restart(t, r.f.client)
	if got = r.exits(t); !got["x-a"] || !got["x-b"] {
		t.Fatal("restart did not resend both exits", got)
	}
	if err := healthy.UpdateByDataSeries(nil, nil); err != nil {
		t.Fatal(err)
	}
	if n := r.openFilled(t); n != 0 {
		t.Fatal("lots still open after restart resend", n)
	}
}

// The same when the poll (projection) fails before any request is processed.
func TestHubProcessOrdersPollFailureJournalsQueuedExit(t *testing.T) {
	r := newSalvageRig(t)
	r.port.failProj.Store(true)
	r.job.Exits = append(r.job.Exits, &strat.ExitReq{OrderID: r.a.ID, CommandID: "x-a", Tag: "t"})
	if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
		t.Fatal("ProcessOrders must return the poll error")
	}
	if done, ok := r.exits(t)["x-a"]; !ok || done {
		t.Fatal("queued exit not journaled", r.exits(t))
	}
	r.port.failProj.Store(false)
	r.restart(t, r.f.client)
	if !r.exits(t)["x-a"] {
		t.Fatal("restart did not send the exit")
	}
}

func TestHubProcessOrdersFailureDropsQueuedEntry(t *testing.T) {
	r := newSalvageRig(t)
	r.port.failProj.Store(true)
	before := len(r.journal(t).Commands)
	r.job.Entrys = append(r.job.Entrys, &strat.EnterReq{CommandID: "late-entry", Tag: "late", Amount: 1})
	if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
		t.Fatal("expected error")
	}
	if after := len(r.journal(t).Commands); after != before {
		t.Fatal("entry was journaled", before, after)
	}
	found := false
	for _, e := range r.logs.All() {
		if e.ContextMap()["command_id"] == "late-entry" && e.Level == zap.WarnLevel {
			found = true
		}
	}
	if !found {
		t.Fatal("dropped entry was not logged")
	}
	if len(r.job.Entrys) != 0 || !r.job.BeginOrderProcessing() {
		t.Fatal("job state inconsistent")
	}
}

func TestHubProcessOrdersFailureDoesNotFreezeTwice(t *testing.T) {
	r := newSalvageRig(t)
	r.port.failProj.Store(true)
	before := len(r.journal(t).Commands)
	for i := 0; i < 2; i++ {
		r.job.Exits = append(r.job.Exits, &strat.ExitReq{OrderID: r.a.ID, CommandID: "x-a", Tag: "t"})
		if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
			t.Fatal("expected error")
		}
	}
	if after := len(r.journal(t).Commands); after != before+1 {
		t.Fatal("duplicate exit frozen twice", before, after)
	}
	// Without a CommandID the key is derived from the journal serial, exactly as
	// the normal path does: it is journaled with a generated "ts/..." id.
	r.job.Exits = append(r.job.Exits, &strat.ExitReq{OrderID: r.b.ID, Tag: "t"})
	if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
		t.Fatal("expected error")
	}
	generated := 0
	for id := range r.exits(t) {
		if len(id) > 3 && id[:3] == "ts/" {
			generated++
		}
	}
	if generated != 1 {
		t.Fatal("exit without CommandID not journaled with a generated id", r.exits(t))
	}
}

// Resending a frozen exit whose lot was closed meanwhile is harmless: the owner
// finds nothing to reduce, the command completes and the robot keeps running.
func TestHubResendExitForClosedLotDoesNotStopRobot(t *testing.T) {
	r := newSalvageRig(t)
	if _, e := r.mgr.ExitOrder(r.b, &strat.ExitReq{CommandID: "close-b", Tag: "t"}); e != nil {
		t.Fatal(e)
	}
	r.port.failProj.Store(true)
	r.job.Exits = append(r.job.Exits, &strat.ExitReq{OrderID: r.b.ID, CommandID: "x-b-late", Tag: "t"})
	if _, _, e := r.mgr.ProcessOrders(r.job); e == nil {
		t.Fatal("expected error")
	}
	if done, ok := r.exits(t)["x-b-late"]; !ok || done {
		t.Fatal("late exit not frozen", r.exits(t))
	}
	m := r.restart(t, r.f.client)
	if err := m.LastError(); err != nil {
		t.Fatal("robot stopped:", err)
	}
	if !r.exits(t)["x-b-late"] {
		t.Fatal("exit for the closed lot was not completed", r.exits(t))
	}
	// The robot keeps working: lot a is still closable afterwards.
	if _, e := m.ExitOrder(r.a, &strat.ExitReq{CommandID: "close-a", Tag: "t"}); e != nil {
		t.Fatal("robot cannot act after resending an exit for a flat lot:", e)
	}
	if n := r.openFilled(t); n != 0 {
		t.Fatal("position still open", n)
	}
}
