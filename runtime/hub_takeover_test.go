package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// hubTakeoverFixture is an account whose owner adopted an external 3-step long
// (take_over_strat) and serves it through a Unix-socket Hub to a Hub robot.
type hubTakeoverFixture struct {
	owner  *sharedTriggerFixture
	ownerR *Runtime
	proc   *Process
	client *execution.HubClient
	bridge *biz.HubOrderBridgeConfig
}

func newHubTakeoverFixture(t *testing.T) *hubTakeoverFixture {
	t.Helper()
	owner := newSharedTriggerFixtureWithAdapter(t, nil, true)
	i := owner.bridge.Instruments["BTC"]
	ctx := context.Background()
	if _, err := owner.adapter.Submit(ctx, execution.OrderIntent{ID: "external", Instrument: i, Side: execution.Buy, Steps: 3, Observation: execution.ExecutionObservation{Price: decimal.NewFromInt(100), AtMS: 100}, SubmitAtMS: 100}, "external-client"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.rt.SharedExecution().ApplyExternalPosition(execution.ExternalPositionEvent{ID: "external-trade", Kind: execution.ExternalCashChange, Instrument: i, Side: execution.Buy, Steps: 3, Price: decimal.NewFromInt(100), AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	owner.process.Close()
	owner.opts.TakeoverStrategy = "ts"
	p := NewProcess()
	t.Cleanup(p.Close)
	ownerRt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &owner.key, SharedExecution: owner.opts, SharedOrderBridge: owner.bridge})
	if err != nil {
		t.Fatal(err)
	}
	ownerRt.Clock.SetTimeMS(101)
	if err := ownerRt.SharedExecution().Reconcile("external-restart", 101); err != nil {
		t.Fatal(err)
	}
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		var service *execution.SharedAccount
		err := ownerRt.SharedExecution().WithState(func(s *execution.SharedAccount) error { service = s; return nil })
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "hub-tk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	server, err := execution.ListenHubUnix(filepath.Join(dir, "s"), hub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client := execution.NewHubClient(filepath.Join(dir, "s"))
	t.Cleanup(func() { client.Close() })
	if err = client.Register(ctx, execution.HubAccountRegistration{Account: owner.key, Env: core.RunEnvDryRun, Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	bridge := &biz.HubOrderBridgeConfig{Client: client, Account: owner.key, Bridge: *owner.bridge, WriterID: "test-robot", StatePath: filepath.Join(dir, "robot.json")}
	return &hubTakeoverFixture{owner: owner, ownerR: ownerRt, proc: p, client: client, bridge: bridge}
}

// robot starts a Hub robot runtime + manager + bound job; takeover is the
// robot's take_over_strat and strategy the strategy name of its job.
func (f *hubTakeoverFixture) robot(t *testing.T, takeover, strategy string) (*Runtime, *biz.HubOrderMgr, *strat.StratJob) {
	t.Helper()
	rt, err := f.proc.NewRuntime(Options{Config: &config.Config{TakeOverStrat: takeover}, Mode: core.RunModeLive, Env: core.RunEnvDryRun, HubOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	biz.EnsureTimeSeriesOrderTask(rt.BizDeps())
	manager, err := biz.NewHubOrderMgr(rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.CleanUp() })
	rt.Trading.SetOrderManager("default", manager)
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: strategy}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "ws", Account: "default", CloseLong: true, CloseShort: true}
	job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	job.BindRuntimeMarket(rt.Market.Prices, rt.Clock)
	if err := manager.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	return rt, manager, job
}

func TestHubTakeoverRendersAdoptedLotSurvivesRestartAndCloses(t *testing.T) {
	f := newHubTakeoverFixture(t)
	ctx := context.Background()
	fills := f.owner.adapter.Metrics().Fills
	_, manager, job := f.robot(t, "legacy", "legacy")
	orders := job.ExecutionSnapshot().LongOrders
	if len(orders) != 1 || orders[0].Enter.Filled != .3 || orders[0].EnterTag != core.EnterTagThird || orders[0].Enter.Average != 100 || orders[0].EnterAt != 101 || orders[0].Strategy != "legacy" || orders[0].Timeframe != "ws" || orders[0].Status != ormo.InOutStatusFullEnter {
		t.Fatalf("adopted lot not rendered as a strategy order: %+v", orders)
	}
	if orders[0].Info["external_takeover"] == nil || orders[0].Info["entry_time_is_adoption"] != true {
		t.Fatalf("takeover provenance missing: %v", orders[0].Info)
	}
	if f.owner.adapter.Metrics().Fills != fills {
		t.Fatal("rendering sent an order")
	}
	id := orders[0].ID
	// Robot restart: fresh runtime and manager over the same journal.
	if e := manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	rt, manager, job := f.robot(t, "legacy", "legacy")
	orders = job.ExecutionSnapshot().LongOrders
	if len(orders) != 1 || orders[0].ID != id || orders[0].Enter.Filled != .3 || f.owner.adapter.Metrics().Fills != fills {
		t.Fatalf("restart changed the adopted order or sent again: %+v", orders)
	}
	// Close through the regular strategy exit.
	if _, e := manager.ExitOpenOrders("BTC", &strat.ExitReq{Tag: "adopted-close", StratName: "legacy", OrderID: id, Force: true, Dirt: core.OdDirtLong}); e != nil {
		t.Fatal(e)
	}
	snapshot, err := f.ownerR.SharedExecution().Snapshot(ctx)
	if err != nil || len(snapshot.Lots) != 0 || len(snapshot.ActualPositions) != 1 || snapshot.ActualPositions[0].SignedSteps != 0 || f.owner.adapter.Metrics().Fills != fills+1 {
		t.Fatal("strategy cannot close adopted inventory through the Hub", snapshot, err)
	}
	if e := manager.UpdateByDataSeries(nil, nil); e != nil {
		t.Fatal(e)
	}
	od := hubOrderByID(rt, id)
	if od == nil || od.Status != ormo.InOutStatusFullExit || od.ExitTag != "adopted-close" || od.Exit == nil || od.Exit.Filled != .3 || od.EnterTag != core.EnterTagThird {
		t.Fatalf("closed adopted order: %+v", od)
	}
	if len(job.ExecutionSnapshot().LongOrders) != 0 {
		t.Fatal("closed adopted order still open on the job")
	}
	sess, conn, connErr := rt.Orders.Conn(false)
	if connErr != nil {
		t.Fatal(connErr)
	}
	rows, rowsErr := sess.GetOrders(ormo.GetOrdersArgs{TaskID: rt.Orders.GetTaskID("default"), Status: 2})
	conn.Close()
	if rowsErr != nil || len(rows) != 1 || rows[0].ExitTag != "adopted-close" || rows[0].EnterTag != core.EnterTagThird || rows[0].Strategy != "legacy" {
		t.Fatalf("closed adopted order not archived with the strategy exit tag: %v %v", rows, rowsErr)
	}
	// A third start must not resurrect the flat lot.
	if e := manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	_, _, job = f.robot(t, "legacy", "legacy")
	if len(job.ExecutionSnapshot().LongOrders) != 0 {
		t.Fatal("flat adopted lot re-rendered after restart")
	}
}

func hubOrderByID(rt *Runtime, id int64) *ormo.InOutOrder {
	ods, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	if od := ods[id]; od != nil {
		return od.Clone()
	}
	return nil
}

func TestHubTakeoverIgnoredByRobotWithOtherStrategy(t *testing.T) {
	f := newHubTakeoverFixture(t)
	// Not configured: default projection requests stay unchanged.
	_, manager, job := f.robot(t, "", "legacy")
	if orders := job.ExecutionSnapshot().LongOrders; len(orders) != 0 {
		t.Fatalf("robot without take_over_strat rendered an adopted lot: %+v", orders)
	}
	// Configured for a strategy this robot does not declare.
	if e := manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	_, _, job = f.robot(t, "other", "legacy")
	if orders := job.ExecutionSnapshot().LongOrders; len(orders) != 0 {
		t.Fatalf("robot with a foreign take_over_strat rendered an adopted lot: %+v", orders)
	}
}

func TestHubTakeoverOwnerExposureScopeAndBinding(t *testing.T) {
	f := newHubTakeoverFixture(t)
	ctx := context.Background()
	page := func(strategies ...execution.StrategyID) execution.TimeSeriesProjectionPage {
		t.Helper()
		p, err := f.client.TimeSeriesProjection(ctx, f.bridge.Account, execution.TimeSeriesProjectionRequest{Name: "probe", Limit: 512, Takeovers: strategies})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if got := page(); len(got.Takeovers) != 0 {
		t.Fatalf("takeovers exposed without being requested: %+v", got.Takeovers)
	}
	if got := page("other"); len(got.Takeovers) != 0 {
		t.Fatalf("takeovers leaked to a foreign strategy: %+v", got.Takeovers)
	}
	got := page("ts")
	if len(got.Takeovers) != 1 || got.Takeovers[0].Attribution.Strategy != "ts" || got.Takeovers[0].Attribution.ExpectedSignedSteps != 3 {
		t.Fatalf("takeover record missing: %+v", got.Takeovers)
	}
	adopted := got.Takeovers[0].Attribution
	meta := execution.TimeSeriesMetadata{OrderID: 77, StrategyName: "legacy", Symbol: "BTC", TimeFrame: "ws", EnterTag: core.EnterTagThird, CreatedMS: adopted.AtMS, InitPrice: decimal.NewFromInt(100)}
	bind := func(strategy execution.StrategyID, lot execution.VirtualLotID, m execution.TimeSeriesMetadata) error {
		return f.client.BindTimeSeriesTakeover(ctx, f.bridge.Account, execution.TimeSeriesTakeoverBinding{Strategy: strategy, Lot: lot, Metadata: m})
	}
	if bind("ts", "external/unknown", meta) == nil || bind("other", adopted.Lot, meta) == nil {
		t.Fatal("binding accepted a lot that is not an adoption of the strategy")
	}
	short := meta
	short.Short = true
	if bind("ts", adopted.Lot, short) == nil {
		t.Fatal("binding accepted a direction that differs from the adopted position")
	}
	if err := bind("ts", adopted.Lot, meta); err != nil {
		t.Fatal(err)
	}
	if err := bind("ts", adopted.Lot, meta); err != nil {
		t.Fatal("identical rebinding must be idempotent", err)
	}
	changed := meta
	changed.OrderID = 78
	if bind("ts", adopted.Lot, changed) == nil {
		t.Fatal("rebinding with another identity accepted")
	}
	if got := page("ts"); len(got.Records) != 1 || got.Records[0].Metadata.OrderID != 77 {
		t.Fatalf("bound metadata not projected: %+v", got.Records)
	}
}
