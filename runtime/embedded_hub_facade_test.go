package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// embeddedHubFixture is an embedded production robot with the Hub order manager: ONE
// SharedAccount owned by the process, borrowed by the runtime (SharedExecution)
// and served to a HubOrderMgr through an in-process Hub port. No SharedOrderMgr
// exists for the account.
type embeddedHubFixture struct {
	process *Process
	key     execution.AccountKey
	adapter *runner.PaperAdapter
	hub     *execution.EmbeddedHub
	bridge  *biz.HubOrderBridgeConfig
	rt      *Runtime
	manager *biz.HubOrderMgr
	price   decimal.Decimal
}

func newEmbeddedHubFixture(t *testing.T, mutate func(*biz.SharedOrderBridgeConfig)) *embeddedHubFixture {
	t.Helper()
	f := &embeddedHubFixture{price: decimal.NewFromInt(100)}
	dir := t.TempDir()
	f.process = NewProcess()
	t.Cleanup(f.process.Close)
	instrument := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	var err error
	f.adapter, err = runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	f.key = execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USDT"}
	opts := &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: f.adapter, AuthoritativeSnapshot: true}
	bridge := biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC": instrument}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
		now = time.Now().UnixMilli()
		return execution.VisibleQuote{Bid: f.price, Ask: f.price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 5000}, nil
	}, IntentTTLMS: 5000}
	if mutate != nil {
		mutate(&bridge)
	}
	// Same order as runTimeSeriesExecutionAccount: borrow first, Hub second,
	// runtime with the SAME options last.
	borrow, err := f.process.BorrowAccount(f.key, *opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(borrow.Release)
	f.hub, err = execution.NewEmbeddedHub(context.Background(), borrow.Service(), 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.hub.Close(); err != nil {
			t.Error(err)
		}
	})
	f.bridge = &biz.HubOrderBridgeConfig{Client: f.hub.Port(), Account: f.key, Bridge: bridge, WriterID: "ts-local-test", StatePath: filepath.Join(dir, "robot", "ts-local.json"), LocalAccount: true}
	f.rt, err = f.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvProd, AccountOwnerKey: &f.key, SharedExecution: opts, SharedMarketData: true, HubOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	f.rt.Clock.SetTimeMS(now)
	account := f.rt.SharedExecution()
	for _, event := range []execution.CashEvent{{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: now}, {ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(1000)}}, AtMS: now}} {
		if err := account.CashEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := account.Reconcile("startup", now); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), nil, false)
	manager, ok := biz.GetOdMgrWithState(f.rt.Trading, "default").(*biz.HubOrderMgr)
	if !ok {
		t.Fatalf("local-account bridge must install HubOrderMgr, got %T", biz.GetOdMgrWithState(f.rt.Trading, "default"))
	}
	f.manager = manager
	t.Cleanup(func() { manager.CleanUp() })
	return f
}

func (f *embeddedHubFixture) order(t *testing.T, id int64) *ormo.InOutOrder {
	t.Helper()
	if err := f.manager.UpdateByDataSeries(nil, nil); err != nil {
		t.Fatal(err)
	}
	orders, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	od := orders[id]
	if od == nil {
		t.Fatal("missing projected order", id)
	}
	return od.Clone()
}

func TestEmbeddedHubFacadeEntryExitRestartAndMetrics(t *testing.T) {
	f := newEmbeddedHubFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	req := &strat.EnterReq{CommandID: "open", Tag: "embedded"}
	od, err := f.manager.EnterOrder(symbol, "1m", req)
	if err != nil || od == nil || od.Enter.Filled != 1 || od.Enter.Average != 100 {
		t.Fatalf("entry: %+v %v", od, err)
	}
	if f.adapter.Metrics().Fills != 1 {
		t.Fatalf("fills: %d", f.adapter.Metrics().Fills)
	}
	// The dashboard reads the ledger once, as a local (credential-holding) account.
	currency, metrics, backed, remote := biz.ExecutionAccountBalance(f.rt.BizDeps(), "default")
	if currency != "USDT" || !backed || remote || !metrics.Valid || metrics.Equity != 1000 {
		t.Fatalf("metrics: %s %+v backed=%v remote=%v", currency, metrics, backed, remote)
	}
	if od, err = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"}); err != nil || od.Status != ormo.InOutStatusFullExit || od.Exit == nil || od.Exit.Filled != 1 {
		t.Fatalf("exit: %+v %v", od, err)
	}
	if err := f.manager.CleanUp(); err != nil {
		t.Fatal(err)
	}
	restarted, e := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.CleanUp()
	replay, err := restarted.EnterOrder(symbol, "1m", req)
	if err != nil || replay == nil || replay.ID != od.ID || f.adapter.Metrics().Fills != 2 {
		t.Fatalf("journal reload lost command identity: %+v %v fills=%d", replay, err, f.adapter.Metrics().Fills)
	}
}

func TestEmbeddedHubFacadeStopBarsExpiryThroughFeed(t *testing.T) {
	f := newEmbeddedHubFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "expires", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 2})
	if err != nil || od == nil || od.Enter.Filled != 0 {
		t.Fatalf("resting limit: %+v %v", od, err)
	}
	base := time.Now().UnixMilli()
	for i := int64(1); i <= 2; i++ {
		// A warm-up or open bar never counts; the closed ones do.
		for _, row := range []*orm.DataSeries{
			{Source: "kline", Sid: 1, TimeFrame: "1m", EndMS: base + i*60000, Closed: true, IsWarmUp: true},
			{Source: "kline", Sid: 1, TimeFrame: "1m", EndMS: base + i*60000, Closed: false},
		} {
			if err := f.manager.UpdateByDataSeries(nil, row); err != nil {
				t.Fatal(err)
			}
		}
		if got := f.order(t, od.ID); got.Status == ormo.InOutStatusDelete {
			t.Fatalf("order expired on bar %d before its deadline", i-1)
		}
		if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", EndMS: base + i*60000, Closed: true}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := f.order(t, od.ID)
		if got.Status == ormo.InOutStatusDelete && got.Enter.Filled == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("StopBars did not expire the resting entry: %+v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if f.adapter.Metrics().Fills != 0 {
		t.Fatal("expired entry reached the venue")
	}
}

func TestEmbeddedHubFacadePendingTickFillsRestingLimit(t *testing.T) {
	f := newEmbeddedHubFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if err != nil || od == nil || od.Enter.Filled != 0 {
		t.Fatalf("resting limit: %+v %v", od, err)
	}
	f.price = decimal.NewFromInt(90)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := f.order(t, od.ID); got.Enter.Filled == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the embedded hub tick never advanced the resting limit")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEmbeddedHubFacadeRejectionDropsCleanly(t *testing.T) {
	f := newEmbeddedHubFixture(t, func(b *biz.SharedOrderBridgeConfig) {
		instrument := b.Instruments["BTC"]
		instrument.MinSteps = 1000
		b.Instruments["BTC"] = instrument
	})
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "tiny", Tag: "tiny", Amount: 1})
	if err != nil || od != nil {
		t.Fatalf("below-minimum entry must be (nil, nil): %v %v", od, err)
	}
	if e := f.manager.LastError(); e != nil || f.adapter.Metrics().Fills != 0 {
		t.Fatalf("rejection left a trace: %v fills=%d", e, f.adapter.Metrics().Fills)
	}
}

func TestLocalAccountBridgeExclusivity(t *testing.T) {
	f := newEmbeddedHubFixture(t, nil)
	deps := f.rt.BizDeps()
	if deps.SharedExecution == nil || deps.SharedOrderBridge != nil || deps.HubOrderBridge != f.bridge {
		t.Fatalf("deps must carry the account and ONE facade bridge: %+v", deps)
	}
	// Without the explicit flag the old exclusivity stands.
	plain := *f.bridge
	plain.LocalAccount = false
	if m, err := biz.NewHubOrderMgr(deps, &plain, nil); err == nil {
		m.CleanUp()
		t.Fatal("Hub bridge without LocalAccount accepted SharedExecution")
	}
	// A socket client can never claim to serve the runtime's own account.
	socket := *f.bridge
	socket.Client = execution.NewHubClient(filepath.Join(t.TempDir(), "none.sock"))
	if err := socket.Validate(); err == nil {
		t.Fatal("LocalAccount accepted a socket client")
	}
	// Another account's runtime cannot reuse the flag.
	other := *f.bridge
	other.Account.Account = "other"
	if m, err := biz.NewHubOrderMgr(deps, &other, nil); err == nil {
		m.CleanUp()
		t.Fatal("LocalAccount accepted a bridge for a different account")
	}
	key := f.key
	opts := &biz.SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "x.db"), SenderLeaseDir: t.TempDir(), Adapter: f.adapter, AuthoritativeSnapshot: true}
	cases := map[string]Options{
		"without shared execution": {Mode: core.RunModeLive, Env: core.RunEnvProd, AccountOwnerKey: &key, SharedMarketData: true, HubOrderBridge: f.bridge},
		"with shared bridge":       {Mode: core.RunModeLive, Env: core.RunEnvProd, AccountOwnerKey: &key, SharedExecution: opts, SharedMarketData: true, SharedOrderBridge: &f.bridge.Bridge, HubOrderBridge: f.bridge},
		"dry run":                  {Mode: core.RunModeLive, Env: core.RunEnvDryRun, AccountOwnerKey: &key, SharedExecution: opts, SharedMarketData: true, HubOrderBridge: f.bridge},
		"backtest":                 {Mode: core.RunModeBackTest, Env: core.RunEnvProd, AccountOwnerKey: &key, SharedExecution: opts, SharedMarketData: true, HubOrderBridge: f.bridge},
	}
	for name, options := range cases {
		if rt, err := NewProcess().NewRuntime(options); err == nil {
			rt.Close()
			rt.Join()
			t.Errorf("%s: runtime accepted a local-account Hub bridge", name)
		}
	}
	plainRemote := Options{Mode: core.RunModeLive, Env: core.RunEnvProd, AccountOwnerKey: &key, SharedExecution: opts, SharedMarketData: true, HubOrderBridge: &plain}
	if rt, err := NewProcess().NewRuntime(plainRemote); err == nil {
		rt.Close()
		rt.Join()
		t.Error("a plain Hub bridge was accepted next to SharedExecution")
	}
}
