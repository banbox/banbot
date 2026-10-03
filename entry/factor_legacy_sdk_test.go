package entry

import (
	"context"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
	"path/filepath"
	"testing"
	"time"
)

func TestFactorLegacySDKDelayedQuotesAllowFreshEntryAndExplicitExit(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	i := execution.Instrument{ID: "BTC/USD", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USD"}
	ex := &legacyEntryExchange{liveEntryExchange: liveEntryExchange{trades: make(chan *banexg.MyTrade)}, positions: map[string]float64{}, orders: map[string]*banexg.Order{}, prices: map[string]float64{i.ID: 100}, books: map[string]int{}, cash: 1000, quoteDelay: 3 * time.Millisecond}
	adapter, err := execution.NewBanexgAdapter(ctx, ex, execution.BanexgAdapterConfig{Account: key, Instruments: []execution.BanexgInstrument{{Symbol: i.ID, Instrument: i}}, Transport: &liveEntryTransport{key: key}})
	if err != nil {
		t.Fatal(err)
	}
	risk := execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{i.ID: i}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: risk, IntentTTLMS: 1000, QuoteContext: func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return adapter.Observe(ctx, id)
	}}
	p := runtime.NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(runtime.Options{Mode: core.RunModeLive, AccountOwnerKey: &key, SharedExecution: &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: adapter, AuthoritativeSnapshot: true}, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	account := rt.SharedExecution()
	now := time.Now().UnixMilli()
	for _, event := range []execution.CashEvent{{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: now}, {ID: "capital", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(1000)}}, AtMS: now}} {
		if err := account.CashEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := account.Reconcile("sdk-startup", now); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	manager := biz.GetOdMgrWithState(rt.Trading, "default")
	symbol := &orm.ExSymbol{ID: 1, Symbol: i.ID}
	od, orderErr := manager.EnterOrder(symbol, "ws", &strat.EnterReq{StratName: "legacy", Tag: "fresh", Amount: 1})
	if orderErr != nil {
		t.Fatal(orderErr)
	}
	if err := account.RecoverPersisted(ctx); err != nil {
		t.Fatal(err)
	}
	if err := account.Reconcile("sdk-entry", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExitOpenOrders(i.ID, &strat.ExitReq{StratName: "legacy", Tag: "explicit", OrderID: od.ID}); err != nil {
		t.Fatal(err)
	}
	if err := account.RecoverPersisted(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Lots) != 0 || len(ex.orders) != 2 || ex.positions[i.ID] != 0 {
		t.Fatal("SDK fresh TS entry/explicit exit did not settle", snapshot.Lots, len(ex.orders), ex.positions)
	}
}
