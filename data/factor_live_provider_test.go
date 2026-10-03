package data_test

import (
	"context"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"path/filepath"
	"testing"
	"time"
)

type providerFactorOutput struct {
	targets    []*factor.TargetPortfolio
	executions []int64
}

func (o *providerFactorOutput) Decision(_ factor.Frame, p *factor.TargetPortfolio, _ []factor.Diagnostic) error {
	if p != nil {
		o.targets = append(o.targets, p)
	}
	return nil
}
func (*providerFactorOutput) Evaluation(research.Report) error { return nil }
func (o *providerFactorOutput) Executed(p *factor.TargetPortfolio, _ backtest.State, at int64) error {
	if at < p.Spec().ExecutableAt {
		return fmt.Errorf("execution preceded target availability")
	}
	o.executions = append(o.executions, at)
	return nil
}

func TestLiveProviderClosedKlinesWarmFactorAndFillSharedPaper(t *testing.T) {
	t.Run("factor-and-price-union", func(t *testing.T) { testLiveProviderFactorPipeline(t, true, false) })
	t.Run("missing-price-feed-preserves-target-without-fill", func(t *testing.T) { testLiveProviderFactorPipeline(t, false, false) })
}

func TestSubWarmPairsRealtimeHistoryInitializesFactorWithoutOrders(t *testing.T) {
	testLiveProviderFactorPipeline(t, true, true)
}

func testLiveProviderFactorPipeline(t *testing.T, observePrices, historicalWarmup bool) {
	t.Helper()
	const hour, minute int64 = 3600000, 60000
	start := int64(1700002800000)
	if historicalWarmup {
		start = time.Now().UnixMilli()/hour*hour - 25*hour
	}
	ctx := context.Background()
	sids := []int32{1, 2, 3}
	sidMap := map[int32]string{}
	symbols := []*orm.ExSymbol{}
	state := orm.NewSymbolStateWithIdentity("fixture", "linear")
	instruments := map[int32]execution.Instrument{}
	for _, sid := range sids {
		name := fmt.Sprintf("asset%d", sid)
		sidMap[sid] = name
		symbol := &orm.ExSymbol{ID: sid, Exchange: "fixture", Market: "linear", Symbol: name}
		if err := state.CacheExSymbolChecked(symbol); err != nil {
			t.Fatal(err)
		}
		symbols = append(symbols, symbol)
		instruments[sid] = execution.Instrument{ID: name, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.RequireFromString("0.01"), MoneyScale: 8}
	}
	dir := t.TempDir()
	c := runner.Config{Mode: runner.Trade, StrategyID: "s", AccountID: "a", InitialNAV: 10000, LatencyMS: 1, ExpiryMS: 2 * minute, DecisionInterval: hour, Prices: runner.PriceStream{Source: "kline", Frequency: "1m", Field: "close"}, Factor: research.DefaultMomentumVolConfig(), Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Version: "u", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}, SIDMap: sidMap, Schemas: map[string]string{"kline": "s1"}, SourceVersions: map[string]string{"kline": "v1"}, VisibilityPolicy: "available-at", AdjustmentVersion: "raw"}, Manifest: research.ManifestSpec{CodeRevision: "test", Currency: "USD", Portfolio: research.PortfolioDefinition{K: 1, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Costs: research.CostSpec{FeeRate: .001, SlippageRate: .001, FundingPolicy: "explicit-zero"}, Labels: []research.LabelSpec{{Name: "1h", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 8760, Overlapping: true}}}, Execution: runner.ExecutionConfig{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "lease"), Instruments: instruments, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(100000), MaxVirtualGross: decimal.NewFromInt(100000), StrategyGrossLimit: decimal.NewFromInt(20000)}}
	sink, closeSink, err := runner.NewPaperSink(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSink()
	clock := btime.NewClockState(true, nil)
	if historicalWarmup {
		clock = btime.NewClockState(false, nil)
		c.ExpiryMS = 2 * hour
		c.Prices = runner.PriceStream{Source: "tick", Frequency: "event", Field: "price"}
	}
	deps := &data.RuntimeDeps{Core: &core.State{LiveMode: true}, Clock: clock, Symbols: state, Strategies: strat.NewState(), ExchangeName: "fixture", MarketType: "linear"}
	out := &providerFactorOutput{}
	live, err := runner.NewLive(c, sink, clock.TimeMS, out)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	closedHours := map[int32]int{}
	var callbackErr error
	consume := func(series *orm.DataSeries) {
		if callbackErr != nil {
			return
		}
		if !series.Closed || (series.TimeFrame != "event" && series.EndMS != series.TimeMS+map[string]int64{"1m": minute, "1h": hour}[series.TimeFrame]) {
			callbackErr = fmt.Errorf("noncanonical closed bar: %+v", series)
			return
		}
		if raw, ok := series.Values["raw"].(int64); !ok || raw != int64(series.Sid) || series.Values["nullable"] != nil || series.Values["tag"] != "kept" {
			callbackErr = fmt.Errorf("arbitrary fields lost: %+v", series.Values)
			return
		}
		if _, ok := series.Values["nullable"]; !ok {
			callbackErr = fmt.Errorf("NULL field removed")
			return
		}
		if series.TimeFrame == "1h" {
			closedHours[series.Sid]++
		}
		if series.TimeFrame == "1m" && !observePrices {
			return
		}
		now := clock.TimeMS()
		row := factor.VersionRecord{Series: *series, EventTime: series.EndMS, AvailableAt: now, IngestedAt: now, Revision: 1, SourceVersion: "v1"}
		if series.IsWarmUp {
			callbackErr = live.Warmup(ctx, row)
		} else {
			callbackErr = live.Observe(ctx, row)
		}
		// Equivalent to BindFactorLive's closed-bar branch; its exact closure is
		// covered in runtime tests, which cannot share private data test helpers.
		if callbackErr == nil && !series.IsWarmUp && series.EndMS%hour == 0 {
			callbackErr = live.Flush(ctx, series.EndMS)
		}
	}
	if historicalWarmup {
		history := make(map[int32][]*orm.DataSeries)
		items := make(map[string]map[string]int)
		for _, sid := range sids {
			items[sidMap[sid]] = map[string]int{"1h": 24}
			for bar := int64(0); bar < 24; bar++ {
				price := 100 + float64(sid)*float64(bar) + float64((bar*bar+int64(sid))%7)
				history[sid] = append(history[sid], &orm.DataSeries{Source: "kline", TimeMS: start + bar*hour, EndMS: start + (bar+1)*hour, Values: map[string]any{"close": price, "raw": int64(sid), "nullable": nil, "tag": "kept"}})
			}
		}
		provider := data.FactorLiveWarmupFixture(t, deps, symbols, history, consume)
		if _, _, _, err := provider.SubWarmPairs(items, true, nil); err != nil {
			t.Fatal(err)
		}
		if callbackErr != nil {
			t.Fatal(callbackErr)
		}
		if len(out.targets) != 0 || sink.Paper.Metrics().Fills != 0 {
			t.Fatal("historical warmup admitted orders")
		}
		if clock.TimeMS() < start+25*hour {
			t.Fatal("warmup rewound realtime receipt clock")
		}
		for _, sid := range sids {
			price := 100 + float64(sid)*24 + float64((24*24+int(sid))%7)
			consume(&orm.DataSeries{Source: "kline", Sid: sid, TimeFrame: "1h", TimeMS: start + 24*hour, EndMS: start + 25*hour, Closed: true, Values: map[string]any{"close": price, "raw": int64(sid), "nullable": nil, "tag": "kept"}})
		}
		if callbackErr != nil {
			t.Fatal(callbackErr)
		}
		if len(out.targets) != 1 {
			t.Fatalf("first live grid ignored warmed state: %d targets", len(out.targets))
		}
		time.Sleep(2 * time.Millisecond)
		for _, sid := range sids {
			now := clock.TimeMS()
			consume(&orm.DataSeries{Source: "tick", Sid: sid, TimeFrame: "event", TimeMS: now, EndMS: now, Closed: true, Values: map[string]any{"price": 100.0, "raw": int64(sid), "nullable": nil, "tag": "kept"}})
		}
		if callbackErr != nil {
			t.Fatal(callbackErr)
		}
		if sink.Paper.Metrics().Fills == 0 {
			t.Fatal("first live target did not execute on later current quotes")
		}
		return
	}
	_, push := data.FactorLiveProviderFixture(t, deps, symbols, consume)
	for bar := int64(0); bar < 26*60+1; bar++ {
		end := start + (bar+1)*minute
		clock.SetTimeMS(end)
		fillsBefore := sink.Paper.Metrics().Fills
		for _, sid := range sids {
			price := 100 + float64(sid)*float64(bar)/60 + float64((bar/60*bar/60+int64(sid))%7)
			row := &orm.DataSeries{Source: "kline", TimeMS: start + bar*minute, EndMS: end, Values: map[string]any{"open": price, "high": price, "low": price, "close": price, "volume": float64(1), "raw": int64(sid), "nullable": nil, "tag": "kept"}}
			push(&data.SeriesMsg{ExgName: "fixture", Market: "linear", Pair: sidMap[sid], NotifySeries: data.NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{row}}})
			if callbackErr != nil {
				t.Fatal(callbackErr)
			}
			if end == start+25*hour && sid < 3 && len(out.targets) != 0 {
				t.Fatal("target before final SID completed warmup barrier")
			}
		}
		if end <= start+25*hour && sink.Paper.Metrics().Fills != 0 {
			t.Fatal("fill before warmed decision and strictly later minute")
		}
		if end%hour == 0 && sink.Paper.Metrics().Fills != fillsBefore {
			t.Fatal("same-decision minute filled a new target")
		}
	}
	for _, sid := range sids {
		if closedHours[sid] != 26 {
			t.Fatalf("SID %d closed hours=%d", sid, closedHours[sid])
		}
	}
	if len(out.targets) != 2 {
		t.Fatalf("warmed factor targets=%d, want 2", len(out.targets))
	}
	if !observePrices {
		if len(out.executions) != 0 || sink.Paper.Metrics().Fills != 0 {
			t.Fatal("missing execution stream produced a fill")
		}
		return
	}
	if len(out.executions) == 0 || sink.Paper.Metrics().Fills == 0 {
		t.Fatalf("pipeline did not execute: targets=%d executions=%v fills=%+v", len(out.targets), out.executions, sink.Paper.Metrics())
	}
	metrics := sink.Paper.Metrics()
	if metrics.LastSourceAt <= out.targets[len(out.targets)-1].Spec().DecisionTime {
		t.Fatal("same-decision quote filled target")
	}
	snap, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Lots) != 2 || len(snap.Orders) != 0 {
		t.Fatalf("shared ledger missing completed long/short fills: %+v", snap)
	}
	virtual := map[string]int64{}
	long, short := false, false
	for _, lot := range snap.Lots {
		virtual[lot.Instrument.ID] += lot.SignedSteps
		long = long || lot.SignedSteps > 0
		short = short || lot.SignedSteps < 0
	}
	for _, position := range snap.ActualPositions {
		if virtual[position.Instrument.ID] != position.SignedSteps {
			t.Fatalf("actual/virtual mismatch: %+v", snap)
		}
	}
	if !long || !short {
		t.Fatal("missing long/short ledger exposure")
	}
	t.Logf("closed hours=%v, targets=%d, executions=%v, fills=%d", closedHours, len(out.targets), out.executions, metrics.Fills)
}
