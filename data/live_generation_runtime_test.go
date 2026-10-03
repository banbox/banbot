package data_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

type generationRuntimeExchange struct{ banexg.BanExchange }

func (*generationRuntimeExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "fixture", MarketType: "linear"}
}

type generationRuntimeSink struct{ targets []*factor.TargetPortfolio }

func (*generationRuntimeSink) StrategyNAV(context.Context, int64) (float64, error) { return 10000, nil }
func (s *generationRuntimeSink) ProcessSnapshot(_ context.Context, p *factor.TargetPortfolio, _ map[int32]backtest.Quote, _ int64) error {
	s.targets = append(s.targets, p)
	return nil
}

func generationRuntimeFixture(t *testing.T) (*runtimectx.Runtime, runner.Config, map[int32]*orm.ExSymbol) {
	t.Helper()
	dir := t.TempDir()
	adapter, err := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	instrument := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USD"}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC": instrument}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, Quote: func(_ string, at int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: at, ReceivedMS: at, ValidUntilMS: at + 600000, Bar: 1}, nil
	}, IntentTTLMS: 600000}
	process := runtimectx.NewProcess()
	t.Cleanup(process.Close)
	rt, err := process.NewRuntime(runtimectx.Options{Mode: core.RunModeBackTest, Config: &config.Config{Exchange: &config.ExchangeConfig{Name: "fixture"}, MarketType: "linear"}, Exchange: &generationRuntimeExchange{}, AccountOwnerKey: &key, SharedExecution: &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "lease"), Adapter: adapter, AuthoritativeSnapshot: true}, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(180002)
	if err := rt.SharedExecution().CashEvent(execution.CashEvent{ID: "generation-deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(1000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(1000)}}, AtMS: rt.Clock.TimeMS()}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SharedExecution().Reconcile("generation-start", rt.Clock.TimeMS()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Mode, c.AccountID = runner.Trade, "default"
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.DecisionInterval, c.LatencyMS, c.ExpiryMS = 60000, 1, 120000
	c.Prices = runner.PriceStream{Source: "kline", Frequency: "1m", Field: "close"}
	c.Snapshot.Universe = factor.Universe{Version: "u", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Evaluation: []int32{1, 2}, Tradable: []int32{1, 2}, Tracked: []int32{1, 2}}
	c.Snapshot.SIDMap = map[int32]string{1: "BTC", 2: "ETH"}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"value"}, Weights: map[string]float64{"value": 1}}
	c.Plan, err = factor.New().Add("value", factor.EMA(factor.Field("kline", "close", "1m"), 3)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	symbols := map[int32]*orm.ExSymbol{}
	for sid, name := range c.Snapshot.SIDMap {
		symbol := &orm.ExSymbol{ID: sid, Symbol: name, Exchange: "fixture", Market: "linear"}
		if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			t.Fatal(err)
		}
		symbols[sid] = symbol
	}
	return rt, c, symbols
}

func generationRuntimeMapper(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
	if value, exists := series.Values["integer"]; exists && value != int64(9007199254740993) {
		return factor.VersionRecord{}, errors.New("raw integer narrowed")
	}
	if value, exists := series.Values["nullable"]; exists && value != nil {
		return factor.VersionRecord{}, errors.New("NULL changed")
	}
	revision := uint64(1)
	if value, exists := series.Values["revision"]; exists {
		revision = value.(uint64)
	}
	return factor.VersionRecord{Series: *series, EventTime: series.EndMS, AvailableAt: series.EndMS, IngestedAt: received, Revision: revision, SourceVersion: "v1"}, nil
}

func TestLiveKlineGenerationUpdateKeepsFixedLegacyWarmupAndRevisions(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[failure], func(t *testing.T) {
			rt, c, symbols := generationRuntimeFixture(t)
			warm, live := 0, 0
			mapped := 0
			mapper := func(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
				mapped++
				return generationRuntimeMapper(series, received)
			}
			job := &strat.StratJob{Account: "default", Symbol: symbols[1], TimeFrame: "ws", Strat: &strat.TradeStrat{Name: "legacy"}}
			sub := &strat.DataSub{Source: "kline", TimeFrame: "1m", ExSymbol: symbols[1], WarmupNum: 2, Fields: []string{"close", "integer", "nullable"}}
			job.Strat.OnDataSubs = func(*strat.StratJob) []*strat.DataSub { return []*strat.DataSub{sub} }
			job.Strat.OnData = func(_ *strat.StratJob, event strat.DataEvent) {
				if event.IsWarmUp {
					warm++
				} else {
					live++
				}
			}
			if err := rt.BindFactorLegacyJobs([]*strat.StratJob{job}, []*strat.DataSub{sub}); err != nil {
				t.Fatal(err)
			}
			harness, provider := data.NewFactorLiveGenerationHarness(rt.DataDeps())
			harness.History = func(index int, symbol *orm.ExSymbol, tf string, count int) ([]*orm.DataSeries, error) {
				if failure && index == 1 {
					return nil, errors.New("candidate kline history unavailable")
				}
				var rows []*orm.DataSeries
				for i := count; i > 0; i-- {
					at := int64(180000 - i*60000)
					rows = append(rows, &orm.DataSeries{Source: "kline", Sid: symbol.ID, ExSymbol: symbol, TimeFrame: tf, TimeMS: at, EndMS: at + 60000, Closed: true, Values: map[string]any{"close": 100 + float64(symbol.ID)*float64(i), "extra": float64(i + 1), "integer": int64(9007199254740993), "nullable": nil, "revision": uint64(1)}})
				}
				return rows, nil
			}
			oldSink := &generationRuntimeSink{}
			old, err := runner.NewLive(c, oldSink, rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := rt.InstallFactorsLive(provider, []*runner.Live{old}, []runner.Config{c}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper})
			if err != nil {
				t.Fatal(err)
			}
			if warm != 2 || rt.Clock.TimeMS() != 180002 {
				t.Fatalf("warmup polluted sessions/clock: warm=%d clock=%d", warm, rt.Clock.TimeMS())
			}
			rt.Clock.SetTimeMS(240002)
			row := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbols[1], TimeFrame: "1m", TimeMS: 180000, EndMS: 240000, Closed: true, Values: map[string]any{"close": 104.0, "extra": 4.0, "integer": int64(9007199254740993), "nullable": nil, "revision": uint64(1)}}
			if err := harness.Emit(0, row); err != nil {
				t.Fatal(err)
			}
			if live != 1 {
				t.Fatal("initial canonical legacy observation missing")
			}
			// Prepare at the original decision boundary using fresh complete history.
			harness.History = func(index int, symbol *orm.ExSymbol, tf string, count int) ([]*orm.DataSeries, error) {
				if failure && index == 1 {
					return nil, errors.New("candidate kline history unavailable")
				}
				var rows []*orm.DataSeries
				for i := count; i > 0; i-- {
					at := int64(240000 - i*60000)
					closeValue, extra := 100+float64(i+int(symbol.ID)), float64(i+1)
					if symbol.ID == 1 && at == 180000 {
						closeValue, extra = 104, 4
					}
					rows = append(rows, &orm.DataSeries{Source: "kline", Sid: symbol.ID, ExSymbol: symbol, TimeFrame: tf, TimeMS: at, EndMS: at + 60000, Closed: true, Values: map[string]any{"close": closeValue, "extra": extra, "integer": int64(9007199254740993), "nullable": nil, "revision": uint64(1)}})
				}
				return rows, nil
			}
			nextCfg := c
			nextCfg.Plan, err = factor.New().Add("value", factor.EMA(factor.Field("kline", "extra", "1m"), 3)).Compile()
			if err != nil {
				t.Fatal(err)
			}
			next, err := runner.NewLive(nextCfg, &generationRuntimeSink{}, rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			previousPlan := owner.Plan()
			committed, err := owner.Update(context.Background(), []*runner.Live{next}, []runner.Config{nextCfg}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper})
			if committed == failure || (err != nil) != failure {
				t.Fatalf("generation update: committed=%v err=%v", committed, err)
			}
			if warm != 2 || live != 1 {
				t.Fatalf("candidate history changed legacy state: warm=%d live=%d", warm, live)
			}
			if failure {
				if owner.Plan() != previousPlan || harness.Stopped(0) || !harness.Stopped(1) {
					t.Fatal("rollback replaced/leaked physical generation")
				}
				return
			}
			if !harness.Stopped(0) || harness.Stopped(1) {
				t.Fatal("physical generation ownership not swapped")
			}
			if err := harness.Emit(1, row); err != nil {
				t.Fatal(err)
			}
			if live != 1 {
				t.Fatal("overlap duplicated old revision")
			}
			corrected := *row
			corrected.Values = map[string]any{"close": 105.0, "extra": 5.0, "integer": int64(9007199254740993), "nullable": nil, "revision": uint64(2)}
			beforeCorrection := mapped
			if err := harness.Emit(1, &corrected); err != nil {
				t.Fatal(err)
			}
			if live != 2 {
				t.Fatal("same-timestamp new revision lost")
			}
			if mapped-beforeCorrection != 1 {
				t.Fatalf("factor and legacy delivery remapped accepted correction: calls=%d", mapped-beforeCorrection)
			}
			if err := harness.Emit(1, row); err != nil {
				t.Fatal(err)
			}
			cached := harness.Cached(1, "BTC", "1m", 180000)
			if live != 2 || cached == nil || cached.Values["close"] != 105.0 {
				t.Fatal("older revision overwrote accepted raw cache/legacy state")
			}
			another := *row
			another.TimeMS, another.EndMS = 120000, 180000
			another.Values = map[string]any{"close": 106.0, "extra": 6.0, "integer": int64(9007199254740993), "nullable": nil, "revision": uint64(2)}
			if err := harness.Emit(1, &another); err != nil {
				t.Fatal(err)
			}
			cached = harness.Cached(1, "BTC", "1m", 180000)
			if live != 3 || cached == nil || cached.Values["close"] != 105.0 {
				t.Fatal("another correction resurrected stale raw revision")
			}
			if got := rt.Market.PairCopied.GetPairCopied("BTC")[0]; got != 240000 {
				t.Fatalf("current-generation progress absent/regressed: %d", got)
			}
			owner.Stop()
			if err := owner.Join(); err != nil {
				t.Fatal(err)
			}
			if !harness.Stopped(1) {
				t.Fatal("current physical generation leaked on Stop/Join")
			}
		})
	}
}

func TestLiveKlineGenerationLegacyOnlyStreamRejectsStaleRawRevision(t *testing.T) {
	rt, c, symbols := generationRuntimeFixture(t)
	var err error
	c.Plan, err = factor.New().Add("value", factor.Field("kline", "close", "1m")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	symbol := &orm.ExSymbol{ID: 3, Symbol: "SOL", Exchange: "fixture", Market: "linear"}
	if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
		t.Fatal(err)
	}
	symbols[3] = symbol
	live := 0
	job := &strat.StratJob{Account: "default", Symbol: symbol, TimeFrame: "ws", Strat: &strat.TradeStrat{Name: "legacy"}}
	sub := &strat.DataSub{Source: "kline", TimeFrame: "1m", ExSymbol: symbol, Fields: []string{"close"}}
	job.Strat.OnDataSubs = func(*strat.StratJob) []*strat.DataSub { return []*strat.DataSub{sub} }
	job.Strat.OnData = func(*strat.StratJob, strat.DataEvent) { live++ }
	if err := rt.BindFactorLegacyJobs([]*strat.StratJob{job}, []*strat.DataSub{sub}); err != nil {
		t.Fatal(err)
	}
	h, p := data.NewFactorLiveGenerationHarness(rt.DataDeps())
	engine, err := runner.NewLive(c, &generationRuntimeSink{}, rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := rt.InstallFactorsLive(p, []*runner.Live{engine}, []runner.Config{c}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){generationRuntimeMapper})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { owner.Stop(); _ = owner.Join() }()
	makeRow := func(revision uint64, value float64) *orm.DataSeries {
		return &orm.DataSeries{Source: "kline", Sid: 3, ExSymbol: symbol, TimeFrame: "1m", TimeMS: 120000, EndMS: 180000, Closed: true, Values: map[string]any{"close": value, "revision": revision}}
	}
	if err := h.Emit(0, makeRow(2, 105)); err != nil {
		t.Fatal(err)
	}
	if err := h.Emit(0, makeRow(1, 100)); err != nil {
		t.Fatal(err)
	}
	cached := h.Cached(0, "SOL", "1m", 120000)
	if live != 1 || cached == nil || cached.Values["close"] != 105.0 {
		t.Fatal("legacy-only source allowed an old revision to replace raw cache")
	}
}
