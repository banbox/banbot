package runner

import (
	"context"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

type capture struct {
	targets  map[int64]map[int32]float64
	reports  []research.Report
	executed []int64
}

func (o *capture) Decision(f factor.Frame, p *factor.TargetPortfolio, _ []factor.Diagnostic) error {
	if p != nil {
		o.targets[f.DecisionTime] = p.Targets()
	}
	return nil
}
func (o *capture) Evaluation(r research.Report) error { o.reports = append(o.reports, r); return nil }
func (o *capture) Executed(_ *factor.TargetPortfolio, _ backtest.State, at int64) error {
	o.executed = append(o.executed, at)
	return nil
}
func archiveConfig(t testing.TB, perturb bool) Config {
	t.Helper()
	store, _ := factor.NewVersionStore(3000)
	sids := make([]int32, 24)
	symbols := map[int32]string{}
	const hour int64 = 3600000
	put := func(source, freq string, sid int32, at, available int64, values map[string]any) {
		t.Helper()
		err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: freq, Sid: sid, TimeMS: at, EndMS: at, Closed: true, Values: values}, EventTime: at, AvailableAt: available, IngestedAt: available, Revision: 1, SourceVersion: "v1"})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range sids {
		sid := int32(i + 1)
		sids[i] = sid
		symbols[sid] = fmt.Sprint("asset", sid)
		for bar := int64(1); bar <= 28; bar++ {
			at := bar * hour
			price := 100 + float64(i+1)*float64(bar) + float64((bar*bar+int64(i))%7)
			values := map[string]any{"close": price, "custom": int64(bar), "nullable": nil}
			if sid == 24 && bar == 25 {
				values["close"] = nil
			}
			put("kline", "1h", sid, at, at, values)
			p := price
			if perturb && bar >= 27 {
				p *= 1.7
			}
			if !(sid == 23 && bar == 27) {
				put("tick", "event", sid, at+1, at+1, map[string]any{"price": p})
			}
			put("funding", "event", sid, at+2, at+2, map[string]any{"rate": .0001})
		}
	}
	path := filepath.Join(t.TempDir(), "raw.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	return Config{Mode: Weights, Chunks: []Chunk{{path, hour, 28*hour + 2}}, MaxRecords: 3000, MaxPending: 10, DecisionInterval: hour, LatencyMS: 1, ExpiryMS: 60000, Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Version: "u", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}, SIDMap: symbols, Schemas: map[string]string{"kline": "s1", "tick": "s1", "funding": "s1"}, SourceVersions: map[string]string{"kline": "v1", "tick": "v1", "funding": "v1"}, VisibilityPolicy: "available-at", AdjustmentVersion: "raw"}, Factor: research.DefaultMomentumVolConfig(), Manifest: research.ManifestSpec{CodeRevision: "test", Currency: "USD", Portfolio: research.PortfolioDefinition{K: 5, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Labels: []research.LabelSpec{{Name: "1h", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 8760, Overlapping: true}}, Costs: research.CostSpec{FeeRate: .001, SlippageRate: .001, FundingPolicy: "required-stream"}}, StrategyID: "s", AccountID: "a", InitialNAV: 10000, Prices: PriceStream{"tick", "event", "price"}, FundingSource: "funding"}
}
func TestRealArchivePipelineGoldenAndFutureIsolation(t *testing.T) {
	cfg := archiveConfig(t, false)
	o := &capture{targets: map[int64]map[int32]float64{}}
	r, err := Run(context.Background(), cfg, nil, o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Decisions != 28 || r.Executions == 0 || len(o.reports) == 0 || r.Book.Fees <= 0 || r.Book.Funding == 0 {
		t.Fatalf("pipeline not exercised: %+v", r)
	}
	for _, at := range o.executed {
		if at%3600000 == 0 {
			t.Fatal("same decision execution")
		}
	}
	changed := archiveConfig(t, true)
	other := &capture{targets: map[int64]map[int32]float64{}}
	r2, err := Run(context.Background(), changed, nil, other)
	if err != nil {
		t.Fatal(err)
	}
	for at, targets := range o.targets {
		if at < 27*3600000 && !reflect.DeepEqual(targets, other.targets[at]) {
			t.Fatalf("future leaked at %d", at)
		}
	}
	if r.StrategyHash != r2.StrategyHash || r.ManifestID == r2.ManifestID {
		t.Fatal("definition/lineage identity wrong")
	}
	cfg.Mode = Research
	rr, err := Run(context.Background(), cfg, nil, &capture{targets: map[int64]map[int32]float64{}})
	if err != nil {
		t.Fatal(err)
	}
	if rr.StrategyHash != r.StrategyHash || rr.ManifestID == r.ManifestID {
		t.Fatal("mode identity wrong")
	}
}
func TestRejectAbsentSinkFundingAndBounds(t *testing.T) {
	cfg := archiveConfig(t, false)
	cfg.Mode = Events
	if _, err := Run(context.Background(), cfg, nil, nil); err == nil {
		t.Fatal("events silently downgraded to weights")
	}
	cfg.Mode = Weights
	cfg.FundingSource = ""
	if _, err := Run(context.Background(), cfg, nil, nil); err == nil {
		t.Fatal("absent funding assumed zero")
	}
	cfg.FundingSource = "funding"
	cfg.MaxPending = 1
	if _, err := Run(context.Background(), cfg, nil, nil); err == nil {
		t.Fatal("unbounded label retention")
	}
	cfg.MaxPending = 10
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, cfg, nil, nil); err == nil {
		t.Fatal("cancellation ignored")
	}
}
func paperConfig(t *testing.T, c Config) Config {
	t.Helper()
	dir := t.TempDir()
	c.Execution = ExecutionConfig{StorePath: filepath.Join(dir, "execution.db"), SenderLeaseDir: filepath.Join(dir, "lease"), Instruments: map[int32]execution.Instrument{}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(100000), MaxVirtualGross: decimal.NewFromInt(100000), StrategyGrossLimit: decimal.NewFromInt(20000)}
	for _, sid := range c.Snapshot.Universe.Investable {
		c.Execution.Instruments[sid] = execution.Instrument{ID: c.Snapshot.SIDMap[sid], Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.RequireFromString("0.01"), MoneyScale: 8}
	}
	return c
}
func TestEventsAndTradeDryRunUseSharedLedgerConstraints(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	sink, close, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	r, err := Run(context.Background(), c, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := sink.Account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Executions == 0 || sink.Paper.Metrics().Fills == 0 || snap.Checkpoint <= 2 || len(snap.Lots) == 0 || r.Book.Fees <= 0 {
		t.Fatalf("no ledger execution %+v %+v", r, snap)
	}
	if sink.Paper.Metrics().LastSourceAt%3600000 != 1 || len(snap.Orders) != 0 {
		t.Fatal("unfilled/non-next-event orders")
	}
	for sid, qty := range r.Book.Quantities {
		if math.Abs(qty/.01-math.Round(qty/.01)) > 1e-6 {
			t.Fatalf("SID%d unconstrained quantity %v", sid, qty)
		}
	}
	c2 := paperConfig(t, c)
	c2.Mode = Trade
	paper, done, err := NewPaperSink(context.Background(), c2)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	tr, err := Run(context.Background(), c2, paper, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tr.StrategyHash != r.StrategyHash || tr.ManifestID == r.ManifestID || math.Abs(tr.Book.NAV-r.Book.NAV) > 1e-8 {
		t.Fatal("trade dryrun/ event definition or ledger parity mismatch")
	}
	if _, _, err = NewPaperSink(context.Background(), c); err == nil {
		t.Fatal("existing ledger silently bootstrapped")
	}
}
func TestPaperLedgerHandCalculatedDriftFundingAndDelayedArrival(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	quotes := map[int32]backtest.Quote{1: {AtMS: 11, AvailableAt: 12, Price: 100}, 3: {AtMS: 11, AvailableAt: 12, Price: 300}}
	for sid, q := range quotes {
		if err = sink.ObserveQuote(ctx, sid, q, 12); err != nil {
			t.Fatal(err)
		}
	}
	p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: 1, SnapshotID: "snapshot", PlanHash: "definition", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "budget", Currency: "USD", NAV: 10000}, Mode: factor.Full}, map[int32]float64{1: -.5, 3: .5})
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.ProcessSnapshot(ctx, p, quotes, 12); err != nil {
		t.Fatal(err)
	}
	state, err := sink.StrategyState(ctx, 12)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(state.NAV-9980.004002) > 1e-8 || math.Abs(state.Fees-9.997998) > 1e-8 || math.Abs(state.Slippage-9.998) > 1e-8 || math.Abs(state.Turnover-9998) > 1e-8 || state.Quantities[1] != -50 || state.Quantities[3] != 16.66 {
		t.Fatalf("hand calculated fills: %+v", state)
	}
	for _, sid := range []int32{1, 3} {
		if err = sink.ObserveFunding(ctx, backtest.Funding{ID: fmt.Sprint("funding", sid), SID: sid, AtMS: 13, AvailableAt: 13, Rate: .0001}, 13); err != nil {
			t.Fatal(err)
		}
	}
	for sid, price := range map[int32]float64{1: 110, 3: 330} {
		if err = sink.ObserveQuote(ctx, sid, backtest.Quote{AtMS: 14, AvailableAt: 14, Price: price}, 14); err != nil {
			t.Fatal(err)
		}
	}
	state, err = sink.StrategyState(ctx, 14)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(state.NAV-9979.804202) > 1e-8 || math.Abs(state.Funding-(-.0002)) > 1e-8 {
		t.Fatalf("drift/funding: %+v", state)
	}
	if err = sink.Account.Reconcile("paper-check", 14); err != nil {
		t.Fatal(err)
	}
	snap, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Checkpoint <= 2 || sink.Paper.Metrics().LastSourceAt != 11 || sink.Paper.Metrics().LastFill.AtMS != 12 {
		t.Fatal("delayed quote source rewritten or fill backdated")
	}
	weights, _ := backtest.NewBook(10000)
	if err = weights.Execute(p, quotes, 12, .001, .001); err != nil {
		t.Fatal(err)
	}
	if math.Abs(weights.State().NAV-9980) > 1e-8 || weights.State().Quantities[3] == 16.66 {
		t.Fatal("weights/events distinction erased")
	}
	if err = sink.ObserveFunding(ctx, backtest.Funding{ID: "late", SID: 1, AtMS: 14, AvailableAt: 15, Rate: .1}, 15); err == nil {
		t.Fatal("late funding repriced using later quantity")
	}
}
func TestPaperTargetsEnforceRiskAndVenueMinimumBeforeSend(t *testing.T) {
	for _, kind := range []string{"gross", "margin", "minimum"} {
		t.Run(kind, func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			switch kind {
			case "gross":
				c.Execution.StrategyGrossLimit = decimal.NewFromInt(1)
			case "margin":
				c.Execution.MaxAccountMargin = decimal.NewFromInt(1)
			case "minimum":
				i := c.Execution.Instruments[1]
				i.MinNotional = decimal.NewFromInt(100000)
				c.Execution.Instruments[1] = i
			}
			sink, done, err := NewPaperSink(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer done()
			q := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
			if err = sink.ObserveQuote(context.Background(), 1, q, 11); err != nil {
				t.Fatal(err)
			}
			p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: 1, SnapshotID: "snapshot", PlanHash: "definition", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "budget", Currency: "USD", NAV: 10000}, Mode: factor.Full}, map[int32]float64{1: .5})
			if err != nil {
				t.Fatal(err)
			}
			if err = sink.ProcessSnapshot(context.Background(), p, map[int32]backtest.Quote{1: q}, 11); err == nil {
				t.Fatal("account constraint bypassed")
			}
			snap, err := sink.Account.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if sink.Paper.Metrics().Fills != 0 || len(snap.Lots) != 0 || len(snap.Orders) != 0 {
				t.Fatal("failed portfolio partially sent")
			}
		})
	}
}
func TestAccountSinkPreservesUntouchedUnfilledTargets(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	other := execution.StrategyID("other")
	if err = sink.Account.CashEvent(execution.CashEvent{ID: "other-capital", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: execution.StrategyID(c.StrategyID), Amount: decimal.NewFromInt(-2000)}, {Strategy: other, Amount: decimal.NewFromInt(2000)}}}); err != nil {
		t.Fatal(err)
	}
	sink.Risk.StrategyGrossLimits[other] = decimal.NewFromInt(20000)
	if err := sink.Account.RegisterRiskPolicy("shared-risk-v1", sink.Risk); err != nil {
		t.Fatal(err)
	}
	for _, sid := range []int32{1, 2} {
		if err = sink.ObserveQuote(ctx, sid, backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}, 11); err != nil {
			t.Fatal(err)
		}
	}
	risk := sink.Risk
	risk.Marks = map[string]decimal.Decimal{c.Execution.Instruments[1].ID: decimal.NewFromInt(100), c.Execution.Instruments[2].ID: decimal.NewFromInt(100)}
	request := execution.CombinedRebalance{PlanID: "pending-instrument-2", Sequence: 1, DecisionMS: 11, ExpiresMS: 100, Risk: risk, Requests: []execution.InstrumentRebalance{{Instrument: c.Execution.Instruments[2], Quote: execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: 11, ReceivedMS: 11, ValidUntilMS: 100, Bar: 11}, Targets: []execution.ExecutableTarget{{Strategy: execution.StrategyID(c.StrategyID), Lot: "factor:2", SignedSteps: 100}, {Strategy: other, Lot: "other:2", SignedSteps: 200}}}}}
	prepared, err := sink.Account.PrepareRebalance(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.OrderIDs) == 0 {
		t.Fatal("fixture lacks pending real order")
	}
	p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: 1, SnapshotID: "snapshot", PlanHash: "definition", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "budget", Currency: "USD", NAV: 8000}, Mode: factor.Patch}, map[int32]float64{1: .5})
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.ProcessSnapshot(ctx, p, map[int32]backtest.Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}}, 11); err != nil {
		t.Fatal(err)
	}
	latest, err := sink.Account.LatestPlan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[execution.StrategyID]int64{}
	for _, target := range latest.Targets {
		if target.Instrument == c.Execution.Instruments[2].ID {
			seen[target.Strategy] = target.SignedSteps
		}
	}
	if seen[execution.StrategyID(c.StrategyID)] != 100 || seen[other] != 200 || latest.Sequence <= prepared.Plan.Sequence {
		t.Fatalf("untouched source targets or shared sequence lost: %+v", latest)
	}
	if len(latest.CarriedIntents) < 2 {
		t.Fatal("pending stable intents lost")
	}
}
func TestFundingRefusesUnassignedOrExternalPosition(t *testing.T) {
	i := execution.Instrument{ID: "asset-1"}
	for _, snap := range []execution.AccountSnapshot{{ExternalPositions: []execution.VirtualLot{{Instrument: i, SignedSteps: 1}}}, {ActualPositions: []execution.VirtualLot{{Instrument: i, SignedSteps: 2}}, Lots: []execution.VirtualLot{{Instrument: i, SignedSteps: 1}}}} {
		if validateFundingAttribution(snap, i) == nil {
			t.Fatal("unassigned funding charged to strategies")
		}
	}
	if err := validateFundingAttribution(execution.AccountSnapshot{ActualPositions: []execution.VirtualLot{{Instrument: i, SignedSteps: 2}}, Lots: []execution.VirtualLot{{Instrument: i, SignedSteps: 1}, {Instrument: i, SignedSteps: 1}}}, i); err != nil {
		t.Fatal(err)
	}
}
