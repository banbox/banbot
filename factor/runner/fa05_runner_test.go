package runner

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

type fa05Capture struct {
	targets map[int64]map[int32]float64
	states  map[int64]backtest.State
}

func (o *fa05Capture) Decision(f factor.Frame, p *factor.TargetPortfolio, _ []factor.Diagnostic) error {
	if p != nil {
		o.targets[f.DecisionTime] = p.Targets()
	}
	return nil
}
func (o *fa05Capture) Evaluation(research.Report) error { return nil }
func (o *fa05Capture) Executed(_ *factor.TargetPortfolio, s backtest.State, at int64) error {
	o.states[at] = s
	return nil
}
func (o *fa05Capture) TargetAccepted(_ *factor.TargetPortfolio, s backtest.State, at int64) error {
	o.states[at] = s
	return nil
}
func (o *fa05Capture) DecisionAllocation(f factor.Frame, t *factor.PortfolioTarget, _ []factor.Diagnostic) error {
	if t != nil {
		w := map[int32]float64{}
		for sid, a := range t.Allocations() {
			if a.Value == "0" {
				w[sid] = 0
			} else {
				w[sid] = 1
			}
		}
		o.targets[f.DecisionTime] = w
	}
	return nil
}
func (o *fa05Capture) AllocationAccepted(_ *factor.PortfolioTarget, s backtest.State, at int64) error {
	o.states[at] = s
	return nil
}

func buildRegimeArchive(t *testing.T, priceTable [][4]float64) Config {
	t.Helper()
	const hour int64 = 3600000
	store, err := factor.NewVersionStore(2000)
	if err != nil {
		t.Fatal(err)
	}
	sids := []int32{1, 2, 3, 4}
	symbols := map[int32]string{1: "BTC", 2: "ETH", 3: "SOL", 4: "BNB"}
	for barIdx, row := range priceTable {
		at := int64(barIdx+1) * hour
		for i, sid := range sids {
			p := row[i]
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": p}},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": p}},
				EventTime:     at + 1,
				AvailableAt:   at + 1,
				IngestedAt:    at + 1,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "regime.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	end := int64(len(priceTable))*hour + 2
	return Config{
		Mode:             Weights,
		Chunks:           []Chunk{{Path: path, From: hour, To: end}},
		MaxRecords:       2000,
		MaxPending:       16,
		DecisionInterval: hour,
		LatencyMS:        1,
		ExpiryMS:         60000,
		Snapshot: factor.SnapshotSpec{
			Universe:          factor.Universe{Version: "u-4", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true},
			SIDMap:            symbols,
			Schemas:           map[string]string{"kline": "s1", "tick": "s1"},
			SourceVersions:    map[string]string{"kline": "v1", "tick": "v1"},
			VisibilityPolicy:  "available-at",
			AdjustmentVersion: "raw",
		},
		Factor:     research.MomentumVolConfig{Source: "kline", Field: "close", TimeFrame: "1h", Window: 1},
		Manifest:   research.ManifestSpec{CodeRevision: "test", Currency: "USDT", Portfolio: research.PortfolioDefinition{K: 2, LongNotional: 1.0, ShortNotional: 0, Mode: factor.Full}, Costs: research.CostSpec{FundingPolicy: "explicit-zero"}},
		StrategyID: "s",
		AccountID:  "a",
		InitialNAV: 10000,
		Prices:     PriceStream{Source: "tick", TimeFrame: "event", Field: "price"},
	}
}

func TestDualMomentumScoreGateLiquidatesInBearMarketAndHoldsPartialCash(t *testing.T) {
	if err := RegisterFactor("fa05-raw-mom-v1", func(c FactorContext) (FactorSpec, error) {
		return FactorSpec{Outputs: map[string]*factor.Node{
			"score": factor.Return(c.Close(), 1),
		}}, c.Params.Err()
	}); err != nil {
		t.Fatal(err)
	}

	// Bar 1: [100, 100, 100, 100] -> warmup for Return(close, 1)
	// Bar 2: [120, 110, 95, 90]   -> ret = [+0.20, +0.10, -0.05, -0.10]: 2 coins >= 0 -> full LongNotional (0.5 + 0.5 = 1.0)
	// Bar 3: [132, 100, 90, 85]   -> ret = [+0.10, -0.0909, -0.0526, -0.0556]: 1 coin >= 0 -> 0.5 invested, 0.5 cash!
	// Bar 4: [110, 95, 85, 80]    -> ret = [-0.1667, -0.05, -0.0556, -0.0588]: 0 coins >= 0 -> 0.0 invested, 100% cash!
	table := [][4]float64{
		{100, 100, 100, 100},
		{120, 110, 95, 90},
		{132, 100, 90, 85},
		{110, 95, 85, 80},
	}
	const hour int64 = 3600000
	zero := 0.0

	for _, mode := range []string{"top-bottom-k", "lifecycle-v1"} {
		t.Run(mode, func(t *testing.T) {
			cfg := buildRegimeArchive(t, table)
			cfg.Definition = "fa05-raw-mom-v1"
			if mode == "top-bottom-k" {
				cfg.Manifest.Portfolio.LongMinScore = &zero
			} else {
				cfg.Manifest.Portfolio.Policy = "lifecycle-v1"
				cfg.Manifest.Portfolio.Selection = &factor.SelectionConfig{LongK: 2, LongMinScore: &zero}
			}
			cap := &fa05Capture{targets: map[int64]map[int32]float64{}, states: map[int64]backtest.State{}}
			res, err := Run(context.Background(), cfg, nil, cap)
			if err != nil {
				t.Fatal(err)
			}
			// Bar 2 fill at 2*hour+1: fully invested (Cash ~ 0)
			st2 := cap.states[2*hour+1]
			if math.Abs(st2.Cash) > 1e-6 || st2.Quantities[1] <= 0 || st2.Quantities[2] <= 0 {
				t.Fatalf("%s bar2 expected 100%% invested in SID 1 & 2, got state=%+v", mode, st2)
			}
			// Bar 3 fill at 3*hour+1: decision-time frozen NAV was st2.NAV (10000); only SID 1 gets 0.5 * st2.NAV = 5000, SID 2 = 0, rest in cash!
			st3 := cap.states[3*hour+1]
			invested3 := st3.NAV - st3.Cash
			if math.Abs(invested3-0.5*st2.NAV) > 1e-6 || st3.Quantities[1] <= 0 || st3.Quantities[2] != 0 {
				t.Fatalf("%s bar3 expected invested=50%% of decision NAV (%g) in SID 1 and SID 2=0, got invested=%g state=%+v", mode, 0.5*st2.NAV, invested3, st3)
			}
			// Bar 4 fill at 4*hour+1: ALL negative -> 100% cash, 0 positions!
			st4 := cap.states[4*hour+1]
			for sid, qty := range res.Book.Quantities {
				if qty != 0 {
					t.Fatalf("%s bar4 bear market kept nonzero position SID %d=%g", mode, sid, qty)
				}
			}
			if math.Abs(st4.Cash-st4.NAV) > 1e-6 || math.Abs(res.Book.Cash-res.Book.NAV) > 1e-6 {
				t.Fatalf("%s bar4 bear market must be 100%% cash, got st4=%+v final=%+v", mode, st4, res.Book)
			}
		})
	}
}

func TestRegisterFactorDynamicExposureScaleBreadthAndSideScales(t *testing.T) {
	if err := RegisterFactor("fa05-breadth-mom-v1", func(c FactorContext) (FactorSpec, error) {
		closeNode := c.Close()
		sma2 := factor.SMA(closeNode, 2)
		return FactorSpec{Outputs: map[string]*factor.Node{
			"score":          factor.Return(closeNode, 1),
			"exposure_scale": factor.CsMean(factor.Gt(closeNode, sma2)),
		}}, c.Params.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFactor("fa05-side-scales-v1", func(c FactorContext) (FactorSpec, error) {
		closeNode := c.Close()
		return FactorSpec{Outputs: map[string]*factor.Node{
			"score":       factor.Return(closeNode, 1),
			"long_scale":  factor.Constant(0.6, c.TimeFrame),
			"short_scale": factor.Constant(0.25, c.TimeFrame),
		}}, c.Params.Err()
	}); err != nil {
		t.Fatal(err)
	}
	// Factor declaring ONLY reserved scale outputs and no score output must be rejected at compile time.
	if err := RegisterFactor("fa05-only-scale-bad", func(c FactorContext) (FactorSpec, error) {
		return FactorSpec{Outputs: map[string]*factor.Node{
			"exposure_scale": factor.Constant(0.5, c.TimeFrame),
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CompileDefinition(Config{Definition: "fa05-only-scale-bad", Factor: research.MomentumVolConfig{TimeFrame: "1h"}}); err == nil {
		t.Fatal("accepted factor with only exposure_scale and no score columns")
	}

	// Verify combo.Columns excludes exposure_scale automatically.
	_, combo, err := CompileDefinition(Config{Definition: "fa05-breadth-mom-v1", Factor: research.MomentumVolConfig{TimeFrame: "1h"}})
	if err != nil || len(combo.Columns) != 1 || combo.Columns[0] != "score" {
		t.Fatalf("expected combo.Columns=[score], got %+v err=%v", combo, err)
	}

	// Bar 1: [100, 100, 100, 100] (warmup)
	// Bar 2: [120, 110, 105, 102] -> all 4 > sma2 -> breadth = 1.0 -> invested = 1.0 * NAV
	// Bar 3: [140, 120, 100, 90]  -> SID 1,2 > sma2; SID 3,4 < sma2 -> breadth = 0.5 -> invested = 0.5 * NAV
	// Bar 4: [110, 100, 80, 70]   -> all 4 < sma2 -> breadth = 0.0 -> invested = 0.0 (100% cash)
	table := [][4]float64{
		{100, 100, 100, 100},
		{120, 110, 105, 102},
		{140, 120, 100, 90},
		{110, 100, 80, 70},
	}
	const hour int64 = 3600000

	for _, usePolicy := range []bool{false, true} {
		cfg := buildRegimeArchive(t, table)
		cfg.Definition = "fa05-breadth-mom-v1"
		if usePolicy {
			cfg.Manifest.Portfolio.Policy = "lifecycle-v1"
			cfg.Manifest.Portfolio.Selection = &factor.SelectionConfig{LongK: 2}
		}
		cap := &fa05Capture{targets: map[int64]map[int32]float64{}, states: map[int64]backtest.State{}}
		res, err := Run(context.Background(), cfg, nil, cap)
		if err != nil {
			t.Fatalf("policy=%v run failed: %v", usePolicy, err)
		}
		st2 := cap.states[2*hour+1]
		if math.Abs(st2.Cash) > 1e-6 {
			t.Fatalf("policy=%v bar2 breadth=1.0 want 0 cash, got %+v", usePolicy, st2)
		}
		st3 := cap.states[3*hour+1]
		invested3 := st3.NAV - st3.Cash
		if math.Abs(invested3-0.5*st2.NAV) > 1e-5 {
			t.Fatalf("policy=%v bar3 breadth=0.5 want invested=50%% of decision NAV (%g), got invested=%g state=%+v", usePolicy, 0.5*st2.NAV, invested3, st3)
		}
		if math.Abs(res.Book.Cash-res.Book.NAV) > 1e-5 {
			t.Fatalf("policy=%v bar4 breadth=0.0 want 100%% cash, got cash=%g nav=%g", usePolicy, res.Book.Cash, res.Book.NAV)
		}
	}

	// Asymmetric long_scale (0.6) and short_scale (0.25) on long/short portfolio (LongNotional=0.5, ShortNotional=0.5):
	// Expected long total weight = 0.5 * 0.6 = 0.30; expected short total weight = -0.5 * 0.25 = -0.125.
	lsCfg := buildRegimeArchive(t, table)
	lsCfg.Definition = "fa05-side-scales-v1"
	lsCfg.Manifest.Portfolio.K = 1
	lsCfg.Manifest.Portfolio.LongNotional = 0.5
	lsCfg.Manifest.Portfolio.ShortNotional = 0.5
	cap := &fa05Capture{targets: map[int64]map[int32]float64{}, states: map[int64]backtest.State{}}
	if _, err := Run(context.Background(), lsCfg, nil, cap); err != nil {
		t.Fatal(err)
	}
	w2 := cap.targets[2*hour]
	if math.Abs(w2[1]-0.30) > 1e-12 || math.Abs(w2[4]+0.125) > 1e-12 {
		t.Fatalf("asymmetric side scales mismatch: got %+v, want long=0.30 short=-0.125", w2)
	}
}
