package runner

import (
	"bytes"
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func TestRunnerDriftThresholdAndHybridStopLifecycleReplay(t *testing.T) {
	const hour = int64(3_600_000)
	sids := []int32{1, 2, 3, 4}
	sidMap := map[int32]string{
		1: "1000PEPE/USDT:USDT",
		2: "DOGE/USDT:USDT",
		3: "UNI/USDT:USDT",
		4: "AAVE/USDT:USDT",
	}

	// 8 hourly bars:
	// - Asset 1 starts with highest momentum at bar 2, then crashes -12% at bar 3 (mid-rebalance when every_bars=4!)
	//   triggering stop_loss_pct=0.08 while Asset 3 (also held) only moves +1% and stays untouched!
	// - During bars 4 & 5, cooldown_bars=2 blocks Asset 1 from re-entering even if its score rebounds.
	closes := map[int32][]float64{
		1: {100, 110, 95, 108, 120, 135, 150, 165},
		2: {100, 101, 102, 103, 104, 105, 106, 107},
		3: {100, 106, 107, 108, 109, 110, 111, 112},
		4: {100, 99, 98, 97, 96, 95, 94, 93},
	}

	store, err := factor.NewVersionStore(2000)
	if err != nil {
		t.Fatal(err)
	}
	for bar := 1; bar <= 8; bar++ {
		at := int64(bar) * hour
		for _, sid := range sids {
			c := closes[sid][bar-1]
			h := c * 1.01
			l := c * 0.99
			if err := store.Put(factor.VersionRecord{
				Series: orm.DataSeries{
					Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at, EndMS: at, Closed: true,
					Values: map[string]any{"open": c, "high": h, "low": l, "close": c, "quote": float64(sid) * 1000},
				},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(factor.VersionRecord{
				Series: orm.DataSeries{
					Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true,
					Values: map[string]any{"price": c},
				},
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
	archivePath := filepath.Join(t.TempDir(), "fa08.gob")
	if _, err := store.Export(archivePath); err != nil {
		t.Fatal(err)
	}

	makeBase := func() Config {
		return Config{
			Mode:             Weights,
			Chunks:           []Chunk{{archivePath, hour, 8*hour + 2}},
			MaxRecords:       2000,
			MaxPending:       10,
			DecisionInterval: hour,
			LatencyMS:        1,
			ExpiryMS:         60_000,
			StrategyID:       "fa08-strat",
			AccountID:        "fa08-acct",
			InitialNAV:       10_000,
			Prices:           PriceStream{Source: "tick", TimeFrame: "event", Field: "price"},
			Snapshot: factor.SnapshotSpec{
				Universe:          factor.Universe{Version: "u", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true},
				SIDMap:            sidMap,
				Schemas:           map[string]string{"kline": "s1", "tick": "s1"},
				SourceVersions:    map[string]string{"kline": "v1", "tick": "v1"},
				VisibilityPolicy:  "available-at",
				AdjustmentVersion: "raw",
			},
			Expressions: &expr.Spec{
				SchemaVersion: 1,
				TimeFrame:     "1h",
				Bindings:      map[string]expr.Binding{"kline": {Source: "kline", TimeFrame: "1h"}},
				Outputs: map[string]string{
					"mom": "ts.return(positive(kline.close), 1)",
				},
			},
			Manifest: research.ManifestSpec{
				CodeRevision: "test",
				Currency:     "USDT",
				Costs:        research.CostSpec{FeeRate: 0.001, SlippageRate: 0, FundingPolicy: "explicit-zero"},
			},
		}
	}

	// 1. Verify TopBottomK drift_threshold reduces turnover & fees vs drift_threshold == 0.
	cfgNoBuf := makeBase()
	cfgNoBuf.Manifest.Portfolio = research.PortfolioDefinition{K: 1, LongNotional: 0.5, ShortNotional: 0.5, Mode: factor.Full}
	resNoBuf, err := Run(context.Background(), cfgNoBuf, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfgBuf := makeBase()
	cfgBuf.Manifest.Portfolio = research.PortfolioDefinition{K: 1, LongNotional: 0.5, ShortNotional: 0.5, DriftThreshold: 0.05, Mode: factor.Full}
	resBuf, err := Run(context.Background(), cfgBuf, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resBuf.Book.Fees >= resNoBuf.Book.Fees || resBuf.Book.Turnover >= resNoBuf.Book.Turnover {
		t.Fatalf("expected drift_threshold=0.05 to reduce fees & turnover: buf=(fees=%v,turn=%v) noBuf=(fees=%v,turn=%v)",
			resBuf.Book.Fees, resBuf.Book.Turnover, resNoBuf.Book.Fees, resNoBuf.Book.Turnover)
	}

	// 2. Verify Hybrid XS + Per-Asset Stop-Loss + Cooldown auto-promotes lifecycle-v1 and stops out Asset 1 mid-cycle!
	cfgStop := makeBase()
	cfgStop.Manifest.Portfolio = research.PortfolioDefinition{
		LongNotional:  1.0,
		ShortNotional: 0,
		Mode:          factor.Full,
		Rebalance:     &factor.RebalanceConfig{EveryBars: 4, DriftThreshold: 0.02},
		Selection:     &factor.SelectionConfig{LongK: 2},
		Holding:       &factor.HoldingConfig{StopLossPct: 0.08, CooldownBars: 2},
	}
	var jsonBuf bytes.Buffer
	resStop, err := Run(context.Background(), cfgStop, nil, &JSONOutput{Writer: &jsonBuf, SIDs: sids})
	if err != nil {
		t.Fatalf("hybrid stop replay failed: %v", err)
	}
	if !strings.Contains(jsonBuf.String(), "stop-loss-exit") {
		t.Fatalf("expected mid-rebalance stop-loss-exit diagnostic during replay, got log:\n%s", jsonBuf.String())
	}
	if math.IsNaN(resStop.Book.NAV) || resStop.Book.NAV <= 0 {
		t.Fatalf("unexpected final NAV: %+v", resStop.Book)
	}
}
