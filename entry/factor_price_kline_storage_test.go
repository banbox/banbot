package entry

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
)

var registerBasisCarryFactorOnce sync.Once

func ensureBasisCarryFactorRegistered(t *testing.T) {
	t.Helper()
	var regErr error
	registerBasisCarryFactorOnce.Do(func() {
		regErr = runner.RegisterFactor("test-basis-carry-d04", func(c runner.FactorContext) (runner.FactorSpec, error) {
			// Combine negative/positive basis from Field("premium_kline", "close", tf)
			// with mark-to-index relative basis (mark_kline 5m AsOfField - index_kline 1h) / index_kline 1h:
			// Short high positive basis (rich perp) and Long negative basis (discounted perp) -> Neg(basis).
			premClose := factor.Field(orm.SeriesSourcePremiumKline, "close", c.TimeFrame)
			indexClose := factor.Positive(factor.Field(orm.SeriesSourceIndexKline, "close", c.TimeFrame))
			markClose := factor.AsOfField(orm.SeriesSourceMarkKline, "close", "5m", c.TimeFrame, 2*3600*1000)
			markIndexBasis := factor.Div(factor.Sub(markClose, indexClose), indexClose)
			carrySignal := factor.Neg(factor.Add(premClose, markIndexBasis))
			return runner.FactorSpec{
				Outputs: map[string]*factor.Node{
					"basis_carry": factor.ZScore(carrySignal),
				},
			}, c.Params.Err()
		})
	})
	if regErr != nil {
		t.Fatal(regErr)
	}
}

func TestFactorPremiumIndexAndMarkKlineStorageReplay(t *testing.T) {
	ensureBasisCarryFactorRegistered(t)

	yamlCfg := "config_version: 2\n" +
		"time_start: '20240101'\n" +
		"time_end: '20240102'\n" +
		"exchange: {name: binance}\n" +
		"market_type: linear\n" +
		"pairs: ['A/USDT:USDT', 'B/USDT:USDT']\n" +
		"stake_currency: [USDT]\n" +
		"wallet_amounts: {USDT: 10000}\n" +
		"execution: {mode: weights, funding_policy: explicit-zero}\n" +
		"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 6000}\n" +
		"run_policy:\n" +
		"  - name: test-basis-carry-d04\n" +
		"    engine: factor\n" +
		"    run_timeframes: [1h]\n" +
		"    params: {k: 1}\n" +
		"    decision: {expiry_ms: 120000}\n" +
		"    prices: {source: kline, timeframe: 1m, field: close}\n" +
		"    portfolio: {k: 1, long_notional: 0.5, short_notional: 0.5, mode: full}\n"

	spec, err := config.LoadRunSpec(&config.CmdArgs{
		NoDefault:  true,
		DataDir:    t.TempDir(),
		ConfigData: yamlCfg,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	configs, buildErr := buildFactorConfigs(spec, runner.Weights)
	if buildErr != nil {
		t.Fatalf("buildFactorConfigs failed: %v", buildErr)
	}

	snapshot, snapErr := spec.RuntimeSnapshot()
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	cat, catErr := data.RuntimeCatalogFromRegisteredSources()
	if catErr != nil {
		t.Fatal(catErr)
	}
	process := runtimectx.NewProcess()
	t.Cleanup(process.Close)
	task, taskErr := process.NewRuntime(runtimectx.Options{
		Context:      context.Background(),
		Config:       snapshot.View(),
		Catalog:      cat,
		Mode:         core.RunModeBackTest,
		ExchangeName: "binance",
		Market:       "linear",
		StartAt:      snapshot.View().TimeRange.StartMS,
		NetDisable:   true,
	})
	if taskErr != nil {
		t.Fatal(taskErr)
	}
	t.Cleanup(func() { task.Close(); task.Join() })

	for i, pair := range snapshot.View().Pairs {
		if err := task.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
			ID: int32(i + 1), Symbol: pair, Exchange: "binance", Market: "linear",
		}); err != nil {
			t.Fatal(err)
		}
	}

	const minMS = int64(60 * 1000)
	const fiveMinMS = 5 * minMS
	const hourMS = 60 * minMS
	anchor := snapshot.View().TimeRange.StartMS
	end := snapshot.View().TimeRange.EndMS
	flipAt := anchor + 12*hourMS

	// Before flipAt: A trades at negative basis (-0.0020) and B at positive basis (+0.0018)
	//   -> Neg(basis) ranks A > B -> A LONG, B SHORT!
	// After flipAt: A flips to positive basis (+0.0022) and B flips to negative basis (-0.0025)
	//   -> Neg(basis) ranks B > A -> B LONG, A SHORT!
	basisFor := func(sid int32, endMS int64) float64 {
		if endMS <= flipAt {
			if sid == 1 {
				return -0.0020
			}
			return 0.0018
		}
		if sid == 1 {
			return 0.0022
		}
		return -0.0025
	}

	query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var out []*orm.DataSeries
		switch sub.Source {
		case orm.SeriesSourceKline:
			step := minMS
			if sub.TimeFrame == "1h" {
				step = hourMS
			}
			for ts := anchor - 4*hourMS; ts < end; ts += step {
				if ts >= start && ts < stop {
					out = append(out, &orm.DataSeries{
						Source: orm.SeriesSourceKline, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame,
						TimeMS: ts, EndMS: ts + step, Closed: true,
						Values: map[string]any{"close": 100.0},
					})
					if limit > 0 && len(out) >= limit {
						break
					}
				}
			}
		case orm.SeriesSourcePremiumKline:
			for ts := anchor - 4*hourMS; ts < end; ts += hourMS {
				if ts >= start && ts < stop {
					endMS := ts + hourMS
					b := basisFor(sub.ExSymbol.ID, endMS)
					out = append(out, &orm.DataSeries{
						Source: orm.SeriesSourcePremiumKline, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame,
						TimeMS: ts, EndMS: endMS, Closed: true,
						Values: map[string]any{"open": b, "high": b + 0.0002, "low": b - 0.0002, "close": b},
					})
					if limit > 0 && len(out) >= limit {
						break
					}
				}
			}
		case orm.SeriesSourceIndexKline:
			for ts := anchor - 4*hourMS; ts < end; ts += hourMS {
				if ts >= start && ts < stop {
					out = append(out, &orm.DataSeries{
						Source: orm.SeriesSourceIndexKline, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame,
						TimeMS: ts, EndMS: ts + hourMS, Closed: true,
						Values: map[string]any{"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0},
					})
					if limit > 0 && len(out) >= limit {
						break
					}
				}
			}
		case orm.SeriesSourceMarkKline:
			for ts := anchor - 4*hourMS; ts < end; ts += fiveMinMS {
				if ts >= start && ts < stop {
					endMS := ts + fiveMinMS
					m := 100.0 * (1.0 + basisFor(sub.ExSymbol.ID, endMS))
					out = append(out, &orm.DataSeries{
						Source: orm.SeriesSourceMarkKline, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame,
						TimeMS: ts, EndMS: endMS, Closed: true,
						Values: map[string]any{"open": m, "high": m, "low": m, "close": m},
					})
					if limit > 0 && len(out) >= limit {
						break
					}
				}
			}
		}
		return out, nil
	}

	prepared, prepErr := assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, query)
	if prepErr != nil {
		t.Fatalf("assembleFactorStorageInputs failed: %v", prepErr)
	}
	prepared[0].HistoricalInput.(*factorStorageInput).assembly.bootstrap = nil

	navOut := &captureNavOutput{}
	res, runErr := runner.Run(context.Background(), prepared[0], nil, navOut)
	if runErr != nil {
		t.Fatalf("runner.Run failed: %v", runErr)
	}
	if res.Incomplete != 0 || res.TargetsAccepted == 0 {
		t.Fatalf("unexpected run metrics: Incomplete=%d TargetsAccepted=%d Decisions=%d diags=%+v",
			res.Incomplete, res.TargetsAccepted, res.Decisions, navOut.diags)
	}
	sawInitial := false
	for _, st := range navOut.states {
		if st.Quantities[1] > 0 && st.Quantities[2] < 0 {
			sawInitial = true
			break
		}
	}
	if !sawInitial {
		t.Fatalf("expected early states to hold sid 1 LONG (negative basis) and sid 2 SHORT (positive basis)")
	}
	if navOut.lastState.Quantities[1] >= 0 || navOut.lastState.Quantities[2] <= 0 || math.IsNaN(navOut.lastState.NAV) {
		t.Fatalf("expected final state to flip to sid 1 SHORT and sid 2 LONG, got qty=%+v nav=%v",
			navOut.lastState.Quantities, navOut.lastState.NAV)
	}
}
