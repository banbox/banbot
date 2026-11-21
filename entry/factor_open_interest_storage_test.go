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

var registerOIFactorOnce sync.Once

func ensureOIFactorRegistered(t *testing.T) {
	t.Helper()
	var regErr error
	registerOIFactorOnce.Do(func() {
		regErr = runner.RegisterFactor("test-oi-ls-factor-d02", func(c runner.FactorContext) (runner.FactorSpec, error) {
			// Combine bar-aligned Field("open_interest", "oi_value", "1h") / kline.quote
			// with multi-timeframe AsOfField("open_interest", "top_ls_pos_ratio", "5m", "1h", ...).
			oiValue := factor.Field(orm.SeriesSourceOpenInterest, orm.FieldOpenInterestValue, c.TimeFrame)
			quoteVol := factor.Positive(factor.Field(orm.SeriesSourceKline, "quote", c.TimeFrame))
			oiTurnoverRatio := factor.Div(oiValue, quoteVol)
			topPosRatio := factor.AsOfField(orm.SeriesSourceOpenInterest, orm.FieldTopLSPosRatio, "5m", c.TimeFrame, 2*3600*1000)
			signal := factor.Mul(oiTurnoverRatio, topPosRatio)
			return runner.FactorSpec{
				Outputs: map[string]*factor.Node{
					"oi_crowding": factor.ZScore(signal),
				},
			}, c.Params.Err()
		})
	})
	if regErr != nil {
		t.Fatal(regErr)
	}
}

func TestFactorFieldAndAsOfFieldOpenInterestStorageReplay(t *testing.T) {
	ensureOIFactorRegistered(t)

	yamlCfg := "config_version: 2\n" +
		"time_start: '20240101'\n" +
		"time_end: '20240102'\n" +
		"exchange: {name: binance}\n" +
		"market_type: linear\n" +
		"pairs: ['A/USDT:USDT', 'B/USDT:USDT']\n" +
		"stake_currency: [USDT]\n" +
		"wallet_amounts: {USDT: 10000}\n" +
		"execution: {mode: weights, funding_policy: explicit-zero}\n" +
		"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 5000}\n" +
		"run_policy:\n" +
		"  - name: test-oi-ls-factor-d02\n" +
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

	// Before flipAt (<= anchor+12h): A has higher oi_value * top_ls_pos_ratio -> A LONG, B SHORT.
	// After flipAt  (>  anchor+12h): B surges in oi_value & top_ls_pos_ratio -> B LONG, A SHORT!
	oiValFor := func(sid int32, endMS int64) float64 {
		if endMS <= flipAt {
			if sid == 1 {
				return 800_000_000.0
			}
			return 200_000_000.0
		}
		if sid == 1 {
			return 150_000_000.0
		}
		return 900_000_000.0
	}
	topPosFor := func(sid int32, endMS int64) float64 {
		if endMS <= flipAt {
			if sid == 1 {
				return 1.80
			}
			return 0.75
		}
		if sid == 1 {
			return 0.70
		}
		return 2.10
	}

	query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var out []*orm.DataSeries
		switch sub.Source {
		case orm.SeriesSourceKline:
			step := hourMS
			if sub.TimeFrame == "1m" {
				step = minMS
			}
			for ts := anchor - 4*hourMS; ts < end; ts += step {
				if ts >= start && ts < stop {
					out = append(out, &orm.DataSeries{
						Source:    orm.SeriesSourceKline,
						Sid:       sub.ExSymbol.ID,
						TimeFrame: sub.TimeFrame,
						TimeMS:    ts,
						EndMS:     ts + step,
						Closed:    true,
						Values: map[string]any{
							"close": 100.0,
							"quote": 100_000_000.0,
						},
					})
					if limit > 0 && len(out) >= limit {
						break
					}
				}
			}
		case orm.SeriesSourceOpenInterest:
			step := hourMS
			if sub.TimeFrame == "5m" {
				step = fiveMinMS
			}
			for ts := anchor - 4*hourMS; ts < end; ts += step {
				if ts >= start && ts < stop {
					endMS := ts + step
					out = append(out, &orm.DataSeries{
						Source:    orm.SeriesSourceOpenInterest,
						Sid:       sub.ExSymbol.ID,
						TimeFrame: sub.TimeFrame,
						TimeMS:    ts,
						EndMS:     endMS,
						Closed:    true,
						Values: map[string]any{
							orm.FieldOpenInterest:      oiValFor(sub.ExSymbol.ID, endMS) / 100.0,
							orm.FieldOpenInterestValue: oiValFor(sub.ExSymbol.ID, endMS),
							orm.FieldTopLSPosRatio:     topPosFor(sub.ExSymbol.ID, endMS),
							orm.FieldTopLSAccRatio:     nil,
						},
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
	// Verify initial regime held A LONG (>0) & B SHORT (<0), and final regime flipped to A SHORT (<0) & B LONG (>0)!
	sawInitial := false
	for _, st := range navOut.states {
		if st.Quantities[1] > 0 && st.Quantities[2] < 0 {
			sawInitial = true
			break
		}
	}
	if !sawInitial {
		t.Fatalf("expected early states to hold sid 1 LONG and sid 2 SHORT")
	}
	if navOut.lastState.Quantities[1] >= 0 || navOut.lastState.Quantities[2] <= 0 || math.IsNaN(navOut.lastState.NAV) {
		t.Fatalf("expected final state to flip to sid 1 SHORT and sid 2 LONG, got qty=%+v nav=%v",
			navOut.lastState.Quantities, navOut.lastState.NAV)
	}
}
