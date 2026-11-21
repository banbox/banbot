package entry

import (
	"context"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
)

var registerCarryFactorOnce sync.Once

func ensureCarryFactorRegistered(t *testing.T) {
	t.Helper()
	var regErr error
	registerCarryFactorOnce.Do(func() {
		regErr = runner.RegisterFactor("test-funding-carry-d01", func(c runner.FactorContext) (runner.FactorSpec, error) {
			return runner.FactorSpec{
				Outputs: map[string]*factor.Node{
					"funding": factor.AsOfField(orm.SeriesSourceFundingRate, "rate", orm.DefaultFundingRateTF, c.TimeFrame, 12*3600*1000),
				},
			}, c.Params.Err()
		})
	})
	if regErr != nil {
		t.Fatal(regErr)
	}
}

type captureNavOutput struct {
	lastState backtest.State
	states    []backtest.State
	diags     [][]factor.Diagnostic
}

func (o *captureNavOutput) Decision(_ factor.Frame, _ *factor.TargetPortfolio, diags []factor.Diagnostic) error {
	if len(diags) > 0 {
		o.diags = append(o.diags, diags)
	}
	return nil
}
func (o *captureNavOutput) Evaluation(research.Report) error { return nil }
func (o *captureNavOutput) Executed(_ *factor.TargetPortfolio, state backtest.State, _ int64) error {
	o.lastState = state
	return nil
}
func (o *captureNavOutput) Nav(state backtest.State, _ int64) error {
	o.lastState = state
	o.states = append(o.states, state)
	return nil
}

func TestFactorAsOfFundingRateAndRealStorageFundingSettlement(t *testing.T) {
	ensureCarryFactorRegistered(t)

	for _, policyMode := range []string{"storage", "required-stream"} {
		t.Run(policyMode, func(t *testing.T) {
			yamlCfg := "config_version: 2\n" +
				"time_start: '20240101'\n" +
				"time_end: '20240102'\n" +
				"exchange: {name: binance}\n" +
				"market_type: linear\n" +
				"pairs: ['A/USDT:USDT', 'B/USDT:USDT']\n" +
				"stake_currency: [USDT]\n" +
				"wallet_amounts: {USDT: 10000}\n" +
				"execution: {funding_policy: " + policyMode + "}\n" +
				"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 5000}\n" +
				"run_policy:\n" +
				"  - name: test-funding-carry-d01\n" +
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
				t.Fatalf("buildFactorConfigs(%s) failed: %v", policyMode, buildErr)
			}
			if configs[0].FundingSource != orm.SeriesSourceFundingRate {
				t.Fatalf("expected default FundingSource %q, got %q", orm.SeriesSourceFundingRate, configs[0].FundingSource)
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

			const hourMS = int64(3600 * 1000)
			const step8h = 8 * hourMS
			anchor := snapshot.View().TimeRange.StartMS
			end := snapshot.View().TimeRange.EndMS

			// Settlements by EndMS:
			// - anchor - 8h, anchor, anchor + 8h: A = +0.002, B = -0.001
			//   -> A goes LONG (+50 @ 100), B goes SHORT (-50 @ 100) at 01:00.
			//   -> At 08:00: A (long +ve) pays +10.0, B (short -ve) pays +5.0 => Funding = +15.0, NAV = 9985.0.
			//   -> At 09:00 rebalance: A LONG +49.925, B SHORT -49.925.
			// - anchor + 16h: A = -0.004, B = +0.003 (sign flip!)
			//   -> At 16:00 settlement (before 17:00 rebalance):
			//      A (long -ve) receives 49.925*100*0.004 = 19.97 (cost = -19.97);
			//      B (short +ve) receives 49.925*100*0.003 = 14.9775 (cost = -14.9775).
			//   -> Net Funding = 15.0 - 19.97 - 14.9775 = -19.9475, NAV = 10019.9475.
			//   -> At 16:00 decision (17:00 fill), AsOfField observes flipped rates and flips B LONG / A SHORT!
			// - anchor + 24h: rate = 0.0 for both.
			rateFor := func(sid int32, endMS int64) float64 {
				switch {
				case endMS <= anchor+step8h:
					if sid == 1 {
						return 0.002
					}
					return -0.001
				case endMS == anchor+2*step8h:
					if sid == 1 {
						return -0.004
					}
					return 0.003
				default:
					return 0.0
				}
			}

			query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				var out []*orm.DataSeries
				switch sub.Source {
				case orm.SeriesSourceKline:
					const minMS = int64(60 * 1000)
					for ts := anchor - 4*hourMS; ts < end; ts += minMS {
						if ts >= start && ts < stop {
							out = append(out, &orm.DataSeries{
								Source:    orm.SeriesSourceKline,
								Sid:       sub.ExSymbol.ID,
								TimeFrame: "1m",
								TimeMS:    ts,
								EndMS:     ts + minMS,
								Closed:    true,
								Values:    map[string]any{"close": 100.0},
							})
							if limit > 0 && len(out) >= limit {
								break
							}
						}
					}
				case orm.SeriesSourceFundingRate:
					for ts := anchor - 3*step8h; ts < end; ts += step8h {
						if ts >= start && ts < stop {
							endMS := ts + step8h
							out = append(out, &orm.DataSeries{
								Source:    orm.SeriesSourceFundingRate,
								Sid:       sub.ExSymbol.ID,
								TimeFrame: "8h",
								TimeMS:    ts,
								EndMS:     endMS,
								Closed:    true,
								Values:    map[string]any{"rate": rateFor(sub.ExSymbol.ID, endMS)},
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
				t.Fatalf("runner.Run(%s) failed: %v", policyMode, runErr)
			}
			_ = io.Discard
			if res.Incomplete != 0 || res.TargetsAccepted == 0 {
				t.Fatalf("unexpected run metrics: Incomplete=%d TargetsAccepted=%d Skipped=%d Decisions=%d Prices=%+v Portfolio=%+v diags=%+v",
					res.Incomplete, res.TargetsAccepted, res.Skipped, res.Decisions, prepared[0].Prices, prepared[0].Manifest.Portfolio, navOut.diags)
			}

			const wantFunding = -19.9475
			const wantNAV = 10019.9475
			if math.Abs(navOut.lastState.Funding-wantFunding) > 1e-6 {
				t.Fatalf("Book.Funding = %.8f, want %.8f", navOut.lastState.Funding, wantFunding)
			}
			if math.Abs(navOut.lastState.NAV-wantNAV) > 1e-6 {
				t.Fatalf("Book.NAV = %.8f, want %.8f", navOut.lastState.NAV, wantNAV)
			}
			// Verify portfolio flipped after 16:00 settlement: sid 1 is now SHORT (< 0) and sid 2 is LONG (> 0).
			if navOut.lastState.Quantities[1] >= 0 || navOut.lastState.Quantities[2] <= 0 {
				t.Fatalf("expected flipped positions after 16:00 AsOfField update, got qty=%+v", navOut.lastState.Quantities)
			}
		})
	}
}
