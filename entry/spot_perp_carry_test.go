package entry

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/factor/statistics"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

var registerSpotCarryStrategiesOnce sync.Once

func ensureSpotCarryStrategiesRegistered(t *testing.T) {
	t.Helper()
	var regErr error
	registerSpotCarryStrategiesOnce.Do(func() {
		// 1. RegisterPairs Cash-and-Carry strategy: Y = BTC/USDT:USDT (perp), X = BTC/USDT (spot).
		// Shorting the spread (-1) when perp trades at a premium goes Short Y (perp) + Long X (spot).
		regErr = runner.RegisterPairs("test-spot-perp-pairs-d05", func(p *runner.Params) (runner.PairsSpec, error) {
			return runner.PairsSpec{
				Pairs:        []runner.Pair{{Y: "BTC/USDT:USDT", X: "BTC/USDT"}},
				Window:       p.Int("window", 4),
				Band:         statistics.BandConfig{Entry: p.Float("entry", 1.0), Exit: p.Float("exit", 0.2)},
				Gross:        1.0,
				LongOnlySpot: p.Int("long_only_spot", 1) != 0,
			}, p.Err()
		})
		if regErr != nil {
			return
		}
		// 2. Deterministic fixed-weight Cash-and-Carry strategy (+0.5 BTC/USDT Spot, -0.5 BTC/USDT:USDT Perp)
		// to verify cent-exact price + basis + perp-only funding reconciliation in both Weights and Events modes.
		regErr = runner.RegisterDefinition("test-spot-perp-exact-d05", func(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
			closeNode := factor.Field(orm.SeriesSourceKline, "close", c.Factor.TimeFrame)
			plan, err := factor.New().Add("signal", closeNode).Compile()
			return plan, research.ComboSpec{Method: research.Equal, Columns: []string{"signal"}}, err
		})
		if regErr != nil {
			return
		}
		regErr = runner.RegisterPortfolioFactory("test-spot-perp-exact-builder-v1", func(c runner.Config) (runner.PortfolioBuilder, error) {
			spotShortAttempt := c.Params().Int("attempt_short_spot", 0) != 0
			entered := false
			return func(_ factor.Frame, _ factor.Universe, target factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
				if entered {
					return nil, nil, nil
				}
				entered = true
				weights := map[int32]float64{}
				for sid, sym := range c.Snapshot.SIDMap {
					if sym == "BTC/USDT" {
						if spotShortAttempt {
							weights[sid] = -0.5
						} else {
							weights[sid] = 0.5
						}
					} else if sym == "BTC/USDT:USDT" {
						if spotShortAttempt {
							weights[sid] = 0.5
						} else {
							weights[sid] = -0.5
						}
					}
				}
				p, err := factor.NewTargetPortfolio(target, weights)
				return p, nil, err
			}, nil
		})
	})
	if regErr != nil {
		t.Fatal(regErr)
	}
}

type carryMultiMarketExchange struct {
	banexg.BanExchange
	info *banexg.ExgInfo
}

func newCarryMultiMarketExchange() *carryMultiMarketExchange {
	spotMarket := &banexg.Market{
		ID:     "BTCUSDT",
		Symbol: "BTC/USDT",
		Base:   "BTC",
		Quote:  "USDT",
		Type:   banexg.MarketSpot,
		Spot:   true,
		Active: true,
		Precision: &banexg.Precision{
			Amount:     0.001,
			ModeAmount: banexg.PrecModeTickSize,
			Price:      2,
			ModePrice:  banexg.PrecModeDecimalPlace,
		},
		Limits: &banexg.MarketLimits{
			Amount: &banexg.LimitRange{Min: 0.001},
			Cost:   &banexg.LimitRange{Min: 5},
		},
	}
	perpMarket := &banexg.Market{
		ID:           "BTCUSDT",
		Symbol:       "BTC/USDT:USDT",
		Base:         "BTC",
		Quote:        "USDT",
		Settle:       "USDT",
		Type:         banexg.MarketLinear,
		Contract:     true,
		Linear:       true,
		Swap:         true,
		Active:       true,
		ContractSize: 1,
		Precision: &banexg.Precision{
			Amount:     0.001,
			ModeAmount: banexg.PrecModeTickSize,
			Price:      2,
			ModePrice:  banexg.PrecModeDecimalPlace,
		},
		Limits: &banexg.MarketLimits{
			Amount: &banexg.LimitRange{Min: 0.001},
			Cost:   &banexg.LimitRange{Min: 5},
		},
	}
	usdt := &banexg.Currency{Code: "USDT", Precision: 8, PrecMode: banexg.PrecModeDecimalPlace}
	info := &banexg.ExgInfo{
		ID:         "binance",
		MarketType: banexg.MarketLinear,
		Markets: banexg.MarketMap{
			spotMarket.Symbol: spotMarket,
			perpMarket.Symbol: perpMarket,
		},
		CurrenciesByCode: banexg.CurrencyMap{"USDT": usdt},
	}
	return &carryMultiMarketExchange{info: info}
}

func (e *carryMultiMarketExchange) Info() *banexg.ExgInfo { return e.info }

func (e *carryMultiMarketExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if mar := e.info.Markets[symbol]; mar != nil {
		if mar.Spot && e.info.MarketType == banexg.MarketLinear {
			if fut := e.info.Markets[symbol+":"+mar.Quote]; fut != nil {
				return fut, nil
			}
		}
		return mar, nil
	}
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "no market for %s", symbol)
}

func TestSpotPerpCashAndCarryBacktestWeightsEventsAndShortRejection(t *testing.T) {
	ensureSpotCarryStrategiesRegistered(t)

	const (
		sidSpot = int32(1)
		sidPerp = int32(2)
		hourMS  = int64(3600 * 1000)
		minMS   = int64(60 * 1000)
		step8h  = 8 * hourMS
	)

	makeRuntimeAndPrepare := func(t *testing.T, yamlCfg string, mode runner.Mode, spotPriceFn, perpPriceFn func(ts int64) float64, fundingRate float64, fundingRequestedSIDs *map[int32]int) ([]runner.Config, *runtimectx.Runtime) {
		t.Helper()
		spec, err := config.LoadRunSpec(&config.CmdArgs{
			NoDefault:  true,
			DataDir:    t.TempDir(),
			ConfigData: yamlCfg,
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		configs, buildErr := buildFactorConfigs(spec, mode)
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
		exgStub := newCarryMultiMarketExchange()
		process := runtimectx.NewProcess()
		t.Cleanup(process.Close)
		task, taskErr := process.NewRuntime(runtimectx.Options{
			Context:      context.Background(),
			Config:       snapshot.View(),
			Catalog:      cat,
			Exchange:     exgStub,
			Mode:         core.RunModeBackTest,
			ExchangeName: "binance",
			Market:       banexg.MarketLinear,
			StartAt:      snapshot.View().TimeRange.StartMS,
			NetDisable:   true,
		})
		if taskErr != nil {
			t.Fatal(taskErr)
		}
		t.Cleanup(func() { task.Close(); task.Join() })

		if err := task.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
			ID: sidSpot, Symbol: "BTC/USDT", Exchange: "binance", Market: banexg.MarketSpot,
		}); err != nil {
			t.Fatalf("cache spot symbol failed: %v", err)
		}
		if err := task.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
			ID: sidPerp, Symbol: "BTC/USDT:USDT", Exchange: "binance", Market: banexg.MarketLinear,
		}); err != nil {
			t.Fatalf("cache perp symbol failed: %v", err)
		}

		anchor := snapshot.View().TimeRange.StartMS
		end := snapshot.View().TimeRange.EndMS
		query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var out []*orm.DataSeries
			switch sub.Source {
			case orm.SeriesSourceKline:
				tfStep := minMS
				if sub.TimeFrame == "1h" {
					tfStep = hourMS
				}
				for ts := anchor - 8*hourMS; ts < end; ts += tfStep {
					if ts >= start && ts < stop {
						px := spotPriceFn(ts)
						if sub.ExSymbol.ID == sidPerp {
							px = perpPriceFn(ts)
						}
						out = append(out, &orm.DataSeries{
							Source:    orm.SeriesSourceKline,
							Sid:       sub.ExSymbol.ID,
							TimeFrame: sub.TimeFrame,
							TimeMS:    ts,
							EndMS:     ts + tfStep,
							Closed:    true,
							Values:    map[string]any{"open": px, "high": px, "low": px, "close": px, "volume": 10.0},
						})
						if limit > 0 && len(out) >= limit {
							break
						}
					}
				}
			case orm.SeriesSourceFundingRate:
				if fundingRequestedSIDs != nil {
					(*fundingRequestedSIDs)[sub.ExSymbol.ID]++
				}
				for ts := anchor - 3*step8h; ts < end; ts += step8h {
					if ts >= start && ts < stop {
						out = append(out, &orm.DataSeries{
							Source:    orm.SeriesSourceFundingRate,
							Sid:       sub.ExSymbol.ID,
							TimeFrame: "8h",
							TimeMS:    ts,
							EndMS:     ts + step8h,
							Closed:    true,
							Values:    map[string]any{"rate": fundingRate},
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
		return prepared, task
	}

	t.Run("exact_weights_and_events_reconciliation", func(t *testing.T) {
		// Window: 2024-01-01 00:00 -> 2024-01-02 00:00 (24h).
		// Decision at 00:00 -> fill at 00:01:
		//   Spot BTC/USDT = $50,000, Perp BTC/USDT:USDT = $50,500 (+1% basis premium).
		//   With $10,000 NAV and weights (+0.5 Spot, -0.5 Perp), Patch/single-entry or constant prices until 16:00:
		// Let's keep Spot = $50,000 and Perp = $50,500 up to 12:00, then at >= 12:00 BTC rallies to $55,000 on BOTH Spot and Perp (basis converges!).
		// Funding rate = +0.002 (+0.2% per 8h) at 08:00, 16:00, 24:00: ONLY the short Perp leg accrues funding!
		baseYAML := "config_version: 2\n" +
			"time_start: '20240101'\n" +
			"time_end: '20240102'\n" +
			"exchange: {name: binance}\n" +
			"market_type: linear\n" +
			"extra_markets: [spot]\n" +
			"leverage: 5\n" +
			"pairs: ['BTC/USDT', 'BTC/USDT:USDT']\n" +
			"stake_currency: [USDT]\n" +
			"wallet_amounts: {USDT: 10000}\n" +
			"execution: {funding_policy: storage}\n" +
			"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 8000}\n" +
			"run_policy:\n" +
			"  - name: test-spot-perp-exact-d05\n" +
			"    engine: factor\n" +
			"    run_timeframes: [1h]\n" +
			"    decision: {expiry_ms: 180000}\n" +
			"    prices: {source: kline, timeframe: 1m, field: close}\n" +
			"    portfolio: {builder: test-spot-perp-exact-builder-v1, mode: full}\n"

		anchor := int64(1704067200000) // 2024-01-01 00:00:00 UTC
		spotPx := func(ts int64) float64 {
			if ts >= anchor+12*hourMS {
				return 55000.0
			}
			return 50000.0
		}
		perpPx := func(ts int64) float64 {
			if ts >= anchor+12*hourMS {
				return 55000.0 // basis $500 converges to $0 while BTC rallies +$5,000!
			}
			return 50500.0
		}

		// 1. Weights mode:
		fundingQueried := map[int32]int{}
		preparedW, _ := makeRuntimeAndPrepare(t, baseYAML, runner.Weights, spotPx, perpPx, 0.002, &fundingQueried)
		if len(preparedW[0].SpotSIDs) != 1 || preparedW[0].SpotSIDs[0] != sidSpot {
			t.Fatalf("expected SpotSIDs=[1], got %v", preparedW[0].SpotSIDs)
		}
		navOut := &captureNavOutput{}
		resW, err := runner.Run(context.Background(), preparedW[0], nil, navOut)
		if err != nil {
			t.Fatalf("Weights Cash-and-Carry run failed: %v", err)
		}
		// Spot SID (1) must NEVER be queried for perpetual funding_rate!
		if fundingQueried[sidSpot] != 0 || fundingQueried[sidPerp] == 0 {
			t.Fatalf("expected only perp SID to query funding_rate, got %+v", fundingQueried)
		}
		// Exact math check for Weights mode:
		// Decision at 00:00 (2024-01-01 00:00) -> filled at 00:01:
		//   Spot qty = +5000 / 50000 = +0.1 BTC
		//   Perp qty = -5000 / 50500 = -0.09900990099009901 BTC
		// Funding settlements on Perp ONLY (rate = +0.002):
		//   08:00 (perp mark = 50500, notional = -5000): short perp receives 5000 * 0.002 = +$10.00
		//   16:00 (perp mark = 55000, notional = -0.09900990099009901 * 55000 = -5445.544554455445): receives +$10.89108910891089
		//   24:00 (perp mark = 55000): receives +$10.89108910891089
		//   Total funding received = $31.78217821782178 (Book.Funding = -31.78217821782178)
		// Price PnL at $55,000:
		//   Spot LongPnL = +0.1 * (55000 - 50000) = +$500.00
		//   Perp ShortPnL = -0.09900990099009901 * (55000 - 50500) = -$445.54455445544556
		//   Net basis convergence + delta PnL = +$54.45544554455444
		// Final NAV = 10000 + 54.45544554455444 + 31.78217821782178 = 10086.237623762376!
		perpQty := -5000.0 / 50500.0
		wantFunding := perpQty*50500.0*0.002 + 2*(perpQty*55000.0*0.002) // negative = net credit
		wantLongPnL := 0.1 * (55000.0 - 50000.0)
		wantShortPnL := perpQty * (55000.0 - 50500.0)
		wantNAV := 10000.0 + wantLongPnL + wantShortPnL - wantFunding

		if math.Abs(resW.Book.Funding-wantFunding) > 1e-6 {
			t.Fatalf("Weights Book.Funding = %.8f, want %.8f", resW.Book.Funding, wantFunding)
		}
		if math.Abs(resW.Book.NAV-wantNAV) > 1e-6 {
			t.Fatalf("Weights Book.NAV = %.8f, want %.8f", resW.Book.NAV, wantNAV)
		}
		if resW.Attribution == nil || math.Abs(resW.Attribution.LongPnL-wantLongPnL) > 1e-6 || math.Abs(resW.Attribution.ShortPnL-wantShortPnL) > 1e-6 {
			t.Fatalf("Weights Attribution = %+v, want LongPnL=%.4f ShortPnL=%.4f", resW.Attribution, wantLongPnL, wantShortPnL)
		}

		// 2. Events mode (PaperAdapter + execution.Store):
		fundingQueriedE := map[int32]int{}
		preparedE, taskE := makeRuntimeAndPrepare(t, baseYAML, runner.Events, spotPx, perpPx, 0.002, &fundingQueriedE)
		units := map[int32]execution.Instrument{}
		for _, sid := range []int32{sidSpot, sidPerp} {
			sym := taskE.Symbols.GetSymbolByID(sid)
			mkt, mErr := orm.LookupExchangeMarket(taskE.Exchange, sym.Symbol, taskE.Symbols)
			if mErr != nil {
				t.Fatal(mErr)
			}
			settleCode := mkt.Settle
			if mkt.Spot && settleCode == "" {
				settleCode = mkt.Quote
			}
			cur := taskE.Exchange.Info().CurrenciesByCode[settleCode]
			inst, iErr := execution.InstrumentFromBanexgMarket(sym.Symbol, mkt, cur)
			if iErr != nil {
				t.Fatal(iErr)
			}
			units[sid] = inst
		}
		preparedE[0].Execution.Instruments = units
		if !units[sidSpot].IsSpot() || units[sidSpot].ShortAllowed() || !units[sidPerp].HasPerpetualFunding() {
			t.Fatalf("unexpected Events instruments: spot=%+v perp=%+v", units[sidSpot], units[sidPerp])
		}
		resE, err := runner.Run(context.Background(), preparedE[0], nil, &captureNavOutput{})
		if err != nil {
			t.Fatalf("Events Cash-and-Carry run failed: %v", err)
		}
		if resE.Account == nil {
			t.Fatal("expected Events AccountSnapshot")
		}
		// Spot qty = 100 steps (0.100 BTC @ $50,000 = $5,000), Perp qty = -99 steps (-0.099 BTC @ $50,500 = $4,999.50).
		// Funding on -0.099 BTC Perp:
		//   08:00 @ 50500: +4999.50 * 0.002 = +9.999
		//   16:00 @ 55000: +5445.00 * 0.002 = +10.89
		//   24:00 @ 55000: +5445.00 * 0.002 = +10.89 => total funding = +31.779
		// Price PnL at $55,000: Spot +$500.00, Perp -0.099 * 4500 = -$445.50 => +$54.50
		// Exact Account Equity = 10000 + 54.50 + 31.779 = 10086.279!
		eqFinal, eqErr := resE.Account.Equity(map[string]decimal.Decimal{
			"BTC/USDT":      decimal.RequireFromString("55000"),
			"BTC/USDT:USDT": decimal.RequireFromString("55000"),
		})
		if eqErr != nil {
			t.Fatal(eqErr)
		}
		if !eqFinal.Equal(decimal.RequireFromString("10086.279")) {
			t.Fatalf("Events final Account.Equity = %s, want exact 10086.279", eqFinal.String())
		}
	})

	t.Run("short_spot_rejected_deterministically", func(t *testing.T) {
		shortSpotYAML := "config_version: 2\n" +
			"time_start: '20240101'\n" +
			"time_end: '20240102'\n" +
			"exchange: {name: binance}\n" +
			"market_type: linear\n" +
			"extra_markets: [spot]\n" +
			"pairs: ['BTC/USDT', 'BTC/USDT:USDT']\n" +
			"stake_currency: [USDT]\n" +
			"wallet_amounts: {USDT: 10000}\n" +
			"execution: {funding_policy: storage}\n" +
			"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 8000}\n" +
			"run_policy:\n" +
			"  - name: test-spot-perp-exact-d05\n" +
			"    engine: factor\n" +
			"    run_timeframes: [1h]\n" +
			"    params: {attempt_short_spot: 1}\n" +
			"    decision: {expiry_ms: 180000}\n" +
			"    prices: {source: kline, timeframe: 1m, field: close}\n" +
			"    portfolio: {builder: test-spot-perp-exact-builder-v1, mode: full}\n"

		constPx := func(int64) float64 { return 50000.0 }
		prepared, _ := makeRuntimeAndPrepare(t, shortSpotYAML, runner.Weights, constPx, constPx, 0.001, nil)
		_, runErr := runner.Run(context.Background(), prepared[0], nil, &captureNavOutput{})
		if runErr == nil || !strings.Contains(runErr.Error(), "spot") {
			t.Fatalf("expected shorting spot asset in backtest to fail with spot error, got %v", runErr)
		}
	})

	t.Run("register_pairs_spot_perp_cash_and_carry", func(t *testing.T) {
		pairsYAML := "config_version: 2\n" +
			"time_start: '20240101'\n" +
			"time_end: '20240102'\n" +
			"exchange: {name: binance}\n" +
			"market_type: linear\n" +
			"extra_markets: [spot]\n" +
			"pairs: ['BTC/USDT', 'BTC/USDT:USDT']\n" +
			"stake_currency: [USDT]\n" +
			"wallet_amounts: {USDT: 10000}\n" +
			"execution: {funding_policy: storage}\n" +
			"data: {pit_policy: static-approximation, page_rows: 500, prefetch_rows: 1000, max_records: 8000}\n" +
			"run_policy:\n" +
			"  - name: test-spot-perp-pairs-d05\n" +
			"    engine: factor\n" +
			"    run_timeframes: [1h]\n" +
			"    params: {window: 8, entry: 1.2, exit: 0.3, long_only_spot: 1}\n" +
			"    decision: {expiry_ms: 120000}\n" +
			"    prices: {source: kline, timeframe: 1m, field: close}\n"

		anchor := int64(1704067200000)
		// Spot oscillates with non-zero variance and perp tracks ~1.0005x spot with small noise;
		// at h=6..8 perp spikes to +2.0% premium (z > +1.2 -> Short Perp + Long Spot), converges by h=12,
		// and dips to -2.0% discount at h=18 (where LongOnlySpot blocks shorting the Spot leg).
		spotSeries := func(ts int64) float64 {
			h := float64((ts - anchor) / hourMS)
			return 50000.0 + 400.0*math.Sin(h*1.7)
		}
		perpSeries := func(ts int64) float64 {
			base := spotSeries(ts)
			h := int((ts - anchor) / hourMS)
			noise := 1.0 + 0.0004*math.Cos(float64(h)*2.3)
			switch {
			case h >= 6 && h <= 9:
				return base * 1.020 // +2% perp premium -> Short Perp + Long Spot
			case h == 18:
				return base * 0.980 // -2% perp discount -> LongOnlySpot blocks shorting Spot
			default:
				return base * 1.0005 * noise
			}
		}

		prepared, _ := makeRuntimeAndPrepare(t, pairsYAML, runner.Weights, spotSeries, perpSeries, 0.002, nil)
		navOut := &captureNavOutput{}
		res, runErr := runner.Run(context.Background(), prepared[0], nil, navOut)
		if runErr != nil {
			t.Fatalf("RegisterPairs Spot-Perp Cash-and-Carry failed: %v", runErr)
		}
		if res.Executions == 0 || len(res.Pairs) != 1 || res.Pairs[0].Trades == 0 {
			t.Fatalf("expected executed cash-and-carry pair trades, got Executions=%d Pairs=%+v", res.Executions, res.Pairs)
		}
		if res.Book.Funding >= 0 {
			t.Fatalf("expected short perp leg to earn positive funding credit (Book.Funding < 0), got %.6f", res.Book.Funding)
		}
		if res.Book.NAV <= 10000 {
			t.Fatalf("expected profitable basis convergence + funding NAV > 10000, got %.6f", res.Book.NAV)
		}
	})
}
