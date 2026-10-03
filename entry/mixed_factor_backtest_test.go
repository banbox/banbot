package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

type mixedReplayExchange struct{ banexg.BanExchange }

func (*mixedReplayExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "mixedfixture", MarketType: banexg.MarketLinear, CurrenciesByCode: map[string]*banexg.Currency{"USD": {Code: "USD", Precision: 8, PrecMode: banexg.PrecModeDecimalPlace}}}
}
func (*mixedReplayExchange) IsContract(string) bool                              { return true }
func (*mixedReplayExchange) CheckSymbols(symbols ...string) ([]string, []string) { return symbols, nil }
func (*mixedReplayExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	return &banexg.Market{Symbol: symbol, Base: "ASSET", Quote: "USD", Settle: "USD", Type: banexg.MarketLinear, Contract: true, Linear: true, Swap: true, ContractSize: 1,
		Precision: &banexg.Precision{Amount: 0.01, Price: 0.01, ModeAmount: banexg.PrecModeTickSize, ModePrice: banexg.PrecModeTickSize},
		Limits:    &banexg.MarketLimits{Amount: &banexg.LimitRange{}, Cost: &banexg.LimitRange{}}}, nil
}
func (*mixedReplayExchange) GetLeverage(string, float64, string) (float64, float64) { return 1, 1 }

func TestMixedReplayRunsRealTSCallbacksOnSharedMemoryAccount(t *testing.T) {
	testMixedReplayAccounts(t, false, false, false)
}

func TestMixedReplayKeepsSeparateTSAccountAndOrderReports(t *testing.T) {
	testMixedReplayAccounts(t, true, false, false)
}

func TestMixedReplayMultipleCSAccountsUseLocalTSClockAndCustomPrice(t *testing.T) {
	testMixedReplayAccounts(t, true, true, false)
}

func TestMixedReplaySeparateTSAccountReceivesFunding(t *testing.T) {
	testMixedReplayAccounts(t, true, false, true)
}

func TestMixedReplayColdHistoryIsAccountScoped(t *testing.T) {
	for _, tsHistory := range []bool{false, true} {
		t.Run(fmt.Sprintf("ts-history=%v", tsHistory), func(t *testing.T) {
			testMixedReplayAccounts(t, true, false, true, tsHistory)
		})
	}
}

func TestMixedStorageReplayTradesTSAssetOutsideFactorUniverse(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(fmt.Sprintf("separate-account=%v", separate), func(t *testing.T) {
			dir := t.TempDir()
			account := "default"
			if separate {
				account = "ts-only"
			}
			body := "config_version: 2\ntime_start: '20240101'\ntime_end: '202401010400'\nexchange: {name: mixedfixture}\nmarket_type: linear\npairs: ['ASSET1/USD:USD', 'ASSET2/USD:USD', 'ASSET3/USD:USD']\nstake_currency: [USD]\naccounts: {default: {}, ts-only: {}}\nexecution: {funding_policy: required-stream}\ndata: {pit_policy: static-approximation, page_rows: 32, prefetch_rows: 64, max_records: 1024}\nrun_policy:\n  - name: momentum-vol\n    engine: factor\n    factor: {funding_source: funding}\n    capital_weight: 0.5\n    pairs: ['ASSET1/USD:USD', 'ASSET2/USD:USD']\n    run_timeframes: [1h]\n    params: {window: 2, k: 1}\n  - name: outside-factor-ts\n    account: " + account + "\n    capital_weight: 0.5\n    pairs: ['ASSET3/USD:USD']\n    run_timeframes: [5m]\n"
			spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}, false)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			configs, err := buildFactorConfigs(spec, runner.Events)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, snapshotErr := spec.RuntimeSnapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			process := runtimepkg.NewProcess()
			t.Cleanup(process.Close)
			task, err := process.NewRuntime(runtimepkg.Options{Context: context.Background(), Config: snapshot.View(), Mode: core.RunModeBackTest, ExchangeName: "mixedfixture", Market: "linear", StartAt: snapshot.View().TimeRange.StartMS, NetDisable: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { task.Close(); task.Join() })
			for i, pair := range snapshot.View().Pairs {
				if err := task.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: int32(i + 1), Symbol: pair, Exchange: "mixedfixture", Market: "linear"}); err != nil {
					t.Fatal(err)
				}
			}
			source, err := data.NewFuncDataSource(orm.NewSeriesInfo("funding", "event", []orm.SeriesField{{Name: "rate", Type: "float"}}), func(context.Context, *orm.Subscription, int64, int64) ([]*orm.DataRecord, error) { return nil, nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := task.Catalog.RegisterDataSource(source); err != nil {
				t.Fatal(err)
			}
			anchor, end := snapshot.View().TimeRange.StartMS, snapshot.View().TimeRange.EndMS
			queried := map[string]bool{}
			query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				queried[fmt.Sprintf("%d/%s/%s", sub.ExSymbol.ID, sub.Source, sub.TimeFrame)] = true
				interval := int64(60000)
				switch sub.TimeFrame {
				case "1h":
					interval = 3600000
				case "5m":
					interval = 300000
				case "event":
					interval = 900000
				}
				var rows []*orm.DataSeries
				for at := anchor - 4*3600000; at <= end; at += interval {
					price := 100 + float64(sub.ExSymbol.ID)*float64(at-anchor)/3600000
					values := map[string]any{"open": price, "high": price, "low": price, "close": price, "volume": 100.0}
					timeMS := at - interval
					if sub.Source == "funding" {
						if at <= anchor {
							continue
						}
						values = map[string]any{"rate": 0.001}
						timeMS = at
					}
					if timeMS < start || timeMS >= stop {
						continue
					}
					rows = append(rows, &orm.DataSeries{Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame, TimeMS: timeMS, EndMS: at, Closed: true, Values: values})
					if len(rows) == limit {
						break
					}
				}
				return rows, nil
			}
			configs, err = assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, query)
			if err != nil {
				t.Fatal(err)
			}
			configs[0].ArtifactPath = filepath.Join(dir, "factor-result.json")
			configs[0].Execution.Instruments = map[int32]execution.Instrument{}
			for _, sid := range []int32{1, 2} {
				unit, err := mixedReplayInstrument(&mixedReplayExchange{}, task.Symbols.GetSymbolByID(sid).Symbol, "USD")
				if err != nil {
					t.Fatal(err)
				}
				configs[0].Execution.Instruments[sid] = unit
			}
			callbacks, fills := 0, 0
			strat.RegisterStrategy("outside-factor-ts", func(*config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{OnBar: func(job *strat.StratJob) {
					callbacks++
					if job.Symbol.ID != 3 {
						t.Fatal("TS pair policy was ignored")
					}
					if job.Env.BarNum == 2 {
						if err := job.OpenOrder(&strat.EnterReq{Tag: "outside", Amount: 1}); err != nil {
							t.Error(err)
						}
					}
				}, OnOrderChange: func(_ *strat.StratJob, _ *ormo.InOutOrder, kind int) {
					if kind == strat.OdChgEnterFill {
						fills++
					}
				}}
			})
			t.Cleanup(func() { strat.UnregisterStrategy("outside-factor-ts") })
			results, err := replayMixedEngines(context.Background(), spec, snapshot, &mixedReplayExchange{}, configs, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if callbacks == 0 || fills != 1 || !queried["3/kline/1m"] || !queried["3/kline/5m"] || !queried["3/funding/event"] || queried["3/kline/1h"] {
				t.Fatalf("callbacks=%d fills=%d streams=%v", callbacks, fills, queried)
			}
			if len(configs[0].Snapshot.Universe.Reference) != 2 || len(configs[0].Snapshot.Universe.Investable) != 2 || configs[0].Snapshot.SIDMap[3] != "" {
				t.Fatal("TS execution symbol polluted the factor universe or identity")
			}
			orders, orderErr := ormo.LoadOrdersGob(filepath.Join(dir, "ts-"+account, "orders.gob"))
			if orderErr != nil || len(orders) != 1 || orders[0].Symbol != "ASSET3/USD:USD" || orders[0].Enter.Filled != 1 {
				t.Fatalf("TS order report=%+v error=%v", orders, orderErr)
			}
			found := false
			for _, result := range results {
				if result.AccountID != account {
					continue
				}
				for _, lot := range result.Account.Lots {
					if lot.Strategy == "outside-factor-ts" && lot.Instrument.ID == "ASSET3/USD:USD" && lot.SignedSteps == 100 {
						found = true
					}
				}
				if !result.Account.SyntheticStrategyCash["outside-factor-ts"].LessThan(decimal.NewFromInt(5000)) {
					t.Fatal("TS asset funding was not charged")
				}
			}
			if !found {
				t.Fatal("TS-only symbol has no attributed filled lot")
			}
		})
	}
}

func testMixedReplayAccounts(t *testing.T, separate, secondCS, funding bool, coldHistory ...bool) {
	t.Helper()
	dir, path := factorYAMLFixture(t)
	body, _ := os.ReadFile(path)
	if len(coldHistory) > 0 {
		settings := "accounts: {default: {history: cs-history.sqlite}"
		if coldHistory[0] {
			settings += ", ts-only: {history: ts-history.sqlite}"
		}
		settings += "}"
		body = []byte(strings.Replace(string(body), "funding_policy: explicit-zero", "funding_policy: explicit-zero, "+settings, 1))
	}
	body = []byte(strings.Replace(string(body), "    engine: factor", "    engine: factor\n    capital_weight: 0.5", 1))
	body = append([]byte("time_start: '19700101'\ntime_end: '19700102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\n"), body...)
	if secondCS {
		body = append(body, []byte("  - name: MomentumVol\n    engine: factor\n    id: second-factor\n    account: ts-only\n    capital_weight: 0.5\n    run_timeframes: [1h]\n    params: {window: 2, k: 1}\n    factor: {archive: data.gob}\n")...)
	}
	body = append(body, []byte("  - name: mixed-fixture-ts\n    capital_weight: 0.5\n    run_timeframes: [1h]\n")...)
	account := "default"
	if separate {
		account = "ts-only"
		body = append([]byte("accounts: {default: {}, ts-only: {}}\n"), body...)
		body = append(body, []byte("    account: ts-only\n")...)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	c := &configs[0]
	c.Manifest.FactorPlanHash = "factor-fixture-lineage"
	c.Manifest.Parameters = map[string]float64{"factor-only": 42}
	if funding {
		c.Manifest.Costs.FundingPolicy = "required-stream"
		c.FundingSource = "funding"
	}
	c.ArtifactPath = filepath.Join(dir, "strategy.json")
	c.Execution.MarginRate = decimal.RequireFromString("0.1")
	c.Execution.MaxAccountMargin = decimal.NewFromInt(10000)
	c.Execution.MaxVirtualGross = decimal.NewFromInt(20000)
	c.Execution.StrategyGrossLimit = decimal.NewFromInt(10000)
	c.Execution.Instruments = map[int32]execution.Instrument{}
	for sid := range c.Snapshot.SIDMap {
		symbol := fmt.Sprintf("ASSET%d/USD:USD", sid)
		c.Snapshot.SIDMap[sid] = symbol
		c.Execution.Instruments[sid] = execution.Instrument{ID: symbol, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.RequireFromString("0.01"), MoneyScale: 8}
	}
	source, err := factor.OpenVersionStore(c.Chunks[0].Path, 100)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := source.Records()
	if err != nil {
		t.Fatal(err)
	}
	store, _ := factor.NewVersionStore(100)
	for _, row := range rows {
		if secondCS && row.Series.Source == "tick" {
			row.Series.Values["mark"] = row.Series.Values["price"]
		}
		if row.Series.Source == "kline" {
			price := row.Series.Values["close"]
			row.Series.Values["open"], row.Series.Values["high"], row.Series.Values["low"], row.Series.Values["volume"] = price, price, price, 100.0
			row.Series.TimeMS = row.Series.EndMS - c.DecisionInterval
		}
		if err := store.Put(row); err != nil {
			t.Fatal(err)
		}
		if funding && row.Series.Source == "tick" {
			fundingRow := row
			fundingRow.Series.Source = "funding"
			fundingRow.Series.Values = map[string]any{"rate": 0.01}
			if err := store.Put(fundingRow); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.Chunks[0].Path = filepath.Join(dir, "ohlc.gob")
	if _, err := store.Export(c.Chunks[0].Path); err != nil {
		t.Fatal(err)
	}
	if funding {
		if err := deriveArchiveIdentity(c); err != nil {
			t.Fatal(err)
		}
	}
	if secondCS {
		c.Prices.Field = "mark"
		// Both independent accounts consume the same immutable source identity,
		// but they retain independent budget and TS execution clocks.
		other := *c
		other.StrategyID, other.AccountID = "second-factor", "ts-only"
		other.ArtifactPath = filepath.Join(dir, "second-strategy.json")
		configs[1] = other
	}
	var callbacks, fills int
	strat.RegisterStrategy("mixed-fixture-ts", func(*config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			OnBar: func(job *strat.StratJob) {
				callbacks++
				if job.Symbol.ID == 1 && job.Env.BarNum == 2 {
					if err := job.OpenOrder(&strat.EnterReq{Tag: "fixture", Amount: 1}); err != nil {
						t.Error(err)
					}
				}
			},
			OnOrderChange: func(_ *strat.StratJob, _ *ormo.InOutOrder, kind int) {
				if kind == strat.OdChgEnterFill {
					fills++
				}
			},
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy("mixed-fixture-ts") })
	snapshot, snapshotErr := spec.RuntimeSnapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	results, err := replayMixedEngines(context.Background(), spec, snapshot, &mixedReplayExchange{}, configs, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(coldHistory) > 0 {
		if _, err := os.Stat(filepath.Join(dir, "cs-history.sqlite")); err != nil {
			t.Fatal("CS cold history missing", err)
		}
		_, err := os.Stat(filepath.Join(dir, "ts-history.sqlite"))
		if coldHistory[0] && err != nil || !coldHistory[0] && !os.IsNotExist(err) {
			t.Fatal("TS account inherited another account's cold history", err)
		}
	}
	wantResults := 1
	if separate {
		wantResults = 2
	}
	if callbacks != 32 || fills != 1 || len(results) != wantResults || results[0].TargetsAccepted == 0 || results[0].Account == nil {
		t.Fatalf("callbacks=%d fills=%d results=%+v", callbacks, fills, results)
	}
	found := false
	for _, lot := range results[wantResults-1].Account.Lots {
		if lot.Strategy == "mixed-fixture-ts" && lot.SignedSteps == 100 {
			found = true
		}
	}
	if !found {
		t.Fatal("TS attribution missing from shared CS account result")
	}
	if funding && !results[wantResults-1].Account.SyntheticStrategyCash[execution.StrategyID("mixed-fixture-ts")].LessThan(decimal.NewFromInt(5000)) {
		t.Fatal("TS-only account did not charge funding to its strategy")
	}
	if separate {
		if !secondCS {
			tsResult := results[wantResults-1]
			manifest := tsResult.Manifest
			if tsResult.Engine != "time_series" || tsResult.ManifestID != "" || tsResult.StrategyHash != "" || manifest.FactorPlanHash != "" || manifest.UniverseVersion != "" || manifest.Combo.Method != "" || manifest.Portfolio.K != 0 || len(manifest.Parameters) != 0 || len(manifest.Labels) != 0 || len(manifest.Snapshots) != 0 {
				t.Fatalf("TS-only account inherited factor provenance: %+v", tsResult)
			}
			if manifest.Currency != "USD" || manifest.Costs != c.Manifest.Costs || manifest.ExecutionMode != "events" || manifest.CodeRevision != core.Version || !strings.Contains(manifest.LatencyAssumption, "next visible quote after intent") {
				t.Fatalf("TS-only account lost shared simulation assumptions: %+v", manifest)
			}
		}
		for _, lot := range results[0].Account.Lots {
			if lot.Strategy == "mixed-fixture-ts" {
				t.Fatal("TS lot leaked into separate CS account")
			}
		}
	}
	orders, loadErr := ormo.LoadOrdersGob(filepath.Join(dir, "ts-"+account, "orders.gob"))
	if loadErr != nil || len(orders) != 1 || orders[0].Strategy != "mixed-fixture-ts" || orders[0].Enter.Filled != 1 {
		t.Fatalf("TS report orders=%+v error=%v", orders, loadErr)
	}
	if csv, readErr := os.ReadFile(filepath.Join(dir, "ts-"+account, "orders.csv")); readErr != nil || !strings.Contains(string(csv), "mixed-fixture-ts") {
		t.Fatalf("TS CSV missing projection: %v", readErr)
	}
	accounts := []string{"default"}
	if separate {
		accounts = append(accounts, "ts-only")
	}
	for _, id := range accounts {
		body, err := os.ReadFile(filepath.Join(dir, "account-"+id, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest execution.CommittedArchiveManifest
		if err := json.Unmarshal(body, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.SchemaVersion != execution.CommittedArchiveVersion || manifest.Status != "complete" || manifest.ExportedThrough != manifest.ThroughInclusive || len(manifest.Files) == 0 {
			t.Fatalf("incomplete account archive %s: %s", id, body)
		}
		for _, chunk := range manifest.Files {
			if _, err := os.Stat(filepath.Join(dir, "account-"+id, chunk.Name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if databases, _ := filepath.Glob(filepath.Join(dir, "*.db")); len(databases) > 0 {
		t.Fatalf("memory replay created databases: %v", databases)
	}
}
