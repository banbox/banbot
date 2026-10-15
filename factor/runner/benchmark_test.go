package runner

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
)

type benchFanout struct {
	sinks   []*AccountSink
	budgets []float64
	peak    uint64
}

func (m *benchFanout) ObserveQuote(ctx context.Context, sid int32, q backtest.Quote, at int64) error {
	for _, s := range m.sinks {
		if err := s.ObserveQuote(ctx, sid, q, at); err != nil {
			return err
		}
	}
	return nil
}
func (m *benchFanout) StrategyNAV(ctx context.Context, at int64) (float64, error) {
	total := 0.0
	for i, s := range m.sinks {
		nav, err := s.StrategyNAV(ctx, at)
		if err != nil {
			return 0, err
		}
		m.budgets[i] = nav
		total += nav
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	m.peak = max(m.peak, stats.HeapAlloc)
	return total / float64(len(m.sinks)), nil
}
func (m *benchFanout) StrategyState(ctx context.Context, at int64) (backtest.State, error) {
	result := backtest.State{Quantities: map[int32]float64{}}
	for _, s := range m.sinks {
		v, err := s.StrategyState(ctx, at)
		if err != nil {
			return result, err
		}
		result.NAV += v.NAV
		result.Cash += v.Cash
		result.Fees += v.Fees
		result.Slippage += v.Slippage
		result.Turnover += v.Turnover
		for sid, q := range v.Quantities {
			result.Quantities[sid] += q
		}
	}
	return result, nil
}
func (m *benchFanout) ProcessSnapshot(ctx context.Context, p *factor.TargetPortfolio, q map[int32]backtest.Quote, at int64) error {
	for i, s := range m.sinks {
		spec := p.Spec()
		spec.StrategyID = s.StrategyID
		spec.AccountID = s.AccountID
		spec.Budget.NAV = m.budgets[i]
		copy, err := factor.NewTargetPortfolio(spec, p.Targets())
		if err != nil {
			return err
		}
		if err = s.ProcessSnapshot(ctx, copy, q, at); err != nil {
			return err
		}
	}
	return nil
}

func benchSize(b *testing.B, name string, def int) int {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		b.Fatalf("%s requires positive integer", name)
	}
	return n
}

// BenchmarkSharedArchivePipeline includes real immutable archive input,
// snapshots/barrier, one shared 20-output DAG, incremental labels/diagnostics,
// strategy NAV decisions, and two real owner/SQLite/coordinator/paper accounts.
// Raw archives are bounded by chunk bars; no full-history panel is retained.
func BenchmarkSharedArchivePipeline(b *testing.B) {
	assets := benchSize(b, "BANBOT_FACTOR_BENCH_ASSETS", 24)
	hours := benchSize(b, "BANBOT_FACTOR_BENCH_HOURS", 32)
	consumers := benchSize(b, "BANBOT_FACTOR_BENCH_STRATEGIES", 1)
	chunkBars := benchSize(b, "BANBOT_FACTOR_BENCH_CHUNK_BARS", 24)
	if assets < 4 {
		b.Fatal("at least four assets required")
	}
	ctx := context.Background()
	root := b.TempDir()
	const hour int64 = 3600000
	sids := make([]int32, assets)
	symbols := map[int32]string{}
	instruments := map[int32]execution.Instrument{}
	for i := range sids {
		sid := int32(i + 1)
		sids[i] = sid
		symbols[sid] = fmt.Sprintf("asset-%d", sid)
		instruments[sid] = execution.Instrument{ID: symbols[sid], Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.001"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.RequireFromString("0.000001"), MoneyScale: 8}
	}
	chunks := []Chunk{}
	for start := 1; start <= hours; start += chunkBars {
		end := min(hours, start+chunkBars-1)
		store, _ := factor.NewVersionStore(assets * chunkBars * 2)
		for bar := start; bar <= end; bar++ {
			for _, sid := range sids {
				price := 100 + 5*math.Sin(float64(bar)/24+float64(sid)*.7) + float64(sid)*.01 + float64(bar)/100000
				price = math.Round(price*1000000) / 1000000
				for _, source := range []string{"kline", "tick"} {
					at := int64(bar) * hour
					freq, field := "1h", "close"
					if source == "tick" {
						at++
						freq, field = "event", "price"
					}
					if err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: source, Sid: sid, TimeFrame: freq, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{field: price, "custom": int64(bar), "nullable": nil}}, EventTime: at, Revision: 1, AvailableAt: at, IngestedAt: at, SourceVersion: "v1"}); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
		path := filepath.Join(root, fmt.Sprintf("chunk-%06d.gob", start))
		if _, err := store.Export(path); err != nil {
			b.Fatal(err)
		}
		chunks = append(chunks, Chunk{Path: path, From: int64(start) * hour, To: int64(end)*hour + 1})
	}
	closeNode := factor.Field("kline", "close", "1h")
	builder := factor.New().Add("momentum", factor.Return(closeNode, 24)).Add("volatility", factor.StdDev(factor.Return(closeNode, 1), 24, 1))
	for i := 1; i <= 18; i++ {
		builder.Add(fmt.Sprintf("aux-%02d", i), factor.EMA(factor.Return(closeNode, 1), i+2))
	}
	plan, err := builder.Compile()
	if err != nil {
		b.Fatal(err)
	}
	cfg := Config{Plan: plan, Mode: Events, Chunks: chunks, MaxRecords: assets * chunkBars * 2, MaxPending: 8, DecisionInterval: hour, LatencyMS: 1, ExpiryMS: 100, Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Version: "static", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}, SIDMap: symbols, Schemas: map[string]string{"kline": "v1", "tick": "v1"}, SourceVersions: map[string]string{"kline": "v1", "tick": "v1"}, VisibilityPolicy: "available-at"}, Factor: research.DefaultMomentumVolConfig(), Combo: research.ComboSpec{Method: research.Fixed, Columns: []string{"momentum", "volatility"}, Weights: map[string]float64{"momentum": 1, "volatility": -1}}, Manifest: research.ManifestSpec{Currency: "USD", CodeRevision: "benchmark-fixture", Portfolio: research.PortfolioDefinition{K: min(5, assets/2), LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Labels: []research.LabelSpec{{Name: "1h", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 8760}}, Costs: research.CostSpec{FundingPolicy: "explicit-zero", FeeRate: .0001, SlippageRate: .0001}}, StrategyID: "shared-template", AccountID: "fanout", InitialNAV: 10000, Prices: PriceStream{"tick", "event", "price"}}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		registry := &execution.AccountRegistry{}
		fan := &benchFanout{budgets: make([]float64, consumers*2)}
		services := []*execution.SharedAccount{}
		adapters := []*PaperAdapter{}
		limits := map[execution.StrategyID]decimal.Decimal{}
		for i := 0; i < consumers; i++ {
			limits[execution.StrategyID(fmt.Sprintf("strategy-%d", i))] = decimal.NewFromInt(1000000000)
		}
		for account := 0; account < 2; account++ {
			nav := decimal.NewFromInt(int64(consumers * 10000))
			adapter, err := NewPaperAdapter(nav, decimal.RequireFromString("0.0001"), decimal.RequireFromString("0.0001"))
			if err != nil {
				b.Fatal(err)
			}
			adapters = append(adapters, adapter)
			key := execution.AccountKey{VenueSessionIdentity: "bench", Account: fmt.Sprintf("account-%d", account), SettlementDomain: "USD"}
			handle, err := registry.Acquire(key)
			if err != nil {
				b.Fatal(err)
			}
			service, err := execution.NewSharedAccount(handle, execution.SharedExecutionOptions{StorePath: filepath.Join(root, fmt.Sprintf("ledger-%d-%d.db", iteration, account)), SenderLeaseDir: filepath.Join(root, "leases"), Adapter: adapter, AuthoritativeSnapshot: true})
			if err != nil {
				b.Fatal(err)
			}
			services = append(services, service)
			borrow := service.Borrow()
			if err = borrow.CashEvent(execution.CashEvent{ID: "initial", Kind: execution.ExternalCashChange, AccountDelta: nav, Postings: []execution.CashPosting{{Amount: nav}}}); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < consumers; i++ {
				id := execution.StrategyID(fmt.Sprintf("strategy-%d", i))
				capital := decimal.NewFromInt(10000)
				if err = borrow.CashEvent(execution.CashEvent{ID: string(id) + "/capital", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: capital.Neg()}, {Strategy: id, Amount: capital}}}); err != nil {
					b.Fatal(err)
				}
				sink := &AccountSink{Account: service.Borrow(), AccountID: key.Account, StrategyID: string(id), Currency: "USD", Instruments: instruments, Paper: adapter, QuoteTTLMS: cfg.DecisionInterval + cfg.ExpiryMS, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: nav, MaxVirtualGross: decimal.NewFromInt(1000000000), StrategyGrossLimits: limits}}
				if err := sink.RegisterExecution(); err != nil {
					b.Fatal(err)
				}
				fan.sinks = append(fan.sinks, sink)
			}
			if err = borrow.Reconcile("initial-reconcile", 0); err != nil {
				b.Fatal(err)
			}
			borrow.Release()
		}
		result, err := Run(ctx, cfg, fan, nil)
		if err != nil {
			b.Fatal(err)
		}
		fills := 0
		for _, a := range adapters {
			fills += a.Metrics().Fills
		}
		for _, s := range fan.sinks {
			s.Account.Release()
		}
		registry.Close()
		for _, s := range services {
			if err = s.Close(); err != nil {
				b.Fatal(err)
			}
		}
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		b.ReportMetric(float64(fan.peak)/1024/1024, "peak-heap-MiB")
		b.ReportMetric(float64(stats.HeapAlloc)/1024/1024, "retained-heap-MiB")
		b.ReportMetric(float64(assets*hours), "asset-bars")
		b.ReportMetric(float64(consumers), "strategies")
		b.ReportMetric(2, "accounts")
		b.ReportMetric(float64(fills), "real-fills")
		b.ReportMetric(float64(result.Decisions)/time.Since(started).Seconds(), "decisions/s")
		updates := uint64(0)
		for _, n := range result.NodeUpdates {
			updates += n
		}
		b.ReportMetric(float64(updates), "node-updates")
		b.ReportMetric(float64(result.NodeCount), "unique-nodes")
		b.ReportMetric(float64(result.MaxRawRecords), "raw-rows-max")
		b.ReportMetric(float64(result.MaxPendingEvaluations), "pending-frames-max")
		b.ReportMetric(float64(result.MaxRetainedValues), "retained-values-max")
	}
}
