package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
)

// buildThreeMonthRollingArchive builds a synthetic 1d archive with 1m execution ticks:
//   - SID 1 (AAA): high volume in Month 1 & Month 2, collapses in Month 3 while still trading (price bars continue!).
//   - SID 2 (BBB): steady Top-3 volume throughout Months 1..3.
//   - SID 3 (CCC): NOT listed at start; lists mid-Month 1 (day 10) with #1 volume & strongest uptrend!
//     Excluded during min_listing_days (14d), enters Investable at Month 2 reconstitution (day 30), gets bought!
//   - SID 4 (DDD): low volume in Month 1 & Month 2, surges in Month 3 to displace AAA from Top-3!
func writeRollingThreeMonthArchive(t *testing.T, totalDays int, mutateAfterDay int, futureSpike float64) (string, int64, int64) {
	t.Helper()
	day := int64(86_400_000)
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	store, err := factor.NewVersionStore(200000)
	if err != nil {
		t.Fatal(err)
	}
	addBar := func(sid int32, d int, closePrice, quoteVol float64) {
		openMS := t0 + int64(d)*day
		endMS := openMS + day
		if err := store.Put(factor.VersionRecord{
			Series: orm.DataSeries{
				Source:    orm.SeriesSourceKline,
				Sid:       sid,
				TimeFrame: "1d",
				TimeMS:    openMS,
				EndMS:     endMS,
				Closed:    true,
				Values:    map[string]any{"close": closePrice, "volume": quoteVol / closePrice, "quote": quoteVol},
			},
			EventTime:     endMS,
			Revision:      1,
			AvailableAt:   endMS,
			IngestedAt:    endMS,
			SourceVersion: "v1",
		}); err != nil {
			t.Fatal(err)
		}
		// Observable tick at endMS + 1000ms for trade execution.
		execAt := endMS + 1000
		if err := store.Put(factor.VersionRecord{
			Series: orm.DataSeries{
				Source:    "tick",
				Sid:       sid,
				TimeFrame: "event",
				TimeMS:    execAt,
				EndMS:     execAt,
				Closed:    true,
				Values:    map[string]any{"price": closePrice},
			},
			EventTime:     execAt,
			Revision:      1,
			AvailableAt:   execAt,
			IngestedAt:    execAt,
			SourceVersion: "v1",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Pre-start 20 days (-20..-1) so AAA, BBB, DDD satisfy min_listing_days=14 & lookback=15 at day 0.
	for d := -20; d < totalDays; d++ {
		step := float64(d + 25)
		// AAA: strong positive trend (+2%/day) so it is selected LONG in Month 1, then volume collapses from day 45 onwards!
		volA := 5_000_000.0
		if d >= 45 {
			volA = 1_000.0 // dries up before Month 3 reconstitution (day 60), while price bars remain live!
		}
		// BBB: mild downtrend so it takes the short leg or middle rank, steady high volume
		volB := 4_000_000.0
		// DDD: low volume until day 45, then surges into Top-3 for Month 3; mild uptrend
		volD := 50_000.0
		if d >= 45 {
			volD = 6_000_000.0
		}
		if mutateAfterDay > 0 && d >= mutateAfterDay {
			volA = futureSpike
			volD = 10.0
		}
		priceA := 100.0 * math.Pow(1.015, step)
		priceB := 200.0 * math.Pow(0.995, step)
		priceD := 80.0 * math.Pow(1.010, step)
		addBar(1, d, priceA, volA)
		addBar(2, d, priceB, volB)
		addBar(4, d, priceD, volD)

		// CCC lists mid-Month 1 at day 10 with #1 volume and strongest uptrend (+3%/day)!
		if d >= 10 {
			volC := 10_000_000.0
			priceC := 50.0 * math.Pow(1.030, float64(d-10+1))
			addBar(3, d, priceC, volC)
		}
		// EEE lists only after mutateAfterDay (when testing no-lookahead).
		if mutateAfterDay > 0 && d >= mutateAfterDay {
			addBar(5, d, 300.0+step, futureSpike)
		}
	}

	archivePath := filepath.Join(t.TempDir(), "rolling.gob")
	if _, err := store.Export(archivePath); err != nil {
		t.Fatal(err)
	}
	return archivePath, t0 + day, t0 + int64(totalDays)*day + 2000
}

func TestRollingUniverseListingEntryAndGracefulDropoutExit(t *testing.T) {
	day := int64(86_400_000)
	archivePath, fromMS, toMS := writeRollingThreeMonthArchive(t, 75, 0, 0)
	sids := []int32{1, 2, 3, 4}
	sidMap := map[int32]string{
		1: "AAA/USDT:USDT",
		2: "BBB/USDT:USDT",
		3: "CCC/USDT:USDT",
		4: "DDD/USDT:USDT",
	}

	makeBaseConfig := func() runner.Config {
		return runner.Config{
			Mode:             runner.Weights,
			MaxRecords:       250000,
			MaxPending:       64,
			DecisionInterval: day,
			LatencyMS:        1,
			ExpiryMS:         60_000,
			InitialNAV:       10_000,
			StrategyID:       "roll-test",
			AccountID:        "default",
			Chunks:           []runner.Chunk{{Path: archivePath, From: fromMS, To: toMS}},
			Prices:           runner.PriceStream{Source: "tick", TimeFrame: "event", Field: "price"},
			Expressions: &expr.Spec{
				SchemaVersion: 1,
				TimeFrame:     "1d",
				Bindings:      map[string]expr.Binding{"kline": {Source: orm.SeriesSourceKline, TimeFrame: "1d"}},
				Outputs:       map[string]string{"mom": "ts.return(positive(kline.close), 5)"},
			},
			RollingUniverse: &factor.RollingUniverseConfig{
				Mode:             "rolling_liquid_top",
				TopN:             2, // Top-2 pool: Month 1=[AAA,BBB], Month 2=[AAA/BBB,CCC], Month 3=[CCC,DDD] (AAA dropped!)
				LookbackDays:     15,
				MinListingDays:   14,
				ReconstituteDays: 30,
			},
			Snapshot: factor.SnapshotSpec{
				Universe:          factor.Universe{Version: "cand-v1", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: false},
				SIDMap:            sidMap,
				Schemas:           map[string]string{orm.SeriesSourceKline: "k1", "tick": "t1"},
				SourceVersions:    map[string]string{orm.SeriesSourceKline: "v1", "tick": "v1"},
				AdjustmentVersion: "raw",
				VisibilityPolicy:  "available-at",
			},
			Manifest: research.ManifestSpec{
				CodeRevision: core.Version,
				Currency:     "USDT",
				Costs:        research.CostSpec{FeeRate: 0.0005, SlippageRate: 0.0002, FundingPolicy: "explicit-zero"},
				Labels:       []research.LabelSpec{{Name: "1d", Kind: research.ExecutableReturn, Horizon: day, PeriodsPerYear: 365.25}},
				Portfolio:    research.PortfolioDefinition{K: 1, LongNotional: 0.5, ShortNotional: 0.5, Mode: factor.Full},
			},
		}
	}

	// 1. Default TopBottomK path: verify Month 1 (CCC excluded), Month 2 (CCC enters & bought), Month 3 (AAA drops out & liquidates cleanly to 0).
	cfg := makeBaseConfig()
	var rawBuf bytes.Buffer
	out := &runner.JSONOutput{Writer: &rawBuf, SIDs: sids, NoPanel: true}
	res, err := runner.Run(context.Background(), cfg, nil, out)
	if err != nil {
		t.Fatalf("TopBottomK rolling run failed: %v", err)
	}
	if res.Incomplete != 0 || res.DelistExits != 0 || res.UniverseNominal != 2 {
		t.Fatalf("unexpected PIT/incomplete stats: incomplete=%d delistExits=%d nominal=%d", res.Incomplete, res.DelistExits, res.UniverseNominal)
	}
	// At end of Month 3 (day 75), AAA (SID 1) rotated out of Investable at day 60 while still having live prices -> MUST be 0 quantity in Book!
	if math.Abs(res.Book.Quantities[1]) > 1e-12 {
		t.Fatalf("AAA (SID 1) dropped from Investable in Month 3 but remained trapped in Book: quantities=%v", res.Book.Quantities)
	}
	// CCC (SID 3) listed mid-Month 1 and entered Top-2 in Month 2 -> MUST be held long in Month 3!
	if res.Book.Quantities[3] <= 0 {
		t.Fatalf("CCC (SID 3) failed to enter Investable and get bought: quantities=%v", res.Book.Quantities)
	}

	// Inspect emitted decision events to verify CCC was never targeted in Month 1 (< day 30), entered in Month 2, and AAA exited in Month 3.
	var month1HadC, month2HadC, month1HadA, month3HadA bool
	t0 := fromMS - day
	for _, line := range strings.Split(strings.TrimSpace(rawBuf.String()), "\n") {
		var evt struct {
			Kind         string
			DecisionTime int64
			Targets      map[int32]float64
		}
		if json.Unmarshal([]byte(line), &evt) != nil || evt.Kind != "decision" {
			continue
		}
		dIdx := int((evt.DecisionTime-t0)/day) - 1 // 0-based day index
		if dIdx < 30 {
			if evt.Targets[3] != 0 {
				month1HadC = true
			}
			if evt.Targets[1] > 0 {
				month1HadA = true
			}
		} else if dIdx >= 30 && dIdx < 60 {
			if evt.Targets[3] > 0 {
				month2HadC = true
			}
		} else if dIdx >= 60 {
			if evt.Targets[1] != 0 {
				month3HadA = true
			}
		}
	}
	if month1HadC || !month1HadA || !month2HadC || month3HadA {
		t.Fatalf("timeline invariant broken: month1HadA=%v month1HadC=%v month2HadC=%v month3HadA=%v", month1HadA, month1HadC, month2HadC, month3HadA)
	}

	// 2. LifecyclePolicy (lifecycle-v1) path with holding.min_bars and rebalance.every_bars:
	//    Verify forced-exit on !investable[sid] cleanly liquidates AAA to 0 in Month 3 even with min_bars!
	lifeCfg := makeBaseConfig()
	lifeCfg.Manifest.Portfolio = research.PortfolioDefinition{
		Policy:        "lifecycle-v1",
		Mode:          factor.Full,
		LongNotional:  1.0,
		ShortNotional: 0.0,
		Rebalance:     &factor.RebalanceConfig{EveryBars: 5},
		Selection:     &factor.SelectionConfig{LongK: 2},
		Holding:       &factor.HoldingConfig{MinBars: 10},
	}
	var lifeBuf bytes.Buffer
	lifeRes, err := runner.Run(context.Background(), lifeCfg, nil, &runner.JSONOutput{Writer: &lifeBuf, SIDs: sids, NoPanel: true})
	if err != nil {
		t.Fatalf("lifecycle-v1 rolling run failed: %v", err)
	}
	if math.Abs(lifeRes.Book.Quantities[1]) > 1e-12 {
		t.Fatalf("lifecycle-v1 failed to liquidate AAA (SID 1) after Month 3 dropout: quantities=%v", lifeRes.Book.Quantities)
	}
	if lifeRes.Book.Quantities[3] <= 0 || lifeRes.Book.Quantities[4] <= 0 {
		t.Fatalf("lifecycle-v1 expected Month 3 Top-2 [CCC, DDD] held long, got quantities=%v", lifeRes.Book.Quantities)
	}
}

func TestRollingUniverseStrictPITNoLookaheadReplay(t *testing.T) {
	day := int64(86_400_000)
	// Archive 1: 70 days normal. Archive 2: identical up to day 40, wild volume/new listing after day 40.
	arch1, fromMS, _ := writeRollingThreeMonthArchive(t, 70, 0, 0)
	arch2, _, _ := writeRollingThreeMonthArchive(t, 70, 40, 1e12)
	midToMS := (fromMS - day) + 40*day + 2000 // replay stops right at day 40 execution

	runWithArchive := func(path string, extraSID bool) (runner.Result, string) {
		sids := []int32{1, 2, 3, 4}
		sidMap := map[int32]string{1: "AAA/USDT:USDT", 2: "BBB/USDT:USDT", 3: "CCC/USDT:USDT", 4: "DDD/USDT:USDT"}
		if extraSID {
			sids = append(sids, 5)
			sidMap[5] = "EEE/USDT:USDT"
		}
		cfg := runner.Config{
			Mode:             runner.Weights,
			MaxRecords:       250000,
			MaxPending:       64,
			DecisionInterval: day,
			LatencyMS:        1,
			ExpiryMS:         60_000,
			InitialNAV:       10_000,
			StrategyID:       "pit-check",
			AccountID:        "default",
			Chunks:           []runner.Chunk{{Path: path, From: fromMS, To: midToMS}},
			Prices:           runner.PriceStream{Source: "tick", TimeFrame: "event", Field: "price"},
			Expressions: &expr.Spec{
				SchemaVersion: 1,
				TimeFrame:     "1d",
				Bindings:      map[string]expr.Binding{"kline": {Source: orm.SeriesSourceKline, TimeFrame: "1d"}},
				Outputs:       map[string]string{"mom": "ts.return(positive(kline.close), 5)"},
			},
			RollingUniverse: &factor.RollingUniverseConfig{
				Mode:             "rolling_liquid_top",
				TopN:             2,
				LookbackDays:     15,
				MinListingDays:   14,
				ReconstituteDays: 30,
			},
			Snapshot: factor.SnapshotSpec{
				Universe:          factor.Universe{Version: "cand-v1", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids},
				SIDMap:            sidMap,
				Schemas:           map[string]string{orm.SeriesSourceKline: "k1", "tick": "t1"},
				SourceVersions:    map[string]string{orm.SeriesSourceKline: "v1", "tick": "v1"},
				AdjustmentVersion: "raw",
				VisibilityPolicy:  "available-at",
			},
			Manifest: research.ManifestSpec{
				CodeRevision: core.Version,
				Currency:     "USDT",
				Costs:        research.CostSpec{FeeRate: 0.0005, SlippageRate: 0.0002, FundingPolicy: "explicit-zero"},
				Labels:       []research.LabelSpec{{Name: "1d", Kind: research.ExecutableReturn, Horizon: day, PeriodsPerYear: 365.25}},
				Portfolio:    research.PortfolioDefinition{K: 1, LongNotional: 0.5, ShortNotional: 0.5, Mode: factor.Full},
			},
		}
		var buf bytes.Buffer
		res, err := runner.Run(context.Background(), cfg, nil, &runner.JSONOutput{Writer: &buf, SIDs: []int32{1, 2, 3, 4}, NoPanel: true})
		if err != nil {
			t.Fatal(err)
		}
		return res, buf.String()
	}

	res1, log1 := runWithArchive(arch1, false)
	res2, log2 := runWithArchive(arch2, true)
	if res1.Decisions != res2.Decisions || res1.Executions != res2.Executions || math.Abs(res1.Book.NAV-res2.Book.NAV) > 1e-9 {
		t.Fatalf("future post-cutoff data leaked into PIT replay: res1=%+v res2=%+v", res1.Book, res2.Book)
	}
	for _, sid := range []int32{1, 2, 3, 4} {
		if math.Abs(res1.Book.Quantities[sid]-res2.Book.Quantities[sid]) > 1e-12 {
			t.Fatalf("quantity mismatch on sid %d: %v vs %v", sid, res1.Book.Quantities, res2.Book.Quantities)
		}
	}
	// Verify every decision target map matches identically.
	extractTargets := func(s string) []map[int32]float64 {
		var out []map[int32]float64
		for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
			var evt struct {
				Kind    string
				Targets map[int32]float64
			}
			if json.Unmarshal([]byte(line), &evt) == nil && evt.Kind == "decision" {
				out = append(out, evt.Targets)
			}
		}
		return out
	}
	t1, t2 := extractTargets(log1), extractTargets(log2)
	if len(t1) != len(t2) {
		t.Fatalf("decision count mismatch: %d vs %d", len(t1), len(t2))
	}
	for i := range t1 {
		for sid := int32(1); sid <= 5; sid++ {
			if math.Abs(t1[i][sid]-t2[i][sid]) > 1e-12 {
				t.Fatalf("decision %d target mismatch on sid %d: %v vs %v", i, sid, t1[i], t2[i])
			}
		}
	}
}

func TestStorageYAMLRollingUniverseProjectsVolumeAndExecutes(t *testing.T) {
	yamlBody := `config_version: 2
time_start: '20240115'
time_end: '20240220'
exchange: {name: binance}
market_type: linear
pairs: ['AAA/USDT:USDT', 'BBB/USDT:USDT', 'CCC/USDT:USDT', 'CORE/USDT:USDT']
stake_currency: [USDT]
execution: {mode: weights, funding_policy: explicit-zero}
data: {pit_policy: static-approximation, page_rows: 200, prefetch_rows: 1000, max_records: 50000}
run_policy:
  - name: pure-close-mom
    engine: factor
    run_timeframes: [1d]
    prices: {source: kline, timeframe: 1m, field: close}
    decision: {expiry_ms: 120000}
    universe:
      mode: rolling_liquid_top
      top_n: 2
      lookback_days: 7
      min_listing_days: 5
      reconstitute_days: 10
      always_include: [CORE]
    portfolio:
      k: 1
      long_notional: 0.5
      short_notional: 0.5
    expressions:
      schema_version: 1
      timeframe: 1d
      bindings:
        kline: {source: kline, timeframe: 1d}
      outputs:
        mom: 'ts.return(positive(kline.close), 3)'
`
	spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: yamlBody}, false)
	if loadErr != nil {
		t.Fatalf("LoadRunSpec: %v", loadErr)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatalf("buildFactorConfigs: %v", err)
	}
	resolved, err := resolvedFactorValues(spec, configs)
	if err != nil || resolved[0]["rolling_universe"].Value == nil {
		t.Fatalf("resolved rolling_universe missing: %v err=%v", resolved, err)
	}
	snapshot, snapshotErr := spec.RuntimeSnapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	process := runtimectx.NewProcess()
	t.Cleanup(process.Close)
	task, err := process.NewRuntime(runtimectx.Options{Context: context.Background(), Config: snapshot.View(), Mode: core.RunModeBackTest, ExchangeName: "binance", Market: "linear", StartAt: snapshot.View().TimeRange.StartMS, NetDisable: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { task.Close(); task.Join() })
	for i, pair := range snapshot.View().Pairs {
		if err := task.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: int32(i + 1), Symbol: pair, Exchange: "binance", Market: "linear"}); err != nil {
			t.Fatal(err)
		}
	}

	day := int64(86_400_000)
	minute := int64(60_000)
	anchor := snapshot.View().TimeRange.StartMS
	end := snapshot.View().TimeRange.EndMS
	sawQuoteProjection := false

	query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
		if sub.TimeFrame == "1d" {
			if slices.Contains(sub.Fields, "quote") && slices.Contains(sub.Fields, "volume") {
				sawQuoteProjection = true
			}
			var rows []*orm.DataSeries
			firstOpen := anchor - 12*day
			if sub.ExSymbol.ID == 3 {
				// CCC lists 2 days after anchor -> excluded at first reconstitution, included later!
				firstOpen = anchor + 2*day
			}
			for openMS := firstOpen; openMS < end; openMS += day {
				endMS := openMS + day
				if openMS < start || openMS >= stop {
					continue
				}
				dIdx := float64((openMS - firstOpen) / day)
				var price, quote float64
				switch sub.ExSymbol.ID {
				case 1: // AAA: strong early volume, collapses 12 days after anchor
					price = 100 * math.Pow(1.02, dIdx+1)
					quote = 5_000_000
					if openMS >= anchor+12*day {
						quote = 100
					}
				case 2: // BBB: steady volume
					price = 100 * math.Pow(0.99, dIdx+1)
					quote = 3_000_000
				case 3: // CCC: mid-run listing with top volume & strong momentum
					price = 50 * math.Pow(1.04, dIdx+1)
					quote = 9_000_000
				case 4: // CORE: tiny volume, kept via always_include
					price = 200 * math.Pow(1.001, dIdx+1)
					quote = 500
				}
				rows = append(rows, &orm.DataSeries{
					Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: "1d",
					TimeMS: openMS, EndMS: endMS, Closed: true,
					Values: map[string]any{"close": price, "volume": quote / price, "quote": quote},
				})
				if len(rows) == limit {
					break
				}
			}
			return rows, nil
		}
		// 1m price stream: emit one 1m bar right after each daily close so execution fills promptly without generating 50k rows.
		var rows []*orm.DataSeries
		firstOpen := anchor
		if sub.ExSymbol.ID == 3 {
			firstOpen = anchor + 2*day
		}
		for dayClose := firstOpen + day; dayClose <= end; dayClose += day {
			barOpen := dayClose
			barEnd := dayClose + minute
			if barOpen < start || barOpen >= stop {
				continue
			}
			dIdx := float64((dayClose - firstOpen) / day)
			price := 100.0
			switch sub.ExSymbol.ID {
			case 1:
				price = 100 * math.Pow(1.02, dIdx)
			case 2:
				price = 100 * math.Pow(0.99, dIdx)
			case 3:
				price = 50 * math.Pow(1.04, dIdx)
			case 4:
				price = 200 * math.Pow(1.001, dIdx)
			}
			rows = append(rows, &orm.DataSeries{
				Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: "1m",
				TimeMS: barOpen, EndMS: barEnd, Closed: true,
				Values: map[string]any{"close": price},
			})
			if len(rows) == limit {
				break
			}
		}
		return rows, nil
	}

	prepared, err := assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, query)
	if err != nil {
		t.Fatalf("assembleFactorStorageInputs: %v", err)
	}
	prepared[0].HistoricalInput.(*factorStorageInput).assembly.bootstrap = func(ctx context.Context, plan *data.SubscriptionPlan) error {
		return plan.Bootstrap(ctx, orm.NewSeriesRepo(nil))
	}
	results, err := runFactorConfigs(context.Background(), prepared, nil, io.Discard)
	if err != nil {
		t.Fatalf("runFactorConfigs: %v", err)
	}
	if !sawQuoteProjection {
		t.Fatal("storage assembly did not project quote/volume for rolling universe on a close-only expression strategy")
	}
	res := results[0]
	if res.Decisions == 0 || res.Executions == 0 || math.Abs(res.Book.Quantities[1]) > 1e-12 || res.Book.Quantities[3] <= 0 {
		t.Fatalf("unexpected storage rolling result: decisions=%d execs=%d quantities=%v", res.Decisions, res.Executions, res.Book.Quantities)
	}
}
