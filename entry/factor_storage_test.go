package entry

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
)

func storageEntrySpec(t *testing.T, pit string, pageBytes ...int64) *config.RunSpec {
	t.Helper()
	body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nexchange: {name: binance}\nmarket_type: linear\npairs: ['A/USDT:USDT', 'B/USDT:USDT']\nstake_currency: [USDT]\nexecution: {funding_policy: explicit-zero}\ndata: {pit_policy: " + pit + ", page_rows: 2, prefetch_rows: 10, max_records: 30}\nrun_policy:\n  - name: momentum-vol\n    engine: factor\n    run_timeframes: [1h]\n    params: {window: 2, k: 1}\n"
	if len(pageBytes) > 0 {
		body = strings.Replace(body, "max_records: 30}", "max_records: 30, page_bytes: "+strconv.FormatInt(pageBytes[0], 10)+"}", 1)
	}
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestStorageEntryPageByteBudgetReachesInjectedQueryAndReport(t *testing.T) {
	spec := storageEntrySpec(t, "static-approximation", 1000)
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
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
	queries := 0
	prepared, err := assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, func(ctx context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
		queries++
		if orm.SeriesReadByteLimit(ctx) != 1000 {
			t.Fatal("query context omitted page budget")
		}
		return []*orm.DataSeries{{Source: sub.Source, TimeFrame: sub.TimeFrame, Sid: sub.ExSymbol.ID, TimeMS: start, EndMS: start + 3600000, Closed: true, Values: map[string]any{"close": float64(1), "wide": strings.Repeat("x", 10000)}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	input := prepared[0].HistoricalInput.(*factorStorageInput)
	report := input.InputBudgetReport().(data.SubscriptionBudgetReport)
	if report.PageBytes != 1000 || report.StreamCount != 2 || report.ActivePageBytesEstimate != 2000 {
		t.Fatalf("compiled budget report missing: %+v", report)
	}
	reader, err := input.Open(context.Background(), prepared[0], input.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	batch, err := reader.Next(context.Background())
	var budgetErr *orm.SeriesByteLimitError
	if !errors.As(err, &budgetErr) || len(batch.Records) > 0 || queries != 1 {
		t.Fatalf("wide page entered replay: batch=%+v err=%v queries=%d", batch, err, queries)
	}
}

func TestStorageConfigPreflightRequiresExplicitApproximationBeforeSession(t *testing.T) {
	spec := storageEntrySpec(t, "strict")
	if _, err := buildFactorConfigs(spec, runner.Weights); err == nil || !strings.Contains(err.Error(), "strict PIT") {
		t.Fatalf("strict latest storage admitted: %v", err)
	}
	spec = storageEntrySpec(t, "static-approximation")
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	if len(configs) != 1 || len(configs[0].Chunks) != 0 || configs[0].Snapshot.VisibilityPolicy != "static-approximation" || configs[0].Prices.Source != "kline" {
		t.Fatal("ordinary storage defaults missing")
	}
	configs, err = buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	if configs[0].Prices.TimeFrame != "1m" || !configs[0].Execution.MarginRate.IsPositive() {
		t.Fatal("observable execution defaults missing")
	}
}

func TestStoragePreparationRejectsCanceledContextBeforeSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, cleanup, err := prepareFactorStorageInputs(ctx, nil, nil, []runner.Config{{}})
	if !errors.Is(err, context.Canceled) || cleanup != nil {
		t.Fatalf("canceled preparation opened a session: cleanup=%v err=%v", cleanup != nil, err)
	}
}

func TestStorageEntryAssemblyPagesTypedRowsAndExtendsActualTSNeeds(t *testing.T) {
	spec := storageEntrySpec(t, "static-approximation")
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
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
	sourceInfo := orm.NewSeriesInfo("ts_only", "event", []orm.SeriesField{{Name: "custom", Type: "int"}, {Name: "nullable", Type: "string"}})
	fetches := 0
	var fetchedEnd int64
	source, err := data.NewFuncDataSource(sourceInfo, func(_ context.Context, _ *orm.Subscription, _, to int64) ([]*orm.DataRecord, error) {
		fetches++
		fetchedEnd = to
		return nil, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = task.Catalog.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	anchor, end := snapshot.View().TimeRange.StartMS, snapshot.View().TimeRange.EndMS
	calls, maxLimit := 0, 0
	query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
		calls++
		maxLimit = max(maxLimit, limit)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var result []*orm.DataSeries
		if sub.Source == "ts_only" {
			if at := anchor + 123; at >= start && at < stop {
				return []*orm.DataSeries{{Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: "event", TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{"custom": int64(9007199254740993), "nullable": nil}}}, nil
			}
			return nil, nil
		}
		for at := anchor - 4*3600000; at <= end; at += 3600000 {
			if at >= start && at < stop {
				result = append(result, &orm.DataSeries{Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame, TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{"close": 100 + float64(sub.ExSymbol.ID)*float64((at-anchor)/3600000), "custom": int64(9007199254740993), "nullable": nil}})
				if len(result) == limit {
					break
				}
			}
		}
		return result, nil
	}
	prepared, err := assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, query)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || prepared[0].HistoricalInput == nil || len(prepared[0].Chunks) != 0 {
		t.Fatal("assembly eagerly read or archived full history")
	}
	initialIdentity := prepared[0].HistoricalInput.Identity()
	extra := &orm.Subscription{Source: "ts_only", TimeFrame: "event", ExSymbol: task.Symbols.GetSymbolByID(1), Fields: []string{"custom", "nullable"}}
	prepared, err = extendFactorStorageInputs(context.Background(), prepared, []*orm.Subscription{extra})
	if err != nil {
		t.Fatal(err)
	}
	if prepared[0].HistoricalInput.Identity() == initialIdentity || prepared[0].Snapshot.Schemas["ts_only"] == "" {
		t.Fatal("TS requirements absent from union identity")
	}
	prepared[0].HistoricalInput.(*factorStorageInput).assembly.bootstrap = func(ctx context.Context, plan *data.SubscriptionPlan) error {
		return plan.Bootstrap(ctx, orm.NewSeriesRepo(nil))
	}
	gotTS := false
	prepared[0].ObserveBatch = func(_ context.Context, b runner.HistoricalBatch) error {
		for _, r := range b.Records {
			if r.Series.Source == "ts_only" {
				gotTS = true
				if r.Series.Values["custom"] != int64(9007199254740993) {
					t.Fatal("concrete type narrowed")
				}
				if _, ok := r.Series.Values["nullable"]; !ok {
					t.Fatal("NULL became missing")
				}
			}
		}
		return nil
	}
	results, err := runFactorConfigs(context.Background(), prepared, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Decisions == 0 || !gotTS || maxLimit > 2 || calls < 10 {
		t.Fatalf("paged storage pipeline incomplete: rows=%+v calls=%d limit=%d TS=%v", results, calls, maxLimit, gotTS)
	}
	input := prepared[0].HistoricalInput
	reader, err := input.Open(context.Background(), prepared[0], input.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil || fetches != 1 || fetchedEnd != end+1 {
		t.Fatalf("source history bootstrap was repeated or omitted: fetches=%d close=%v", fetches, err)
	}
	previousCalls := calls
	failure := errors.New("source bootstrap failed")
	failedInput := &factorStorageInput{HistoricalInputFactory: input, plan: input.(*factorStorageInput).plan, assembly: &factorStorageAssembly{runtime: task, bootstrap: func(context.Context, *data.SubscriptionPlan) error { return failure }}}
	if reader, err := failedInput.Open(context.Background(), prepared[0], input.Ranges()[0]); !errors.Is(err, failure) || reader != nil || calls != previousCalls {
		t.Fatalf("source bootstrap failure allowed storage reader: reader=%v err=%v calls=%d/%d", reader != nil, err, calls, previousCalls)
	}
	sourceInfo.Binding.Fields = append(sourceInfo.Binding.Fields, orm.SeriesField{Name: "changed", Type: "int"})
	if _, err = input.Open(context.Background(), prepared[0], input.Ranges()[0]); err == nil {
		t.Fatal("changed source metadata accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = input.Open(ctx, prepared[0], input.Ranges()[0]); err == nil || !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "changed") {
		t.Fatal("canceled input accepted")
	}
}
