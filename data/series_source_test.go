package data

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type stubSeriesSource struct {
	info           *orm.SeriesInfo
	version        string
	fetchCount     int
	subscribeCount int
	rows           []*orm.DataRecord
	subscribedSubs [][]*strat.DataSub
	subscribeErr   error
	fetchErr       error
}

func newStubRegistrySource(name string) *stubSeriesSource {
	return &stubSeriesSource{
		info: &orm.SeriesInfo{
			Name:      name,
			TimeFrame: "1d",
			Binding: orm.SeriesBinding{
				Table:      name,
				TimeColumn: "ts",
				EndColumn:  "end_ms",
				SIDColumn:  "sid",
				Fields: []orm.SeriesField{
					{Name: "value", Type: "float", Role: "value"},
				},
			},
		},
	}
}

func resetDataSourcesForTest(t *testing.T) {
	t.Helper()
	dataSourcesMu.Lock()
	old := dataSources
	oldSta := dataSourceSta
	dataSources = make(map[string]DataSource)
	dataSourceSta = make(map[string]*DataSourceStatus)
	dataSourcesMu.Unlock()
	t.Cleanup(func() {
		dataSourcesMu.Lock()
		dataSources = old
		dataSourceSta = oldSta
		dataSourcesMu.Unlock()
	})
}

func (s *stubSeriesSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *stubSeriesSource) Version() string {
	return s.version
}

func (s *stubSeriesSource) FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	s.fetchCount++
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}
	return append([]*orm.DataRecord(nil), s.rows...), nil
}

func (s *stubSeriesSource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error {
	s.subscribeCount++
	cp := make([]*strat.DataSub, 0, len(subs))
	for _, sub := range subs {
		if sub == nil {
			cp = append(cp, nil)
			continue
		}
		dup := *sub
		cp = append(cp, &dup)
	}
	s.subscribedSubs = append(s.subscribedSubs, cp)
	return s.subscribeErr
}

type stubDataSink struct{}

type stubSeriesRepo struct {
	ensureTableCalls int
	insertCalls      int
	deleteCalls      int
	updateCalls      int
	missingCalls     int
	queryRows        []*orm.DataRecord
	missing          []orm.MSRange
	coverageRows     []*orm.DataRecord
	coverageStart    int64
	coverageEnd      int64
	ensureTableErr   *errs.Error
	insertErr        *errs.Error
	queryErr         *errs.Error
	deleteErr        *errs.Error
	updateErr        *errs.Error
}

func (s *stubSeriesRepo) EnsureSeriesTable(ctx context.Context, info *orm.SeriesInfo) *errs.Error {
	s.ensureTableCalls++
	return s.ensureTableErr
}

func (s *stubSeriesRepo) InsertSeriesBatch(ctx context.Context, info *orm.SeriesInfo, rows []*orm.DataRecord) *errs.Error {
	s.insertCalls++
	return s.insertErr
}

func (s *stubSeriesRepo) QuerySeriesRange(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*orm.DataRecord, *errs.Error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	var out []*orm.DataRecord
	for _, row := range s.queryRows {
		if row == nil || row.TimeMS < startMS || row.TimeMS >= endMS {
			continue
		}
		out = append(out, row)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *stubSeriesRepo) DeleteSeriesRange(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	s.deleteCalls++
	return s.deleteErr
}

func (s *stubSeriesRepo) MissingSeriesRanges(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) ([]orm.MSRange, *errs.Error) {
	s.missingCalls++
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	if s.missing != nil {
		return append([]orm.MSRange(nil), s.missing...), nil
	}
	return []orm.MSRange{{Start: startMS, Stop: endMS}}, nil
}

func (s *stubSeriesRepo) UpdateSeriesRange(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	s.updateCalls++
	return s.updateErr
}

func (s *stubSeriesRepo) UpdateSeriesCoverage(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64, rows []*orm.DataRecord) *errs.Error {
	s.updateCalls++
	s.coverageStart = startMS
	s.coverageEnd = endMS
	s.coverageRows = append([]*orm.DataRecord(nil), rows...)
	return s.updateErr
}

func (s *stubSeriesRepo) GetSeriesRange(ctx context.Context, info *orm.SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	return 0, 0, nil
}

func (s stubDataSink) Emit(sub *strat.DataSub, rows []*orm.DataRecord) error {
	return nil
}

func TestRegisterDataSourceRejectsDuplicates(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_dup_test")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	if err := RegisterDataSource(src); err == nil {
		t.Fatalf("expected duplicate data source registration to fail")
	}
	status := ListDataSourceStatus()
	if len(status) != 1 || status[0].DuplicateRegistrations != 1 {
		t.Fatalf("expected duplicate registration to be visible, got %+v", status)
	}
}

func TestRegisterDataSourceRejectsNil(t *testing.T) {
	resetDataSourcesForTest(t)
	if err := RegisterDataSource(nil); err == nil {
		t.Fatalf("expected nil data source registration to fail")
	}
}

func TestRegisterDataSourceRejectsInvalidInfo(t *testing.T) {
	resetDataSourcesForTest(t)
	src := &stubSeriesSource{info: &orm.SeriesInfo{
		Name:      "macro_invalid_test",
		TimeFrame: "1d",
		Binding: orm.SeriesBinding{
			Table:      "macro_invalid_test",
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			Fields:     []orm.SeriesField{{Name: "value", Type: "unknown"}},
		},
	}}
	if err := RegisterDataSource(src); err == nil {
		t.Fatalf("expected invalid data source registration to fail")
	}
}

func TestGetAndListDataSources(t *testing.T) {
	resetDataSourcesForTest(t)
	alpha := newStubRegistrySource("macro_alpha_test")
	beta := newStubRegistrySource("macro_beta_test")
	if err := RegisterDataSource(beta); err != nil {
		t.Fatalf("RegisterDataSource beta failed: %v", err)
	}
	if err := RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if got := GetDataSource(alpha.info.Name); got != alpha {
		t.Fatalf("expected GetDataSource to return the registered source instance, got %+v", got)
	}
	if got := GetDataSource("missing_source"); got != nil {
		t.Fatalf("expected missing source lookup to return nil, got %+v", got)
	}
	gotNames := ListDataSources()
	wantNames := []string{alpha.info.Name, beta.info.Name}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("expected %d source names, got %d (%v)", len(wantNames), len(gotNames), gotNames)
	}
	for i, want := range wantNames {
		if gotNames[i] != want {
			t.Fatalf("expected sorted source names %v, got %v", wantNames, gotNames)
		}
	}
	status := ListDataSourceStatus()
	if len(status) != 2 || status[0].Name != alpha.info.Name || status[1].Name != beta.info.Name {
		t.Fatalf("expected sorted source status, got %+v", status)
	}
	if status[0].Health != "registered" || status[0].TimeFrame != "1d" || status[0].Table != alpha.info.Binding.Table {
		t.Fatalf("unexpected registered source status: %+v", status[0])
	}
}

func TestRegisterDataSourceStatusIncludesGovernanceMetadata(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_status_metadata_test")
	src.version = "v1-test"
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}

	status := ListDataSourceStatus()
	if len(status) != 1 {
		t.Fatalf("expected one status item, got %+v", status)
	}
	got := status[0]
	if got.Health != "registered" || got.Version != "v1-test" || got.RegisteredAtMS == 0 || got.RegisterSource == "" {
		t.Fatalf("expected registration governance metadata, got %+v", got)
	}
	if !strings.Contains(got.RegisterSource, "series_source_test.go") {
		t.Fatalf("expected register source to point at caller, got %q", got.RegisterSource)
	}
	if got.LastBackfill.State != "idle" || got.Subscription.State != "idle" {
		t.Fatalf("expected idle runtime states, got %+v", got)
	}
}

func TestRegisterDataSourceDoesNotSubscribeLive(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_definition_only_test")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	if src.subscribeCount != 0 {
		t.Fatalf("expected RegisterDataSource to be definition-only and not call SubscribeLive, got %d subscribe calls", src.subscribeCount)
	}
}

func TestActivateDataSourcesGroupsSelectedSubsBySource(t *testing.T) {
	resetDataSourcesForTest(t)
	alpha := newStubRegistrySource("macro_activation_alpha_test")
	beta := newStubRegistrySource("macro_activation_beta_test")
	if err := RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if err := RegisterDataSource(beta); err != nil {
		t.Fatalf("RegisterDataSource beta failed: %v", err)
	}
	subs := []*strat.DataSub{
		{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101}, TimeFrame: alpha.info.TimeFrame, WarmupNum: 1},
		{Source: beta.info.Name, ExSymbol: &orm.ExSymbol{ID: 202}, TimeFrame: beta.info.TimeFrame},
		{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101}, TimeFrame: alpha.info.TimeFrame, WarmupNum: 7},
		{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 303}, TimeFrame: alpha.info.TimeFrame},
	}
	activated, err := ActivateDataSources(context.Background(), subs, stubDataSink{})
	if err != nil {
		t.Fatalf("ActivateDataSources failed: %v", err)
	}
	if len(activated) != 3 {
		t.Fatalf("expected 3 activated unique subs, got %+v", activated)
	}
	if alpha.subscribeCount != 1 {
		t.Fatalf("expected alpha SubscribeLive once, got %d", alpha.subscribeCount)
	}
	if beta.subscribeCount != 1 {
		t.Fatalf("expected beta SubscribeLive once, got %d", beta.subscribeCount)
	}
	if len(alpha.subscribedSubs) != 1 || len(alpha.subscribedSubs[0]) != 2 {
		t.Fatalf("expected alpha activation group of 2 subs, got %+v", alpha.subscribedSubs)
	}
	if len(beta.subscribedSubs) != 1 || len(beta.subscribedSubs[0]) != 1 {
		t.Fatalf("expected beta activation group of 1 sub, got %+v", beta.subscribedSubs)
	}
	if alpha.subscribedSubs[0][0].ExSymbol.ID != 101 || alpha.subscribedSubs[0][1].ExSymbol.ID != 303 {
		t.Fatalf("expected alpha grouped ids [101 303], got %+v", alpha.subscribedSubs[0])
	}
	if alpha.subscribedSubs[0][0].WarmupNum != 7 {
		t.Fatalf("expected duplicate activation sub to preserve max warmup, got %+v", alpha.subscribedSubs[0][0])
	}
	if beta.subscribedSubs[0][0].ExSymbol.ID != 202 {
		t.Fatalf("expected beta grouped id 202, got %+v", beta.subscribedSubs[0][0])
	}
	if GetDataSource(alpha.info.Name) != alpha || GetDataSource(beta.info.Name) != beta {
		t.Fatalf("expected activation to reuse registered sources without mutating registry")
	}
}

func TestActivateDataSourcesFailsForUnknownSource(t *testing.T) {
	resetDataSourcesForTest(t)
	if _, err := ActivateDataSources(context.Background(), []*strat.DataSub{{
		Source:    "missing_source_activation_test",
		ExSymbol:  &orm.ExSymbol{ID: 101},
		TimeFrame: "1d",
	}}, stubDataSink{}); err == nil {
		t.Fatalf("expected ActivateDataSources to fail for an unknown source")
	}
}

func TestActivateDataSourcesRecordsSubscriptionFailure(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_subscribe_fail_test")
	src.subscribeErr = fmt.Errorf("subscribe failed")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}

	_, err := ActivateDataSources(context.Background(), []*strat.DataSub{{
		Source:    src.info.Name,
		ExSymbol:  &orm.ExSymbol{ID: 101},
		TimeFrame: src.info.TimeFrame,
	}}, stubDataSink{})
	if err == nil {
		t.Fatalf("expected subscription failure")
	}
	status := ListDataSourceStatus()
	if len(status) != 1 || status[0].Health != "error" || status[0].Subscription.State != "error" || !strings.Contains(status[0].Subscription.Error, "subscribe failed") {
		t.Fatalf("expected subscription error status, got %+v", status)
	}
}

func TestActivateDataSourcesRequiresSink(t *testing.T) {
	resetDataSourcesForTest(t)
	alpha := newStubRegistrySource("macro_activation_sink_test")
	if err := RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if _, err := ActivateDataSources(context.Background(), []*strat.DataSub{{
		Source:    alpha.info.Name,
		ExSymbol:  &orm.ExSymbol{ID: 101},
		TimeFrame: alpha.info.TimeFrame,
	}}, nil); err == nil {
		t.Fatalf("expected ActivateDataSources to require a sink")
	}
}

func TestSeriesRuntimeSyncLiveEnsuresThenActivatesNewSubs(t *testing.T) {
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
			},
		},
	}
	steps := make([]string, 0, 3)
	rt := NewSeriesRuntime(stubDataSink{})
	rt.EnsureFunc = func(ctx context.Context, plan *SeriesPlan) *errs.Error {
		steps = append(steps, fmt.Sprintf("ensure:%d:%d", plan.StartMS, plan.EndMS))
		return nil
	}
	rt.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink DataSink) ([]*strat.DataSub, error) {
		steps = append(steps, fmt.Sprintf("activate:%d", len(subs)))
		if sink == nil {
			t.Fatalf("expected runtime sink")
		}
		return subs, nil
	}

	plan, err := rt.SyncLive(context.Background(), []*strat.StratJob{job}, 200_000_000)
	if err != nil {
		t.Fatalf("SyncLive failed: %v", err)
	}
	if plan.StartMS != 27_200_000 || plan.EndMS != 200_000_000 {
		t.Fatalf("unexpected live plan range: %+v", plan)
	}
	want := []string{"ensure:27200000:200000000", "activate:1"}
	if strings.Join(steps, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected runtime steps want=%v got=%v", want, steps)
	}

	steps = steps[:0]
	if _, err := rt.SyncLive(context.Background(), []*strat.StratJob{job}, 200_000_000); err != nil {
		t.Fatalf("second SyncLive failed: %v", err)
	}
	want = []string{"ensure:27200000:200000000"}
	if strings.Join(steps, ",") != strings.Join(want, ",") {
		t.Fatalf("expected already-active sub to skip activation, got %v", steps)
	}
}

func TestSeriesRuntimePartialActivationKeepsFailedSubsRetryable(t *testing.T) {
	subs := []*strat.DataSub{
		{Source: "alpha", ExSymbol: &orm.ExSymbol{ID: 9}, TimeFrame: "1d"},
		{Source: "beta", ExSymbol: &orm.ExSymbol{ID: 9}, TimeFrame: "1d"},
	}
	rt := NewSeriesRuntime(stubDataSink{})
	rt.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink DataSink) ([]*strat.DataSub, error) {
		return subs[:1], fmt.Errorf("activate data source \"beta\": bad source")
	}

	err := rt.ActivateNew(context.Background(), subs)
	if err == nil || !strings.Contains(err.Error(), "phase=activate") {
		t.Fatalf("expected activate phase error, got %v", err)
	}
	if !rt.Active(strat.DataSubKey("alpha", 9, "1d")) {
		t.Fatalf("expected successful alpha activation to be remembered")
	}
	if rt.Active(strat.DataSubKey("beta", 9, "1d")) {
		t.Fatalf("expected failed beta activation to remain retryable")
	}
}

func TestSeriesRuntimeReactivatesExpandedFields(t *testing.T) {
	rt := NewSeriesRuntime(stubDataSink{})
	var calls [][]string
	rt.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink DataSink) ([]*strat.DataSub, error) {
		calls = append(calls, append([]string(nil), subs[0].Fields...))
		return subs, nil
	}
	exs := &orm.ExSymbol{ID: 9}
	if err := rt.ActivateNew(context.Background(), []*strat.DataSub{{Source: "macro", ExSymbol: exs, TimeFrame: "1d", Fields: []string{"value"}}}); err != nil {
		t.Fatalf("first activation failed: %v", err)
	}
	if err := rt.ActivateNew(context.Background(), []*strat.DataSub{{Source: "macro", ExSymbol: exs, TimeFrame: "1d", Fields: []string{"value"}}}); err != nil {
		t.Fatalf("duplicate activation failed: %v", err)
	}
	if err := rt.ActivateNew(context.Background(), []*strat.DataSub{{Source: "macro", ExSymbol: exs, TimeFrame: "1d", Fields: []string{"value", "revision"}}}); err != nil {
		t.Fatalf("expanded activation failed: %v", err)
	}
	if len(calls) != 2 || !reflect.DeepEqual(calls[1], []string{"value", "revision"}) {
		t.Fatalf("expected one initial and one expanded activation, got %v", calls)
	}
}

func TestCollectRuntimeDataSubsFiltersKlinesAndDeduplicates(t *testing.T) {
	jobs := []*strat.StratJob{
		{
			Strat: &strat.TradeStrat{
				OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
					return []*strat.PairSub{{Pair: "_cur_", TimeFrame: "5m", WarmupNum: 20}}
				},
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 5}}
				},
			},
			Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		},
		{
			Strat: &strat.TradeStrat{
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 9}}
				},
			},
			Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		},
	}

	subs, err := CollectRuntimeDataSubs(jobs)
	if err != nil {
		t.Fatalf("CollectRuntimeDataSubs failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 deduped non-kline sub, got %d (%+v)", len(subs), subs)
	}
	if subs[0].Source != "macro" || subs[0].ExSymbol == nil || subs[0].ExSymbol.ID != 7 || subs[0].TimeFrame != "1d" {
		t.Fatalf("unexpected deduped sub: %+v", subs[0])
	}
	if subs[0].WarmupNum != 9 {
		t.Fatalf("expected dedupe to preserve max warmup depth, got %+v", subs[0])
	}
}

func TestMergeDataSubUnionsFieldsWithoutMutatingInput(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7}
	first := &strat.DataSub{Source: "macro", ExSymbol: exs, TimeFrame: "1d", WarmupNum: 2, Fields: []string{"close", "signal_a"}}
	second := &strat.DataSub{Source: "macro", ExSymbol: exs, TimeFrame: "1d", WarmupNum: 5, Fields: []string{"volume", "signal_b", "close"}}
	seen := make(map[string]*strat.DataSub)
	mergeDataSub(seen, first)
	mergeDataSub(seen, second)
	got := seen[strat.DataSubKey("macro", 7, "1d")]
	want := []string{"close", "signal_a", "volume", "signal_b"}
	if got == first || got.WarmupNum != 5 || !reflect.DeepEqual(got.Fields, want) {
		t.Fatalf("unexpected merged subscription: %+v", got)
	}
	if !reflect.DeepEqual(first.Fields, []string{"close", "signal_a"}) {
		t.Fatalf("merge mutated input: %v", first.Fields)
	}
}

func TestCollectRuntimeDataSubsRejectsMalformedSubs(t *testing.T) {
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: &orm.ExSymbol{ID: 0}, TimeFrame: "1d"}}
			},
		},
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}

	_, err := CollectRuntimeDataSubs(jobs)
	if err == nil {
		t.Fatalf("expected malformed sub collection to fail")
	}
	if !strings.Contains(err.Error(), "bootstrap collect") || !strings.Contains(err.Error(), "exsymbol") {
		t.Fatalf("expected collect-phase exsymbol error, got %v", err)
	}
}

func TestNewThirdPartySeriesBootstrapPlansWarmupOnce(t *testing.T) {
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
			},
		},
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}

	plan, err := NewThirdPartySeriesBootstrap(jobs, 200_000_000, 300_000_000)
	if err != nil {
		t.Fatalf("NewThirdPartySeriesBootstrap failed: %v", err)
	}
	if !plan.HasSubs() || !plan.HasHistoryRange() {
		t.Fatalf("expected bootstrap plan with history range, got %+v", plan)
	}
	wantStart := int64(200_000_000 - 2*86_400_000)
	if plan.StartMS != wantStart || plan.EndMS != 300_000_000 {
		t.Fatalf("unexpected bootstrap range start=%d end=%d", plan.StartMS, plan.EndMS)
	}
	if len(plan.Subs) != 1 || plan.Subs[0].Source != "macro" || plan.Subs[0].ExSymbol.ID != 7 {
		t.Fatalf("unexpected bootstrap subs: %+v", plan.Subs)
	}
}

func TestThirdPartyWarmupStartHonorsExplicitZeroWarmup(t *testing.T) {
	subs := []*strat.DataSub{{
		Source:    "macro",
		ExSymbol:  &orm.ExSymbol{ID: 7},
		TimeFrame: "1d",
		WarmupNum: 0,
	}}

	startMS, err := ThirdPartyWarmupStart(subs, 200_000_000)
	if err != nil {
		t.Fatalf("ThirdPartyWarmupStart failed: %v", err)
	}
	if startMS != 200_000_000 {
		t.Fatalf("expected zero warmup to keep anchor unchanged, got %d", startMS)
	}
}

func TestThirdPartyWarmupStartRejectsNegativeWarmup(t *testing.T) {
	subs := []*strat.DataSub{{
		Source:    "macro",
		ExSymbol:  &orm.ExSymbol{ID: 7},
		TimeFrame: "1d",
		WarmupNum: -1,
	}}

	_, err := ThirdPartyWarmupStart(subs, 200_000_000)
	if err == nil || !strings.Contains(err.Error(), "warmup must not be negative") {
		t.Fatalf("expected negative warmup validation error, got %v", err)
	}
}

func TestEnsureThirdPartySeriesRangeDeduplicatesAndUsesCoverage(t *testing.T) {
	resetDataSourcesForTest(t)
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	now := time.Now().UnixNano()
	src := &stubSeriesSource{
		info: &orm.SeriesInfo{
			Name:      fmt.Sprintf("macro_runtime_%d", now),
			TimeFrame: "1d",
			Binding: orm.SeriesBinding{
				Table:      fmt.Sprintf("runtime_series_%d", now),
				TimeColumn: "ts",
				EndColumn:  "end_ms",
				SIDColumn:  "sid",
				Fields:     []orm.SeriesField{{Name: "value", Type: "float", Role: "value"}},
			},
		},
		rows: []*orm.DataRecord{{TimeMS: 1_700_000_000_000, EndMS: 1_700_086_400_000, Values: map[string]any{"value": 1.0}}},
	}
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	jobs := []*strat.StratJob{
		{Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}}, Symbol: &orm.ExSymbol{ID: int32(now % 1_000_000), Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}},
		{Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}}, Symbol: &orm.ExSymbol{ID: int32(now % 1_000_000), Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}},
	}

	ctx := context.Background()
	startMS := int64(1_700_000_000_000)
	endMS := int64(1_700_086_400_000)
	subs, err := EnsureThirdPartySeriesRange(ctx, nil, jobs, startMS, endMS)
	if err != nil {
		t.Fatalf("EnsureThirdPartySeriesRange failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 ensured sub, got %d", len(subs))
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected one deduped fetch, got %d", src.fetchCount)
	}
	if _, err := EnsureThirdPartySeriesRange(ctx, nil, jobs, startMS, endMS); err != nil {
		t.Fatalf("EnsureThirdPartySeriesRange second pass failed: %v", err)
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected coverage metadata to skip duplicate bootstrap fetch, got %d", src.fetchCount)
	}
}

func TestEnsureThirdPartySeriesRangeUnknownSource(t *testing.T) {
	resetDataSourcesForTest(t)
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: "missing_source", ExSymbol: s.Symbol, TimeFrame: "1d"}}
		}},
		Symbol: &orm.ExSymbol{ID: 42, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}

	_, err := EnsureThirdPartySeriesRange(context.Background(), &stubSeriesRepo{}, jobs, 100, 200)
	if err == nil {
		t.Fatalf("expected unknown source bootstrap to fail")
	}
	msg := err.Short()
	if !strings.Contains(msg, "source=missing_source") || !strings.Contains(msg, "sid=42") || !strings.Contains(msg, "tf=1d") || !strings.Contains(msg, "phase=ensure") {
		t.Fatalf("expected startup-visible source/sid/tf/phase error, got %q", msg)
	}
}

func TestEnsureThirdPartySeriesRangePropagatesFetchFailure(t *testing.T) {
	resetDataSourcesForTest(t)
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	src := newStubRegistrySource("macro_fetch_fail_test")
	src.fetchErr = fmt.Errorf("fetch timeout")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}},
		Symbol: &orm.ExSymbol{ID: 77, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}
	repo := &stubSeriesRepo{}

	_, err := EnsureThirdPartySeriesRange(context.Background(), repo, jobs, 100, 200)
	if err == nil {
		t.Fatalf("expected fetch failure bootstrap to fail")
	}
	msg := err.Short()
	if !strings.Contains(msg, "source="+src.info.Name) || !strings.Contains(msg, "sid=77") || !strings.Contains(msg, "tf=1d") || !strings.Contains(msg, "phase=ensure") || !strings.Contains(msg, "fetch timeout") {
		t.Fatalf("expected startup-visible fetch failure, got %q", msg)
	}
	if repo.ensureTableCalls != 0 || repo.insertCalls != 0 || repo.updateCalls != 0 {
		t.Fatalf("expected fetch failure before store writes, got ensure=%d insert=%d update=%d",
			repo.ensureTableCalls, repo.insertCalls, repo.updateCalls)
	}
	status := ListDataSourceStatus()
	if len(status) != 1 || status[0].Health != "error" || status[0].LastBackfill.State != "error" || !strings.Contains(status[0].LastBackfill.Error, "fetch timeout") {
		t.Fatalf("expected backfill error status, got %+v", status)
	}
}

func TestEnsureRuntimeSeriesRangeUsesInjectedRepositoryCoverage(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_injected_repo_test")
	src.rows = []*orm.DataRecord{{TimeMS: 300, EndMS: 400, Values: map[string]any{"value": 1.0}}}
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	repo := &stubSeriesRepo{missing: []orm.MSRange{{Start: 300, Stop: 400}}}
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}},
		Symbol: &orm.ExSymbol{ID: 55, Exchange: "custom", Market: "macro", Symbol: "CPI_US"},
	}}

	subs, err := EnsureRuntimeSeriesRange(context.Background(), repo, jobs, 100, 500)
	if err != nil {
		t.Fatalf("EnsureRuntimeSeriesRange failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected one ensured sub, got %+v", subs)
	}
	if repo.missingCalls != 1 || repo.insertCalls != 1 || repo.updateCalls != 1 {
		t.Fatalf("expected injected repo to drive missing/write/coverage, got missing=%d insert=%d update=%d",
			repo.missingCalls, repo.insertCalls, repo.updateCalls)
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected one fetch for injected missing range, got %d", src.fetchCount)
	}
}

func TestEnsureRuntimeSeriesRangeMarksEmptyFetchAsHole(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_empty_fetch_test")
	src.rows = nil
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	repo := &stubSeriesRepo{missing: []orm.MSRange{{Start: 300, Stop: 400}}}
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}},
		Symbol: &orm.ExSymbol{ID: 56, Exchange: "custom", Market: "macro", Symbol: "PMI_CN"},
	}}

	_, err := EnsureRuntimeSeriesRange(context.Background(), repo, jobs, 100, 500)
	if err != nil {
		t.Fatalf("EnsureRuntimeSeriesRange failed: %v", err)
	}
	if repo.insertCalls != 0 {
		t.Fatalf("expected empty fetch to skip insert, got %d insert calls", repo.insertCalls)
	}
	if repo.updateCalls != 1 || repo.coverageStart != 300 || repo.coverageEnd != 400 || len(repo.coverageRows) != 0 {
		t.Fatalf("expected empty fetch to mark a coverage hole, got updates=%d start=%d end=%d rows=%+v",
			repo.updateCalls, repo.coverageStart, repo.coverageEnd, repo.coverageRows)
	}
}

func TestEnsureThirdPartySeriesRangeRejectsSourceMetadataMismatch(t *testing.T) {
	resetDataSourcesForTest(t)
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	src := newStubRegistrySource("macro_tf_mismatch_test")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: "4h"}}
		}},
		Symbol: &orm.ExSymbol{ID: 88, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}

	_, err := EnsureThirdPartySeriesRange(context.Background(), &stubSeriesRepo{}, jobs, 100, 200)
	if err == nil {
		t.Fatalf("expected timeframe mismatch bootstrap to fail")
	}
	msg := err.Short()
	if !strings.Contains(msg, "source="+src.info.Name) || !strings.Contains(msg, "sid=88") || !strings.Contains(msg, "tf=4h") || !strings.Contains(msg, "phase=ensure") || !strings.Contains(msg, "match source timeframe") {
		t.Fatalf("expected startup-visible timeframe mismatch, got %q", msg)
	}
}

func TestEnsureThirdPartySeriesRangePropagatesRepoFailure(t *testing.T) {
	resetDataSourcesForTest(t)
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	src := newStubRegistrySource("macro_repo_fail_test")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	jobs := []*strat.StratJob{{
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: src.info.Name, ExSymbol: s.Symbol, TimeFrame: src.info.TimeFrame}}
		}},
		Symbol: &orm.ExSymbol{ID: 99, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}}
	src.rows = []*orm.DataRecord{{TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.0}}}
	repo := &stubSeriesRepo{insertErr: errs.NewMsg(core.ErrDbExecFail, "repo boom")}

	_, err := EnsureThirdPartySeriesRange(context.Background(), repo, jobs, 100, 200)
	if err == nil {
		t.Fatalf("expected repo failure bootstrap to fail")
	}
	msg := err.Short()
	if !strings.Contains(msg, "source="+src.info.Name) || !strings.Contains(msg, "sid=99") || !strings.Contains(msg, "tf=1d") || !strings.Contains(msg, "phase=ensure") || !strings.Contains(msg, "repo boom") {
		t.Fatalf("expected startup-visible repo failure, got %q", msg)
	}
}

func TestRegisteredSourceLookupsStayActivationFree(t *testing.T) {
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("macro_lookup_definition_test")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}

	if got := GetDataSource(src.info.Name); got != src {
		t.Fatalf("expected GetDataSource to return the registered source instance, got %+v", got)
	}
	gotNames := ListDataSources()
	if len(gotNames) != 1 || gotNames[0] != src.info.Name {
		t.Fatalf("expected ListDataSources to expose only the registered definition, got %v", gotNames)
	}
	if src.subscribeCount != 0 {
		t.Fatalf("expected registry reads to remain activation-free, got %d subscribe calls", src.subscribeCount)
	}
	if src.fetchCount != 0 {
		t.Fatalf("expected registry reads to remain activation-free, got %d fetch calls", src.fetchCount)
	}
}

func TestEnsureSeriesRangeTimescale(t *testing.T) {
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	runEnsureSeriesRangeTest(t, "timescale")
}

func TestEnsureSeriesRangeQuestDB(t *testing.T) {
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.yml"))
	runEnsureSeriesRangeTest(t, "quest")
}

func mustFindSeriesSourceConfig(t *testing.T, name string) string {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "..", "data", name),
		filepath.Join("..", name),
		name,
	}
	for _, candidate := range candidates {
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	t.Fatalf("series source test config %q not found in candidates: %v", name, candidates)
	return ""
}

func runEnsureSeriesRangeTest(t *testing.T, backend string) {
	t.Helper()
	now := time.Now().UnixNano()
	info := &orm.SeriesInfo{
		Name:      fmt.Sprintf("macro_%s_%d", backend, now),
		TimeFrame: "1d",
		Binding: orm.SeriesBinding{
			Table:      fmt.Sprintf("series_source_%s_%d", backend, now),
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []orm.SeriesField{
				{Name: "value", Type: "float", Role: "value"},
				{Name: "label", Type: "string", Role: "custom"},
			},
		},
	}
	startMS := int64(1_700_000_000_000)
	rows := []*orm.DataRecord{
		{
			TimeMS: startMS,
			EndMS:  startMS + 86_400_000,
			Values: map[string]any{"value": 12.5, "label": "fred"},
		},
		{
			TimeMS: startMS + 86_400_000,
			EndMS:  startMS + 172_800_000,
			Values: map[string]any{"value": 13.5, "label": "wind"},
		},
	}
	src := &stubSeriesSource{info: info, rows: rows}
	sub := &strat.DataSub{
		Source:    info.Name,
		ExSymbol:  &orm.ExSymbol{ID: int32(now % 1_000_000)},
		TimeFrame: info.TimeFrame,
	}
	ctx := context.Background()
	if err := EnsureSeriesRange(ctx, src, sub, startMS, startMS+172_800_000); err != nil {
		t.Fatalf("EnsureSeriesRange failed: %v", err)
	}
	got, err := orm.DefaultSeriesRepo().QuerySeriesRange(ctx, info, sub.ExSymbol.ID, startMS, startMS+172_800_000, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected initial fetch count to be 1, got %d", src.fetchCount)
	}
	if err := EnsureSeriesRange(ctx, src, sub, startMS, startMS+172_800_000); err != nil {
		t.Fatalf("EnsureSeriesRange second pass failed: %v", err)
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected coverage metadata to skip duplicate fetch, got %d fetches", src.fetchCount)
	}
}

func initSeriesSourceTestApp(t *testing.T, cfgPath string) {
	t.Helper()
	config.Loaded = false
	config.DataDir = ""
	config.Args = nil
	var args config.CmdArgs
	args.NoDefault = true
	args.Configs = []string{cfgPath}
	if err := config.LoadConfig(&args); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if err := exg.Setup(); err != nil {
		t.Fatalf("exg.Setup failed: %v", err)
	}
	if err := orm.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
}
