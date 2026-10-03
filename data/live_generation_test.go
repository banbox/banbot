package data

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
)

type generationSeriesSink struct {
	mu       sync.Mutex
	rows     []*orm.DataSeries
	consume  func(*orm.DataSeries)
	revision func(*orm.Subscription, *orm.DataSeries) (LiveSeriesRevision, error)
}

func (s *generationSeriesSink) SeriesRevision(sub *orm.Subscription, row *orm.DataSeries) (LiveSeriesRevision, error) {
	if s.revision != nil {
		return s.revision(sub, row)
	}
	revision := uint64(1)
	if value, ok := row.Values["revision"]; ok {
		revision = value.(uint64)
	}
	return LiveSeriesRevision{Revision: revision, EventTime: row.EndMS, SourceVersion: "v1"}, nil
}

func TestLiveKlineGenerationHydratesTypedFieldsBeforeRevisionWithOneRead(t *testing.T) {
	h, p, _, deps, symbol := generationDataFixture(t, "1m")
	fields := []string{"close", "integer", "label", "flag", "nullable"}
	plan, err := deps.Catalog.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{{Subscription: Subscription{Source: "kline", TimeFrame: "1m", ExSymbol: symbol, Fields: fields}, Consumer: "factor", Required: true}}, SubscriptionPlanOptions{AnchorMS: 1_000_000, EndMS: 1_000_000, PrefetchRows: 20})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"integer": int64(9007199254740993), "label": "typed", "flag": true, "nullable": nil}
	checkFields := func(row *orm.DataSeries) {
		t.Helper()
		for field, value := range want {
			got, exists := row.Values[field]
			if !exists || !reflect.DeepEqual(got, value) {
				t.Fatalf("typed field %s lost before admission/dispatch: %#v", field, row.Values)
			}
		}
	}
	revisions := 0
	sink := &generationSeriesSink{consume: checkFields, revision: func(_ *orm.Subscription, row *orm.DataSeries) (LiveSeriesRevision, error) {
		checkFields(row)
		revisions++
		return LiveSeriesRevision{Revision: 1, EventTime: row.EndMS, SourceVersion: "v1"}, nil
	}}
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	holder, _ := p.getHolder(symbol.Symbol)
	feeder := holder.(*generationWarmupFeeder)
	reads := 0
	feeder.readKlineFields = func(_ *orm.ExSymbol, tf string, projected []string, start, end int64) ([]*orm.DataSeries, *errs.Error) {
		reads++
		if tf != "1m" || start != 60_000 || end != 120_000 {
			t.Fatalf("unexpected projection range: %s %d..%d %v", tf, start, end, projected)
		}
		return []*orm.DataSeries{{TimeMS: 60_000, Values: want}}, nil
	}
	row := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: 60_000, EndMS: 120_000, Closed: true, Values: map[string]any{"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0}}
	p.RememberSeriesRevision(row, LiveSeriesRevision{Revision: 1, EventTime: 120_000, SourceVersion: "v1", Warmup: true})
	if err := h.Emit(0, row); err != nil {
		t.Fatal(err)
	}
	if reads != 1 || revisions != 1 || len(sink.rows) != 1 {
		t.Fatalf("hydration duplicated reads/admission: reads=%d revisions=%d callbacks=%d", reads, revisions, len(sink.rows))
	}
	if len(row.Values) != 5 {
		t.Fatal("storage hydration mutated caller's raw input")
	}
}

func (*generationSeriesSink) Emit(*orm.Subscription, []*orm.DataRecord) error {
	return errors.New("typed ingress narrowed to storage rows")
}
func (s *generationSeriesSink) WarmupSeries(sub *orm.Subscription, rows []*orm.DataSeries) error {
	return s.EmitSeries(sub, rows)
}
func (s *generationSeriesSink) EmitSeries(_ *orm.Subscription, rows []*orm.DataSeries) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range rows {
		s.rows = append(s.rows, row)
		if s.consume != nil {
			s.consume(row)
		}
	}
	return nil
}

func generationDataFixture(t *testing.T, timeframes ...string) (*FactorLiveGenerationHarness, *LiveProvider, *SubscriptionPlan, *RuntimeDeps, *orm.ExSymbol) {
	t.Helper()
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.ExgName, state.Market = "fixture", "linear"
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC", Exchange: "fixture", Market: "linear"}
	symbols := orm.NewSymbolStateWithIdentity("fixture", "linear")
	if err := symbols.CacheExSymbolChecked(symbol); err != nil {
		t.Fatal(err)
	}
	deps := &RuntimeDeps{Core: state, Clock: btime.NewClockState(true, time.UTC), ExchangeName: "fixture", MarketType: "linear", Symbols: symbols, Strategies: strat.NewState(), Market: com.NewMarketState("fixture"), Catalog: NewDataSourceCatalog()}
	deps.Clock.SetTimeMS(1_000_000)
	var requests []SubscriptionRequest
	for _, timeframe := range timeframes {
		requests = append(requests, SubscriptionRequest{Subscription: Subscription{Source: "kline", TimeFrame: timeframe, ExSymbol: symbol, Fields: []string{"close"}}, Consumer: "factor", Required: true})
	}
	plan, planErr := deps.Catalog.CompileSubscriptionPlan(context.Background(), requests, SubscriptionPlanOptions{AnchorMS: 1_000_000, EndMS: 1_000_000, PrefetchRows: 20})
	if planErr != nil {
		t.Fatal(planErr)
	}
	harness, provider := NewFactorLiveGenerationHarness(deps)
	t.Cleanup(func() { _ = provider.Stop(); provider.Join() })
	return harness, provider, plan, deps, symbol
}

func TestLiveKlineGenerationQueuesFullSeriesMetadata(t *testing.T) {
	h, p, plan, _, symbol := generationDataFixture(t, "1m")
	sink := &generationSeriesSink{}
	installation, err := p.PrepareSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	adj := &orm.AdjInfo{ExSymbol: symbol, Factor: .5, CumFactor: .25, StartMS: 60_000, StopMS: 600_000}
	row := &orm.DataSeries{Source: "", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: 60_000, EndMS: 120_000, Closed: true, Adj: adj, Values: map[string]any{"close": 100.0, "integer": int64(9007199254740993), "nullable": nil}}
	if err := h.Emit(0, row); err != nil {
		t.Fatal(err)
	}
	adj.Factor = 99
	row.Values["integer"] = int64(1)
	if len(sink.rows) != 0 {
		t.Fatal("prepared generation admitted live callbacks")
	}
	if err := installation.Activate(); err != nil {
		t.Fatal(err)
	}
	got := sink.rows[0]
	if got.Source != "kline" || got.Adj == nil || got.Adj.Factor != .5 || got.Values["integer"] != int64(9007199254740993) {
		t.Fatalf("queued metadata mutated/narrowed: %#v", got)
	}
	if value, exists := got.Values["nullable"]; !exists || value != nil {
		t.Fatal("explicit NULL lost")
	}
}

func TestLiveKlineGenerationRealIngressRejectsUnprovenCoarseCorrection(t *testing.T) {
	const base int64 = 1_800_000_000_000
	h, p, plan, _, symbol := generationDataFixture(t, "1m", "3m")
	sink := &generationSeriesSink{}
	sink.consume = func(row *orm.DataSeries) {
		revision, _ := sink.SeriesRevision(nil, row)
		p.RememberSeriesRevision(row, revision)
	}
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	makeRow := func(at int64, volume float64) *orm.DataSeries {
		return &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: at, EndMS: at + 60_000, Closed: true, Values: map[string]any{"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": volume, "revision": uint64(1), "integer": int64(9007199254740993), "nullable": nil}}
	}
	for _, at := range []int64{base + 180_000, base + 240_000, base + 300_000} {
		if err := h.Emit(0, makeRow(at, 1)); err != nil {
			t.Fatal(err)
		}
	}
	// Keep a newer unfinished bucket while correcting a non-last older child.
	if err := h.Emit(0, makeRow(base+360_000, 1)); err != nil {
		t.Fatal(err)
	}
	holder, _ := p.getHolder("BTC")
	states := holder.getStates()
	previousWait, previousLatest, nextMS := states[1].WaitBar, states[1].Latest, states[1].NextMS
	before := len(sink.rows)
	corrected := makeRow(base+240_000, 5)
	corrected.Values["revision"] = uint64(2)
	if err := h.Emit(0, corrected); err == nil {
		t.Fatal("coarse correction accepted without derived revision evidence")
	}
	if len(sink.rows) != before || states[1].WaitBar != previousWait || states[1].Latest != previousLatest || states[1].NextMS != nextMS {
		t.Fatal("rejected correction changed current partial bucket/cursor")
	}
	feeder := holder.(*generationWarmupFeeder)
	if feeder.tfBars["1m"][1].Values["volume"] != 1.0 {
		t.Fatal("rejected correction mutated source cache")
	}
}

func TestLiveKlineGenerationRealHandlerFailureStopsInstallation(t *testing.T) {
	_, p, plan, _, symbol := generationDataFixture(t, "1m")
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, &generationSeriesSink{})
	if err != nil {
		t.Fatal(err)
	}
	makeOnSeriesMsg(p)(&SeriesMsg{ExgName: "fixture", Market: "linear", Pair: symbol.Symbol, NotifySeries: NotifySeries{TFSecs: 120, Interval: 120, Rows: []*orm.DataSeries{{TimeMS: 60_000}}}})
	p.joinHandlers()
	select {
	case err := <-installation.Errors():
		if err == nil {
			t.Fatal("missing invalid-period failure")
		}
	default:
		t.Fatal("handler failure not propagated")
	}
	if !p.handlerStop {
		t.Fatal("failed provider kept admitting callbacks")
	}
	installation.Stop()
	if installation.Join() == nil {
		t.Fatal("Join lost real feeder failure")
	}
}

func TestLiveKlineGenerationRealIngressSerializesPairAndJoin(t *testing.T) {
	_, p, plan, _, symbol := generationDataFixture(t, "1m")
	entered, release := make(chan struct{}), make(chan struct{})
	sink := &generationSeriesSink{consume: func(row *orm.DataSeries) {
		if row.TimeMS == 60_000 {
			close(entered)
			<-release
		}
	}}
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	send := func(at int64) {
		makeOnSeriesMsg(p)(&SeriesMsg{ExgName: "fixture", Market: "linear", Pair: symbol.Symbol, NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{{Source: "kline", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: at, EndMS: at + 60_000, Values: map[string]any{"close": 100.0}}}}})
	}
	send(60_000)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real handler did not enter")
	}
	second := make(chan struct{})
	go func() { send(120_000); close(second) }()
	select {
	case <-second:
		t.Fatal("same pair dispatch bypassed active feeder")
	case <-time.After(30 * time.Millisecond):
	}
	installation.Stop()
	joined := make(chan struct{})
	go func() { _ = installation.Join(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("Join skipped accepted physical callback")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not complete")
	}
	<-second
}

func TestLiveKlineGenerationCanceledDialUsesSubscriptionContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	state, stateErr := core.NewState(context.Background())
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	watcher, dialErr := NewSeriesWatcherWithRuntimeDeps(&RuntimeDeps{Core: state, SubscriptionContext: ctx, IsolatedSubscriptions: true}, listener.Addr().String())
	if watcher != nil {
		watcher.ClientIO.Stop()
		watcher.ClientIO.Join()
		t.Fatal("canceled preparation opened a socket")
	}
	if dialErr == nil || !errors.Is(dialErr, context.Canceled) {
		t.Fatalf("dial ignored preparation cancellation: %v", dialErr)
	}
}

func TestLiveKlineGenerationEarlyPreparationFailureClosesSocket(t *testing.T) {
	_, p, plan, _, _ := generationDataFixture(t, "1m")
	server, client := net.Pipe()
	defer server.Close()
	p.SeriesWatcher = &SeriesWatcher{ClientIO: &utils.ClientIO{BanConn: utils.BanConn{Conn: client, Ready: true}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if installation, err := p.PrepareSubscriptionPlan(ctx, plan, &generationSeriesSink{}); err == nil || installation != nil {
		t.Fatal("canceled candidate was prepared")
	}
	if !p.SeriesWatcher.ClientIO.IsClosed() || !p.handlerStop {
		t.Fatal("early candidate preparation leaked physical socket")
	}
}

func TestLiveKlineGenerationIsolatesMarketAndLifecycle(t *testing.T) {
	_, _, plan, deps, _ := generationDataFixture(t, "1m")
	isolated, err := isolateSubscriptionDeps(deps, context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	watcher := &SeriesWatcher{deps: isolated}
	before := deps.Market.PairCopied.GetPairCopieds()
	watcher.setPairMS("BTC", 180_000, 60_000)
	watcher.addTfPairHits("1m", "BTC", 1)
	if !reflect.DeepEqual(before, deps.Market.PairCopied.GetPairCopieds()) || len(deps.Core.DrainTfPairHits()) != 0 {
		t.Fatal("prepared ingress mutated published runtime progress")
	}
	recorder := &providerLifecycleRecorder{}
	isolated.Callbacks = recorder
	p := &LiveProvider{deps: isolated}
	p.registerLifecycle()
	if recorder.close != nil || recorder.wait != nil {
		t.Fatal("retired generations retained by runtime lifecycle hooks")
	}
}

func TestLiveKlineGenerationRejectsDuplicateLogicalRowsBeforeCacheMutation(t *testing.T) {
	h, p, plan, _, symbol := generationDataFixture(t, "1m")
	sink := &generationSeriesSink{}
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	row := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000, Closed: true, Values: map[string]any{"close": 100.0}}
	if err := h.Emit(0, row); err != nil {
		t.Fatal(err)
	}
	before := h.Cached(0, "BTC", "1m", 60000)
	p.RememberSeriesRevision(row, LiveSeriesRevision{Revision: 1, EventTime: row.EndMS, SourceVersion: "v1"})
	newer, older := *row, *row
	newer.Values = map[string]any{"close": 105.0, "revision": uint64(3)}
	older.Values = map[string]any{"close": 104.0, "revision": uint64(2)}
	makeOnSeriesMsg(p)(&SeriesMsg{ExgName: "fixture", Market: "linear", Pair: "BTC", NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{&newer, &older}}})
	p.joinHandlers()
	if err := installation.Join(); err == nil || !strings.Contains(err.Error(), "duplicate logical observations") || h.Cached(0, "BTC", "1m", 60000).Values["close"] != before.Values["close"] {
		t.Fatal("same-key batch changed cached latest revision")
	}
}

func TestLiveKlineGenerationEqualWarmCoarseOverlapDoesNotFreeze(t *testing.T) {
	const base int64 = 1_800_000_000_000
	h, p, plan, _, symbol := generationDataFixture(t, "1m", "3m")
	sink := &generationSeriesSink{}
	installation, err := p.InstallSubscriptionPlan(context.Background(), plan, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	row := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbol, TimeFrame: "1m", TimeMS: base, EndMS: base + 60000, Closed: true, Values: map[string]any{"close": 100.0, "revision": uint64(1)}}
	p.RememberSeriesRevision(row, LiveSeriesRevision{Revision: 1, EventTime: row.EndMS, SourceVersion: "v1", Warmup: true})
	if err := h.Emit(0, row); err != nil {
		t.Fatal(err)
	}
	if p.handlerStop || len(sink.rows) != 0 {
		t.Fatal("equal warmed coarse observation treated as a correction")
	}
}
