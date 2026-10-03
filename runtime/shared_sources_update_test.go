package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type replacementHandle struct {
	cancel          context.CancelFunc
	done            chan struct{}
	stopped, joined atomic.Bool
	joinRelease     <-chan struct{}
}

func (h *replacementHandle) Stop() { h.stopped.Store(true); h.cancel() }
func (h *replacementHandle) Join() error {
	<-h.done
	if h.joinRelease != nil {
		<-h.joinRelease
	}
	h.joined.Store(true)
	return nil
}

type replacementProducer struct {
	ctx    context.Context
	handle *replacementHandle
	sink   data.DataSink
	subs   []*orm.Subscription
}

func (p *replacementProducer) emit(sid int32, at int64, closed bool) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	for _, sub := range p.subs {
		if sub.ExSymbol.ID == sid {
			return p.sink.Emit(sub, []*orm.DataRecord{{Sid: sid, TimeMS: at - 1, EndMS: at, Closed: closed, Values: map[string]any{"value": float64(sid), "price": 100.0, "custom": int64(9007199254740993), "null": nil}}})
		}
	}
	return errors.New("test: missing source SID")
}

type replacementSource struct {
	name        string
	mu          sync.Mutex
	producers   []*replacementProducer
	startupErr  error
	history     func(context.Context, *orm.Subscription, int64, int64) ([]*orm.DataRecord, error)
	prepare     func(*replacementProducer) error
	joinRelease <-chan struct{}
	version     atomic.Int32
	versionHook func()
}

func (s *replacementSource) Version() string {
	if s.versionHook != nil {
		s.versionHook()
	}
	return fmt.Sprintf("v%d", s.version.Load()+1)
}

func (s *replacementSource) Info() *orm.SeriesInfo {
	return orm.NewSeriesInfo(s.name, "event", []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "price", Type: "float"}, {Name: "rate", Type: "float"}, {Name: "mark", Type: "float"}, {Name: "account_amount", Type: "float"}, {Name: "settlement_id", Type: "string"}})
}
func (s *replacementSource) WarmupStart(_ context.Context, sub *orm.Subscription, anchor int64) (int64, error) {
	return anchor - int64(sub.WarmupNum)*10, nil
}
func (s *replacementSource) FetchHistory(ctx context.Context, sub *orm.Subscription, from, to int64) ([]*orm.DataRecord, error) {
	if s.history != nil {
		return s.history(ctx, sub, from, to)
	}
	return nil, nil
}
func (*replacementSource) SubscribeLive(context.Context, []*orm.Subscription, data.DataSink) error {
	panic("owned managed source required")
}
func (s *replacementSource) SubscribeManaged(ctx context.Context, subs []*orm.Subscription, sink data.DataSink) (data.LiveSourceSubscription, error) {
	ctx, cancel := context.WithCancel(ctx)
	h := &replacementHandle{cancel: cancel, done: make(chan struct{}), joinRelease: s.joinRelease}
	p := &replacementProducer{ctx: ctx, handle: h, sink: sink, subs: subs}
	s.mu.Lock()
	s.producers = append(s.producers, p)
	s.mu.Unlock()
	go func() { <-ctx.Done(); close(h.done) }()
	if s.prepare != nil {
		if err := s.prepare(p); err != nil {
			return h, err
		}
	}
	return h, s.startupErr
}
func (s *replacementSource) latest() *replacementProducer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.producers) == 0 {
		return nil
	}
	return s.producers[len(s.producers)-1]
}

func replacementConfig(t *testing.T, f *sharedTriggerFixture, source string, dataSIDs, tracked []int32) runner.Config {
	t.Helper()
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.DecisionInterval, c.LatencyMS, c.ExpiryMS = 10, 1, 60
	c.Snapshot.Universe = factor.Universe{Version: source, Static: true, Investable: dataSIDs, Reference: dataSIDs, Tradable: dataSIDs, Evaluation: dataSIDs, Tracked: tracked}
	c.Snapshot.SIDMap = map[int32]string{1: "BTC", 2: "ETH", 3: "SOL"}
	for sid, symbol := range c.Snapshot.SIDMap {
		instrument := c.Execution.Instruments[sid]
		instrument.ID = symbol
		c.Execution.Instruments[sid] = instrument
	}
	c.Snapshot.Schemas = map[string]string{source: "schema-v1", "price": "schema-v1", "funding": "schema-v1"}
	c.Snapshot.SourceVersions = map[string]string{source: "v1", "price": "v1", "funding": "v1"}
	c.Prices = runner.PriceStream{Source: "price", Frequency: "event", Field: "price"}
	c.FundingSource = "funding"
	c.Plan, err = factor.New().Add("value", factor.Field(source, "value", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"value"}, Weights: map[string]float64{"value": 1}}
	for sid, symbol := range c.Snapshot.SIDMap {
		exchange, market := f.rt.Core.ExgName, f.rt.Core.Market
		if exchange == "" {
			exchange = "<runtime-unconfigured>"
		}
		if market == "" {
			market = "<runtime-unconfigured>"
		}
		if err := f.rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: exchange, Market: market}); err != nil {
			t.Fatal(err)
		}
	}
	return c
}
func replacementMapper(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
	if value, ok := s.Values["custom"]; ok && value != int64(9007199254740993) {
		return factor.VersionRecord{}, errors.New("custom type/precision lost")
	}
	if value, ok := s.Values["null"]; ok && value != nil {
		return factor.VersionRecord{}, errors.New("NULL lost")
	}
	return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
}
func registerReplacementSources(t *testing.T, f *sharedTriggerFixture, sources ...*replacementSource) {
	t.Helper()
	for _, source := range sources {
		if err := f.rt.Catalog.RegisterDataSource(source); err != nil {
			t.Fatal(err)
		}
	}
}
func replacementEngine(t *testing.T, f *sharedTriggerFixture, c runner.Config, sink *closedGridSink) *runner.Live {
	t.Helper()
	engine, err := runner.NewLive(c, sink, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
func installReplacement(t *testing.T, f *sharedTriggerFixture, c runner.Config, engine *runner.Live, mapper func(*orm.DataSeries, int64) (factor.VersionRecord, error)) *FactorLiveSubscription {
	t.Helper()
	owner, err := f.rt.InstallFactorsLive(data.NewLiveSourceProvider(f.rt.Catalog), []*runner.Live{engine}, []runner.Config{c}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper})
	if err != nil {
		t.Fatal(err)
	}
	return owner
}
func updateReplacement(owner *FactorLiveSubscription, ctx context.Context, c runner.Config, engine *runner.Live) (bool, error) {
	return owner.Update(ctx, []*runner.Live{engine}, []runner.Config{c}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){replacementMapper})
}

func TestLiveSubscriptionUpdatePreservesActiveOrdersAndNonzeroLots(t *testing.T) {
	for _, kind := range []string{"active-order", "nonzero-lot"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			req := &strat.EnterReq{Tag: kind, Amount: 1}
			if kind == "active-order" {
				req.OrderType = core.OrderTypeLimitMaker
				req.Limit = 90
			}
			f.entry(t, req)
			snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "active-order" && len(snapshot.Orders) == 0 || kind == "nonzero-lot" && (len(snapshot.Lots) == 0 || snapshot.Lots[0].SignedSteps == 0) {
				t.Fatal("account protection fixture lacks actual active order/nonzero lot")
			}
			oldSource, newSource, price, funding := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}, &replacementSource{name: "funding"}
			registerReplacementSources(t, f, oldSource, newSource, price, funding)
			oldCfg := replacementConfig(t, f, "old_signal", []int32{1}, []int32{1})
			oldCfg.Manifest.Costs.FundingPolicy = "required-stream"
			old := replacementEngine(t, f, oldCfg, &closedGridSink{})
			owner := installReplacement(t, f, oldCfg, old, replacementMapper)
			oldProducer, oldPrice, oldFunding := oldSource.latest(), price.latest(), funding.latest()
			nextCfg := replacementConfig(t, f, "new_signal", []int32{2}, []int32{1, 2})
			nextCfg.Manifest.Costs.FundingPolicy = "required-stream"
			next := replacementEngine(t, f, nextCfg, &closedGridSink{})
			committed, err := updateReplacement(owner, context.Background(), nextCfg, next)
			if err != nil || !committed {
				t.Fatalf("protected update: committed=%v err=%v", committed, err)
			}
			streams := owner.Plan().Streams()
			hasPrice, hasFunding, oldFactor := false, false, false
			for _, stream := range streams {
				if stream.Subscription.ExSymbol.ID != 1 {
					continue
				}
				switch stream.Subscription.Source {
				case "price":
					hasPrice = true
				case "funding":
					hasFunding = true
				case "old_signal":
					oldFactor = true
				}
			}
			if !hasPrice || !hasFunding || oldFactor {
				t.Fatal("factor removal lost protected account feeds")
			}
			for _, producer := range []*replacementProducer{oldProducer, oldPrice, oldFunding} {
				if !producer.handle.stopped.Load() || !producer.handle.joined.Load() {
					t.Fatal("old generation not stopped and joined")
				}
			}
			if price.latest().handle.stopped.Load() || funding.latest().handle.stopped.Load() {
				t.Fatal("candidate protection feeds stopped")
			}
			if err := price.latest().emit(1, 100, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLiveSubscriptionUpdateRejectsUnrepresentedAccountInstrument(t *testing.T) {
	for _, kind := range []string{"active-order", "nonzero-lot"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			req := &strat.EnterReq{Tag: kind, Amount: 1}
			if kind == "active-order" {
				req.OrderType = core.OrderTypeLimitMaker
				req.Limit = 90
			}
			f.entry(t, req)
			snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "active-order" && len(snapshot.Orders) == 0 || kind == "nonzero-lot" && (len(snapshot.Lots) == 0 || snapshot.Lots[0].SignedSteps == 0) {
				t.Fatal("account protection fixture lacks actual active order/nonzero lot")
			}
			oldSource, newSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, oldSource, newSource, price)
			oldCfg := replacementConfig(t, f, "old_signal", []int32{2}, []int32{2})
			owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), replacementMapper)
			previousPlan, previousProducer := owner.Plan(), oldSource.latest()
			nextCfg := replacementConfig(t, f, "new_signal", []int32{3}, []int32{2, 3})
			next := replacementEngine(t, f, nextCfg, &closedGridSink{})
			committed, err := updateReplacement(owner, context.Background(), nextCfg, next)
			if committed || err == nil || !strings.Contains(err.Error(), "active account instrument BTC") {
				t.Fatalf("unrepresented account risk accepted: %v %v", committed, err)
			}
			if owner.Plan() != previousPlan || previousProducer.handle.stopped.Load() || !newSource.latest().handle.joined.Load() {
				t.Fatal("account protection failure replaced old generation or leaked candidate")
			}
		})
	}
}

func TestLiveSubscriptionCandidateFailureRetainsOldPlanAndAdmission(t *testing.T) {
	f := newSharedTriggerFixture(t)
	failure := errors.New("candidate not ready")
	oldSource, newSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal", startupErr: failure}, &replacementSource{name: "price"}
	newSource.prepare = func(p *replacementProducer) error { return p.emit(2, 100, true) }
	registerReplacementSources(t, f, oldSource, newSource, price)
	oldCfg := replacementConfig(t, f, "old_signal", []int32{1}, []int32{1})
	var mapped atomic.Int32
	mapper := func(s *orm.DataSeries, at int64) (factor.VersionRecord, error) {
		mapped.Add(1)
		return replacementMapper(s, at)
	}
	owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), mapper)
	previousPlan, previousProducer, previousPrice := owner.Plan(), oldSource.latest(), price.latest()
	nextCfg := replacementConfig(t, f, "new_signal", []int32{2}, []int32{1, 2})
	next := replacementEngine(t, f, nextCfg, &closedGridSink{})
	committed, err := updateReplacement(owner, context.Background(), nextCfg, next)
	if committed || !errors.Is(err, failure) || owner.Plan() != previousPlan {
		t.Fatalf("candidate failure lost old plan: %v %v", committed, err)
	}
	if previousProducer.handle.stopped.Load() || previousPrice.handle.stopped.Load() || mapped.Load() != 0 || !newSource.latest().handle.joined.Load() {
		t.Fatal("candidate preparation published or stopped old source")
	}
	if err := previousProducer.emit(1, 99, false); err != nil || mapped.Load() != 1 {
		t.Fatalf("old admission lost: %v", err)
	}
	select {
	case err := <-owner.Errors():
		t.Fatalf("candidate failure reported fatal: %v", err)
	default:
	}
}

func TestLiveSubscriptionUpdateWaitsCallbackBoundaryThenJoinsOutsideIt(t *testing.T) {
	f := newSharedTriggerFixture(t)
	joinRelease := make(chan struct{})
	var joinOnce sync.Once
	releaseJoin := func() { joinOnce.Do(func() { close(joinRelease) }) }
	defer releaseJoin()
	oldSource, newSource, price := &replacementSource{name: "old_signal", joinRelease: joinRelease}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, oldSource, newSource, price)
	oldCfg := replacementConfig(t, f, "old_signal", []int32{1}, []int32{1})
	entered, release := make(chan struct{}), make(chan struct{})
	var callbackOnce sync.Once
	releaseCallback := func() { callbackOnce.Do(func() { close(release) }) }
	defer releaseCallback()
	mapper := func(s *orm.DataSeries, at int64) (factor.VersionRecord, error) {
		close(entered)
		<-release
		return replacementMapper(s, at)
	}
	owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), mapper)
	oldProducer := oldSource.latest()
	oldPlan := owner.Plan()
	callbackDone := make(chan error, 1)
	go func() { callbackDone <- oldProducer.emit(1, 99, false) }()
	<-entered
	nextCfg := replacementConfig(t, f, "new_signal", []int32{2}, []int32{1, 2})
	next := replacementEngine(t, f, nextCfg, &closedGridSink{})
	type result struct {
		committed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		committed, err := updateReplacement(owner, context.Background(), nextCfg, next)
		done <- result{committed, err}
	}()
	select {
	case <-done:
		t.Fatal("update passed admitted old callback")
	case <-time.After(20 * time.Millisecond):
	}
	if owner.Plan() != oldPlan {
		t.Fatal("generation switched inside old callback")
	}
	releaseCallback()
	if err := <-callbackDone; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for owner.Plan() == oldPlan && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if owner.Plan() == oldPlan {
		t.Fatal("ready candidate did not commit at boundary")
	}
	if err := newSource.latest().emit(2, 99, false); err != nil {
		t.Fatalf("new callback blocked behind retired Join: %v", err)
	}
	select {
	case <-done:
		t.Fatal("update returned before actual retired Join")
	case <-time.After(20 * time.Millisecond):
	}
	// Release the owned retired producer and join the update before cleanup.
	releaseJoin()
	resultValue := <-done
	if resultValue.err != nil || !resultValue.committed {
		t.Fatal(resultValue.err)
	}
}

func TestLiveSubscriptionUpdateCarriesMonotonicTargetSequence(t *testing.T) {
	f := newSharedTriggerFixture(t)
	oldSource, newSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, oldSource, newSource, price)
	oldCfg := replacementConfig(t, f, "old_signal", []int32{1, 2}, []int32{1, 2})
	oldSink := &closedGridSink{}
	owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, oldSink), replacementMapper)
	for _, sid := range []int32{1, 2} {
		if err := oldSource.latest().emit(sid, 100, true); err != nil {
			t.Fatal(err)
		}
	}
	f.rt.Clock.SetTimeMS(102)
	for _, sid := range []int32{1, 2} {
		if err := price.latest().emit(sid, 101, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(oldSink.targets) != 1 || oldSink.targets[0].Spec().PlanSequence != 1 {
		t.Fatal("initial target missing")
	}
	nextCfg := replacementConfig(t, f, "new_signal", []int32{1, 2}, []int32{1, 2})
	nextSink := &closedGridSink{}
	if committed, err := updateReplacement(owner, context.Background(), nextCfg, replacementEngine(t, f, nextCfg, nextSink)); err != nil || !committed {
		t.Fatal(err)
	}
	f.rt.Clock.SetTimeMS(110)
	for _, sid := range []int32{1, 2} {
		if err := newSource.latest().emit(sid, 110, true); err != nil {
			t.Fatal(err)
		}
	}
	f.rt.Clock.SetTimeMS(112)
	for _, sid := range []int32{1, 2} {
		if err := price.latest().emit(sid, 111, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(nextSink.targets) != 1 || nextSink.targets[0].Spec().PlanSequence != 2 || len(oldSink.targets) != 1 {
		t.Fatal("generation reset target identity or let retired decisions execute")
	}
}

func TestLiveSubscriptionSettledScopeRemovalStillAcceptsNewTargets(t *testing.T) {
	for _, mode := range []factor.PortfolioMode{factor.Full, factor.Patch} {
		t.Run(string(mode), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			oldSource, newSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, oldSource, newSource, price)
			oldCfg := replacementConfig(t, f, oldSource.name, []int32{1, 2}, []int32{1, 2})
			oldCfg.Manifest.Portfolio.Mode = mode
			oldSink := &closedGridSink{}
			owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, oldSink), replacementMapper)
			for _, sid := range []int32{1, 2} {
				if err := oldSource.latest().emit(sid, 100, true); err != nil {
					t.Fatal(err)
				}
			}
			f.rt.Clock.SetTimeMS(102)
			for _, sid := range []int32{1, 2} {
				if err := price.latest().emit(sid, 101, false); err != nil {
					t.Fatal(err)
				}
			}
			if len(oldSink.targets) != 1 {
				t.Fatal("old effective target scope was not accepted")
			}
			nextCfg := replacementConfig(t, f, newSource.name, []int32{2, 3}, []int32{2, 3})
			// A Full successor also removes stale scope inherited from a Patch predecessor.
			nextCfg.Manifest.Portfolio.Mode = factor.Full
			nextSink := &closedGridSink{}
			committed, err := updateReplacement(owner, context.Background(), nextCfg, replacementEngine(t, f, nextCfg, nextSink))
			if !committed || err != nil {
				t.Fatal(err)
			}
			f.rt.Clock.SetTimeMS(110)
			for _, sid := range []int32{2, 3} {
				if err := newSource.latest().emit(sid, 110, true); err != nil {
					t.Fatal(err)
				}
			}
			f.rt.Clock.SetTimeMS(112)
			for _, sid := range []int32{2, 3} {
				if err := price.latest().emit(sid, 111, false); err != nil {
					t.Fatal(err)
				}
			}
			if len(nextSink.targets) != 1 || nextSink.targets[0].Spec().PlanSequence != 2 {
				t.Fatal("removed settled SID left the new target waiting for an unsubscribed quote")
			}
			if _, exists := nextSink.targets[0].Targets()[1]; exists {
				t.Fatal("removed SID leaked into new portfolio")
			}
			if len(oldSink.targets[0].Targets()) != 2 {
				t.Fatal("inheritance mutated the old effective portfolio")
			}
		})
	}
}

func TestLiveSubscriptionStrategyActiveMembershipCannotUseSiblingQuotes(t *testing.T) {
	for _, scenario := range []string{"settled", "active-order", "nonzero-lot"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			if scenario != "settled" {
				req := &strat.EnterReq{Tag: scenario, Amount: 1}
				if scenario == "active-order" {
					req.OrderType = core.OrderTypeLimitMaker
					req.Limit = 90
				}
				f.entry(t, req)
			}
			signal, price := &replacementSource{name: "signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, signal, price)
			oldA := replacementConfig(t, f, signal.name, []int32{1}, []int32{1})
			oldA.StrategyID = "ts"
			oldB := oldA
			oldB.StrategyID = "sibling"
			owner, err := f.rt.InstallFactorsLive(data.NewLiveSourceProvider(f.rt.Catalog), []*runner.Live{replacementEngine(t, f, oldA, &closedGridSink{}), replacementEngine(t, f, oldB, &closedGridSink{})}, []runner.Config{oldA, oldB}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){replacementMapper, replacementMapper})
			if err != nil {
				t.Fatal(err)
			}
			nextA := replacementConfig(t, f, signal.name, []int32{2}, []int32{2})
			nextA.StrategyID = "ts"
			nextB := oldB
			committed, err := owner.Update(context.Background(), []*runner.Live{replacementEngine(t, f, nextA, &closedGridSink{}), replacementEngine(t, f, nextB, &closedGridSink{})}, []runner.Config{nextA, nextB}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){replacementMapper, replacementMapper})
			if scenario == "settled" {
				if !committed || err != nil {
					t.Fatalf("settled strategy scope could not be removed: %v", err)
				}
				return
			}
			if committed || err == nil || !strings.Contains(err.Error(), "strategy ts active instrument BTC") {
				t.Fatalf("sibling quotes hid strategy-owned active risk: committed=%v err=%v", committed, err)
			}
		})
	}
}

func TestLiveSubscriptionRejectsCrossGenerationComputationAliasAndAllowsCandidateSharing(t *testing.T) {
	f := newSharedTriggerFixture(t)
	signal, price := &replacementSource{name: "signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, signal, price)
	c := replacementConfig(t, f, signal.name, []int32{1, 2}, []int32{1, 2})
	c.Chunks = nil
	c.ComputationGroup = runner.NewComputationGroup()
	c.ComputationContext = runner.ComputationContext{DataNamespace: "test", ClockDomain: "clock", SamplingIdentity: "source"}
	oldSink := &closedGridSink{}
	old := replacementEngine(t, f, c, oldSink)
	owner := installReplacement(t, f, c, old, replacementMapper)
	alias := replacementEngine(t, f, c, &closedGridSink{})
	defer alias.Stop()
	if !alias.SharesComputation(old) {
		t.Fatal("fixture did not alias real mutable session")
	}
	if committed, err := updateReplacement(owner, context.Background(), c, alias); committed || err == nil || !strings.Contains(err.Error(), "independent computation sessions") {
		t.Fatalf("cross-generation session alias accepted: %v %v", committed, err)
	}
	if len(signal.producers) != 1 {
		t.Fatal("rejected shared candidate started warming producers")
	}
	for _, sid := range []int32{1, 2} {
		if err := signal.latest().emit(sid, 100, true); err != nil {
			t.Fatal(err)
		}
	}
	f.rt.Clock.SetTimeMS(102)
	for _, sid := range []int32{1, 2} {
		if err := price.latest().emit(sid, 101, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(oldSink.targets) != 1 {
		t.Fatal("rejected candidate damaged old computation")
	}
	// Two consumers of one candidate may share its independently owned group.
	// A distinct owner fixture avoids expanding strategy ownership during update.
	f2 := newSharedTriggerFixture(t)
	signal2, price2 := &replacementSource{name: "signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f2, signal2, price2)
	a := replacementConfig(t, f2, signal2.name, []int32{1, 2}, []int32{1, 2})
	a.Chunks = nil
	b := a
	b.StrategyID = "second"
	pairOwner, err := f2.rt.InstallFactorsLive(data.NewLiveSourceProvider(f2.rt.Catalog), []*runner.Live{replacementEngine(t, f2, a, &closedGridSink{}), replacementEngine(t, f2, b, &closedGridSink{})}, []runner.Config{a, b}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){replacementMapper, replacementMapper})
	if err != nil {
		t.Fatal(err)
	}
	a.ComputationGroup = runner.NewComputationGroup()
	a.ComputationContext = c.ComputationContext
	b.ComputationGroup = a.ComputationGroup
	b.ComputationContext = a.ComputationContext
	nextA, nextB := replacementEngine(t, f2, a, &closedGridSink{}), replacementEngine(t, f2, b, &closedGridSink{})
	if !nextA.SharesComputation(nextB) {
		t.Fatal("candidate consumers did not share intended session")
	}
	if committed, err := pairOwner.Update(context.Background(), []*runner.Live{nextA, nextB}, []runner.Config{a, b}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){replacementMapper, replacementMapper}); !committed || err != nil {
		t.Fatalf("independent candidate sharing rejected: %v %v", committed, err)
	}
}

func TestLiveSubscriptionSnapshotBoundarySerializesMutationAndCancellation(t *testing.T) {
	f := newSharedTriggerFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	boundaryDone := make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		boundaryDone <- f.rt.SharedExecution().AtSnapshot(context.Background(), func(snapshot execution.AccountSnapshot) error { close(entered); <-release; return nil })
	}()
	<-entered
	mutated := make(chan error, 1)
	go func() {
		mutated <- f.rt.SharedExecution().CashEvent(execution.CashEvent{ID: "boundary-funding", Kind: execution.Funding, AccountDelta: decimal.NewFromInt(1), Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(1)}}, AtMS: 101})
	}()
	select {
	case <-mutated:
		t.Fatal("account mutation crossed serialized snapshot callback")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-boundaryDone; err != nil {
		t.Fatal(err)
	}
	if err := <-mutated; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := f.rt.SharedExecution().AtSnapshot(ctx, func(execution.AccountSnapshot) error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
		t.Fatal("canceled snapshot committed callback")
	}
}

func TestLiveSubscriptionCallerCancellationAfterCommitDoesNotStopNewGeneration(t *testing.T) {
	f := newSharedTriggerFixture(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	oldSource, nextSource, price := &replacementSource{name: "old_signal", joinRelease: release}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, oldSource, nextSource, price)
	oldCfg := replacementConfig(t, f, oldSource.name, []int32{1}, []int32{1})
	owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), replacementMapper)
	oldPlan := owner.Plan()
	nextCfg := replacementConfig(t, f, nextSource.name, []int32{2}, []int32{1, 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		committed, err := updateReplacement(owner, ctx, nextCfg, replacementEngine(t, f, nextCfg, &closedGridSink{}))
		if !committed && err == nil {
			err = errors.New("candidate not committed")
		}
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for owner.Plan() == oldPlan && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if owner.Plan() == oldPlan {
		t.Fatal("candidate did not reach commit")
	}
	cancel()
	if err := nextSource.latest().emit(2, 99, false); err != nil || nextSource.latest().handle.stopped.Load() {
		t.Fatalf("postcommit caller cancellation stopped owned generation: %v", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLiveSubscriptionCandidateCancellationAndMetadataChangesKeepOldGeneration(t *testing.T) {
	for _, scenario := range []string{"startup-version", "history-failure", "history-cancel", "commit-cancel", "commit-version", "commit-stop", "stale-warmup"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			oldSource, nextSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, oldSource, nextSource, price)
			oldCfg := replacementConfig(t, f, oldSource.name, []int32{1}, []int32{1})
			owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), replacementMapper)
			oldPlan, oldProducer := owner.Plan(), oldSource.latest()
			nextCfg := replacementConfig(t, f, nextSource.name, []int32{2}, []int32{1, 2})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var prepared atomic.Bool
			nextSource.prepare = func(*replacementProducer) error {
				prepared.Store(true)
				if scenario == "startup-version" {
					nextSource.version.Add(1)
				}
				return nil
			}
			if strings.HasPrefix(scenario, "history-") || scenario == "stale-warmup" {
				var err error
				nextCfg.Plan, err = factor.New().Add("value", factor.EMA(factor.Field(nextSource.name, "value", "event"), 2)).Compile()
				if err != nil {
					t.Fatal(err)
				}
				nextSource.history = func(historyCtx context.Context, sub *orm.Subscription, from, to int64) ([]*orm.DataRecord, error) {
					if scenario == "history-failure" {
						return nil, errors.New("candidate history missing")
					}
					if scenario == "history-cancel" {
						cancel()
						<-historyCtx.Done()
						return nil, historyCtx.Err()
					}
					var rows []*orm.DataRecord
					for i := 0; i < sub.WarmupNum; i++ {
						at := to - int64(sub.WarmupNum-1-i)*10
						rows = append(rows, &orm.DataRecord{Sid: sub.ExSymbol.ID, TimeMS: at - 1, EndMS: at, Closed: true, Values: map[string]any{"value": float64(i + 1)}})
					}
					f.rt.Clock.SetTimeMS(to + 10)
					return rows, nil
				}
			}
			if strings.HasPrefix(scenario, "commit-") {
				// Version is checked at prepare completion and again inside
				// the serialized commit callback; trigger on the latter read.
				var reads atomic.Int32
				nextSource.versionHook = func() {
					if !prepared.Load() || reads.Add(1) != 2 {
						return
					}
					switch scenario {
					case "commit-cancel":
						cancel()
					case "commit-version":
						nextSource.version.Add(1)
					case "commit-stop":
						owner.Stop()
					}
				}
			}
			next := replacementEngine(t, f, nextCfg, &closedGridSink{})
			committed, err := updateReplacement(owner, ctx, nextCfg, next)
			if committed || err == nil || owner.Plan() != oldPlan {
				t.Fatalf("failed candidate changed generation: committed=%v err=%v", committed, err)
			}
			if !nextSource.latest().handle.stopped.Load() || !nextSource.latest().handle.joined.Load() {
				t.Fatal("failed candidate producer was not stopped and joined")
			}
			if err := next.ValidateWarmup(f.rt.Clock.TimeMS()); err == nil {
				t.Fatal("candidate runner left active after rollback")
			}
			if scenario != "commit-stop" {
				if oldProducer.handle.stopped.Load() {
					t.Fatal("rollback stopped previous producer")
				}
				if err := oldProducer.emit(1, 99, false); err != nil {
					t.Fatalf("previous intake unhealthy: %v", err)
				}
			}
		})
	}
}

func TestLiveSubscriptionUpdateRejectsSharedLegacyBeforeCandidateStartup(t *testing.T) {
	f := newSharedTriggerFixture(t)
	oldSource, nextSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, oldSource, nextSource, price)
	oldCfg := replacementConfig(t, f, oldSource.name, []int32{1}, []int32{1})
	owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), replacementMapper)
	f.rt.factorLegacySubs = []*strat.DataSub{{Source: oldSource.name, TimeFrame: "event", ExSymbol: f.rt.Symbols.GetSymbolByID(1), WarmupNum: 2}}
	nextCfg := replacementConfig(t, f, nextSource.name, []int32{2}, []int32{1, 2})
	next := replacementEngine(t, f, nextCfg, &closedGridSink{})
	defer next.Stop()
	if committed, err := updateReplacement(owner, context.Background(), nextCfg, next); committed || err == nil || !strings.Contains(err.Error(), "fixed legacy") {
		t.Fatalf("shared legacy candidate accepted: %v %v", committed, err)
	}
	if nextSource.latest() != nil || oldSource.latest().handle.stopped.Load() {
		t.Fatal("rejected legacy update affected producers")
	}
}

func TestFactorLivePlanUsesRuntimeSourceBudgets(t *testing.T) {
	f := newSharedTriggerFixture(t)
	signal, price := &replacementSource{name: "signal"}, &replacementSource{name: "price"}
	registerReplacementSources(t, f, signal, price)
	f.rt.sourcePlanOptions = data.SubscriptionPlanOptions{Namespace: "experiment", PageRows: 7, PrefetchRows: 11, PageBytes: 512}
	c := replacementConfig(t, f, signal.name, []int32{1}, []int32{1})
	engine := replacementEngine(t, f, c, &closedGridSink{})
	defer engine.Stop()
	plan, err := f.rt.CompileFactorLivePlan(engine, c)
	if err != nil {
		t.Fatal(err)
	}
	options := plan.Options()
	// Two streams share the eleven-row prefetch budget, capping pages at five.
	if options.PageRows != 5 || options.PrefetchRows != 11 || options.PageBytes != 512 || !strings.HasSuffix(options.Namespace, "/namespace/experiment") || options.AnchorMS != f.rt.Clock.TimeMS() {
		t.Fatalf("runtime data budget lost: %+v", options)
	}
}

func TestLiveSubscriptionUpdateRemovesSettledExecutionMembershipAndRejectsMissingKlineFactory(t *testing.T) {
	for _, scenario := range []string{"execution-removal", "kline"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			oldSource, nextSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "new_signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, oldSource, nextSource, price)
			oldCfg := replacementConfig(t, f, oldSource.name, []int32{1}, []int32{1})
			owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), replacementMapper)
			oldPlan := owner.Plan()
			nextCfg := replacementConfig(t, f, nextSource.name, []int32{2}, []int32{2})
			want := "independent kline provider generation factory"
			if scenario == "kline" {
				var err error
				nextCfg.Plan, err = factor.New().Add("value", factor.Field(orm.SeriesSourceKline, "close", "1m")).Compile()
				if err != nil {
					t.Fatal(err)
				}
				nextCfg.Snapshot.Schemas[orm.SeriesSourceKline], nextCfg.Snapshot.SourceVersions[orm.SeriesSourceKline] = "schema-v1", "v1"
				nextCfg.Snapshot.Universe.Tracked = []int32{1, 2}
				want = "independent kline provider generation factory"
			}
			next := replacementEngine(t, f, nextCfg, &closedGridSink{})
			defer next.Stop()
			committed, err := updateReplacement(owner, context.Background(), nextCfg, next)
			if scenario == "execution-removal" {
				if !committed || err != nil || owner.Plan() == oldPlan {
					t.Fatalf("settled SID removal failed: committed=%v err=%v", committed, err)
				}
				for _, stream := range owner.Plan().Streams() {
					if stream.Subscription.ExSymbol.ID == 1 {
						t.Fatal("settled SID left subscribed")
					}
				}
				return
			}
			if committed || err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("unsupported update accepted: %v %v", committed, err)
			}
			if owner.Plan() != oldPlan || oldSource.latest().handle.stopped.Load() || nextSource.latest() != nil {
				t.Fatal("rejected update changed installed generation")
			}
		})
	}
}

func TestLiveSubscriptionFixedLegacyCanonicalIngressSurvivesUpdateAndRollback(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			rt, err := f.process.NewRuntime(Options{Mode: core.RunModeBackTest, Config: &config.Config{Exchange: &config.ExchangeConfig{Name: "observer"}, MarketType: "linear"}, Exchange: &observerRuntimeExchange{}, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			f.rt = rt
			rt.Clock.SetTimeMS(100)
			oldSource, nextSource, price := &replacementSource{name: "old_signal"}, &replacementSource{name: "next_signal"}, &replacementSource{name: "price"}
			registerReplacementSources(t, f, oldSource, nextSource, price)
			oldCfg := replacementConfig(t, f, oldSource.name, []int32{1}, []int32{1})
			legacySub := &strat.DataSub{Source: oldSource.name, TimeFrame: "event", ExSymbol: f.rt.Symbols.GetSymbolByID(1), WarmupNum: 2, Fields: []string{"value", "custom", "null"}}
			// Keep the source schema explicit even for arbitrary typed extras.
			legacySub.Fields = []string{"value"}
			warm, live := 0, 0
			f.job.Strat.OnDataSubs = func(*strat.StratJob) []*strat.DataSub { return []*strat.DataSub{legacySub} }
			f.job.Strat.OnData = func(job *strat.StratJob, event strat.DataEvent) {
				if event.Raw("custom") != int64(9007199254740993) || event.Raw("null") != nil {
					t.Error("legacy raw type/NULL changed")
				}
				if job.IsWarmUp {
					warm++
				} else {
					live++
				}
			}
			if err := f.rt.BindFactorLegacyJobs([]*strat.StratJob{f.job}, []*strat.DataSub{legacySub}); err != nil {
				t.Fatal(err)
			}
			oldSource.history = func(_ context.Context, sub *orm.Subscription, _, to int64) ([]*orm.DataRecord, error) {
				var rows []*orm.DataRecord
				for i := sub.WarmupNum; i > 0; i-- {
					at := to - int64(i-1)*10
					rows = append(rows, &orm.DataRecord{Sid: 1, TimeMS: at - 1, EndMS: at, Closed: true, Values: map[string]any{"value": 1.0, "price": 100.0, "custom": int64(9007199254740993), "null": nil}})
				}
				return rows, nil
			}
			mapper := func(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
				record, err := replacementMapper(series, received)
				if revision, exists := series.Values["revision"]; exists {
					record.Revision = revision.(uint64)
				}
				return record, err
			}
			owner := installReplacement(t, f, oldCfg, replacementEngine(t, f, oldCfg, &closedGridSink{}), mapper)
			if warm != 2 || len(oldSource.producers) != 1 {
				t.Fatal("initial union duplicated history or physical stream")
			}
			previousProducer, previousPlan := oldSource.latest(), owner.Plan()
			if err := previousProducer.emit(1, 99, false); err != nil {
				t.Fatal(err)
			}
			if live != 1 {
				t.Fatal("legacy canonical ingress missing")
			}
			oldSource.prepare = func(p *replacementProducer) error {
				if err := p.emit(1, 99, false); err != nil {
					return err
				}
				return p.sink.Emit(p.subs[0], []*orm.DataRecord{{Sid: 1, TimeMS: 98, EndMS: 99, Values: map[string]any{"value": 2.0, "price": 100.0, "custom": int64(9007199254740993), "null": nil, "revision": uint64(2)}}})
			}
			if failure {
				nextSource.startupErr = errors.New("candidate unavailable")
			}
			nextCfg := replacementConfig(t, f, nextSource.name, []int32{2}, []int32{1, 2})
			next := replacementEngine(t, f, nextCfg, &closedGridSink{})
			committed, err := owner.Update(context.Background(), []*runner.Live{next}, []runner.Config{nextCfg}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper})
			if committed == failure || (err != nil) != failure {
				t.Fatalf("update: %v %v", committed, err)
			}
			if warm != 2 {
				t.Fatal("candidate replay warmed shared TS state")
			}
			if failure {
				if live != 1 || owner.Plan() != previousPlan || previousProducer.handle.stopped.Load() || !nextSource.latest().handle.joined.Load() {
					t.Fatal("failed candidate polluted legacy/current ingress")
				}
			} else {
				if live != 2 {
					t.Fatalf("queued overlap revision handling: calls=%d", live)
				}
				if !previousProducer.handle.joined.Load() || oldSource.latest().handle.stopped.Load() {
					t.Fatal("canonical physical producer not replaced and joined")
				}
			}
		})
	}
}
