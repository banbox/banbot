package data

import (
	"context"
	"fmt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"sync"
	"testing"
)

// FactorLiveGenerationHarness supplies independent in-memory provider/feeder
// generations. It uses real callback admission, warming and Stop/Join. Only
// historical reads and the physical socket are fixture-owned.
type FactorLiveGenerationHarness struct {
	mu          sync.Mutex
	providers   []*LiveProvider
	History     func(int, *orm.ExSymbol, string, int) ([]*orm.DataSeries, error)
	WarmStarted func(int)
}

type generationWarmupFeeder struct {
	*SeriesFeeder
	harness *FactorLiveGenerationHarness
	index   int
}

func (f *generationWarmupFeeder) WarmTfs(_ int64, nums map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	if f.harness.WarmStarted != nil {
		f.harness.WarmStarted(f.index)
	}
	var through int64
	for tf, count := range nums {
		if count == 0 {
			continue
		}
		rows, err := f.harness.History(f.index, f.ExSymbol, tf, count)
		if err != nil {
			return 0, nil, errs.New(1, err)
		}
		var warmErr *errs.Error
		through, warmErr = f.warmTfWithErr(tf, rows)
		if warmErr != nil {
			return 0, nil, warmErr
		}
	}
	return through, nil, nil
}

func NewFactorLiveGenerationHarness(deps *RuntimeDeps) (*FactorLiveGenerationHarness, *LiveProvider) {
	h := &FactorLiveGenerationHarness{}
	var factory LiveProviderGenerationFactory
	factory = func(_ context.Context, _ *SubscriptionPlan) (*LiveProvider, error) {
		h.mu.Lock()
		index := len(h.providers)
		p := &LiveProvider{catalog: deps.Catalog, deps: deps, symbols: deps.Symbols, Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{}, deps: deps, wsSubs: strat.NewWsSubJobRegistryWithState(deps.Strategies, deps.Symbols)}}
		p.generationFactory = factory
		p.newFeeder = func(pair string, tfs []string) (IDataFeeder, *errs.Error) {
			exchange, market := deps.identity()
			symbol := deps.Symbols.GetExSymbol2(exchange, market, pair)
			if symbol == nil {
				return nil, errs.NewMsg(1, "missing fixture symbol")
			}
			feeder := &SeriesFeeder{Feeder: Feeder{ExSymbol: symbol, deps: deps, symbols: deps.Symbols, tfBars: map[string][]*orm.DataSeries{}}}
			for _, tf := range tfs {
				seconds, _ := utils2.TFToSecSafe(tf)
				feeder.States = append(feeder.States, &PairTFCache{TimeFrame: tf, TFSecs: seconds})
			}
			return &generationWarmupFeeder{SeriesFeeder: feeder, harness: h, index: index}, nil
		}
		h.providers = append(h.providers, p)
		h.mu.Unlock()
		return p, nil
	}
	initial, _ := factory(context.Background(), nil)
	return h, initial
}

func (h *FactorLiveGenerationHarness) Emit(index int, row *orm.DataSeries) error {
	h.mu.Lock()
	p := h.providers[index]
	h.mu.Unlock()
	if h.Stopped(index) {
		return context.Canceled
	}
	if _, exists := p.getHolder(row.ExSymbol.Symbol); !exists {
		return fmt.Errorf("fixture generation lacks holder")
	}
	exchange, market := p.deps.identity()
	seconds, _ := utils2.TFToSecSafe(row.TimeFrame)
	makeOnSeriesMsg(p)(&SeriesMsg{ExgName: exchange, Market: market, Pair: row.ExSymbol.Symbol, NotifySeries: NotifySeries{TFSecs: seconds, Interval: seconds, Rows: []*orm.DataSeries{row}}})
	p.joinHandlers()
	if p.subscriptionSink != nil {
		p.subscriptionSink.mu.Lock()
		defer p.subscriptionSink.mu.Unlock()
		return p.subscriptionSink.firstErr
	}
	return nil
}

func (h *FactorLiveGenerationHarness) Stopped(index int) bool {
	h.mu.Lock()
	p := h.providers[index]
	h.mu.Unlock()
	p.handlerLock.Lock()
	defer p.handlerLock.Unlock()
	return p.handlerStop
}

func (h *FactorLiveGenerationHarness) Cached(index int, pair, timeframe string, timeMS int64) *orm.DataSeries {
	h.mu.Lock()
	p := h.providers[index]
	h.mu.Unlock()
	holder, ok := p.getHolder(pair)
	if !ok {
		return nil
	}
	for _, row := range holder.(*generationWarmupFeeder).tfBars[timeframe] {
		if row.TimeMS == timeMS {
			copyRow := *row
			return &copyRow
		}
	}
	return nil
}

// FactorLiveProviderFixture installs in-memory, already-subscribed feeders only
// in test builds. Delivery uses the real provider handler and SeriesFeeder.
// Historical warming/storage and spider sockets are outside this fixture.
func FactorLiveProviderFixture(t *testing.T, deps *RuntimeDeps, symbols []*orm.ExSymbol, consume FnDataSeries) (*LiveProvider, func(*SeriesMsg)) {
	t.Helper()
	p := &LiveProvider{Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{}}, deps: deps, symbols: deps.Symbols}
	for _, symbol := range symbols {
		p.setHolder(symbol.Symbol, &SeriesFeeder{Feeder: Feeder{ExSymbol: symbol, deps: deps, symbols: deps.Symbols, CallBack: consume, tfBars: map[string][]*orm.DataSeries{}, States: []*PairTFCache{{TimeFrame: "1m", TFSecs: 60}, {TimeFrame: "1h", TFSecs: 3600}}}})
	}
	onMessage := makeOnSeriesMsg(p)
	t.Cleanup(func() { _ = p.Stop(); p.Join() })
	return p, func(msg *SeriesMsg) {
		onMessage(msg)
		// Wait before the next message advances the injected receipt clock.
		p.joinHandlers()
	}
}

type factorWarmupFeeder struct {
	*SeriesFeeder
	rows []*orm.DataSeries
}

func (f *factorWarmupFeeder) WarmTfs(_ int64, tfNums map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	var end int64
	for tf := range tfNums {
		var err *errs.Error
		end, err = f.warmTfWithErr(tf, f.rows)
		if err != nil {
			return 0, nil, err
		}
	}
	return end, nil, nil
}

// FactorLiveWarmupFixture runs the real SubWarmPairs orchestration and the
// actual SeriesFeeder warmup callbacks. Only historical storage fetches and
// spider network subscription are replaced by already-loaded typed rows.
func FactorLiveWarmupFixture(t *testing.T, deps *RuntimeDeps, symbols []*orm.ExSymbol, history map[int32][]*orm.DataSeries, consume FnDataSeries) *Provider[IDataFeeder] {
	t.Helper()
	byName := make(map[string]*orm.ExSymbol)
	for _, symbol := range symbols {
		byName[symbol.Symbol] = symbol
	}
	p := &Provider[IDataFeeder]{deps: deps, holders: make(map[string]IDataFeeder), wsSubs: strat.NewWsSubJobRegistryWithState(deps.Strategies, deps.Symbols)}
	p.newFeeder = func(pair string, tfs []string) (IDataFeeder, *errs.Error) {
		symbol := byName[pair]
		feeder := &SeriesFeeder{Feeder: Feeder{ExSymbol: symbol, deps: deps, symbols: deps.Symbols, CallBack: consume, tfBars: make(map[string][]*orm.DataSeries)}}
		feeder.SubTfs(tfs, false)
		return &factorWarmupFeeder{SeriesFeeder: feeder, rows: history[symbol.ID]}, nil
	}
	return p
}
