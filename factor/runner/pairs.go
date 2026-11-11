package runner

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// Pair names two symbols exactly as they appear in the run's SIDMap
// (for example "ETH/USDT:USDT"). The regression is ln(Y) = alpha + beta*ln(X).
type Pair struct{ Y, X string }

// PairsSpec declares a rolling-regression z-score pairs strategy.
type PairsSpec struct {
	Pairs  []Pair
	Field  string                // kline field, default "close"
	Window int                   // regression window in decision bars, at least 3
	Band   statistics.BandConfig // entry/exit/stop/max-hold thresholds
	// Gross is the gross notional of one open pair as a fraction of strategy NAV.
	// Default 1/len(Pairs), so all pairs open at once never exceed 100% gross.
	Gross float64
}

const pairsLogOutput = "logp"

// RegisterPairs registers a statistical-arbitrage strategy in one call. The
// build function reads params once per run and returns a declarative spec:
//
//	runner.RegisterPairs("eth-btc", func(p *runner.Params) (runner.PairsSpec, error) {
//	    return runner.PairsSpec{
//	        Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
//	        Window: p.Int("window", 120),
//	        Band:   statistics.BandConfig{Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5), Stop: p.Float("stop", 4)},
//	    }, p.Err()
//	})
//
// Both legs must be in the run's Tradable universe. Each run gets private
// state: one RollingSpread and one BandTrader per pair, so replay and live
// runs never share positions. The hedge ratio is frozen at entry while a pair
// is open (no beta-drift churn) and refreshed for the next entry. Each pair
// contributes Y: +s*G/(1+|b|), X: -s*G*b/(1+|b|) where s is the spread
// position and G the pair's gross share, so a pair is dollar-hedged by beta.
//
// The strategy binds its own portfolio builder, so YAML only needs
// `name: <name>`. Wire it with `run_timeframes` for the decision bar.
func RegisterPairs(name string, build func(*Params) (PairsSpec, error)) error {
	if name == "" || build == nil {
		return errors.New("runner: pairs strategy needs name and build function")
	}
	builderName := name + "-pairs-v1"
	pairsLegResolvers.Store(builderName, build)
	err := RegisterDefinition(name, func(c Config) (*factor.Plan, research.ComboSpec, error) {
		spec, err := resolvePairsSpec(build, c)
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		field := factor.Field("kline", spec.Field, c.Factor.TimeFrame)
		logp := factor.Custom("pairs-log-v1", []*factor.Node{field}, func(v []factor.Numeric) factor.Numeric {
			if v[0].Validity != factor.Valid {
				return v[0]
			}
			if v[0].Value <= 0 {
				return factor.Numeric{Value: math.NaN(), Validity: factor.NonFinite}
			}
			return factor.Numeric{Value: math.Log(v[0].Value), Validity: factor.Valid}
		})
		plan, err := factor.New().Add(pairsLogOutput, logp).Compile()
		return plan, research.ComboSpec{Method: research.Equal, Columns: []string{pairsLogOutput}}, err
	})
	if err != nil {
		return err
	}
	err = RegisterPortfolioFactory(builderName, func(c Config) (PortfolioBuilder, error) {
		spec, err := resolvePairsSpec(build, c)
		if err != nil {
			return nil, err
		}
		builder, tracker, err := newTrackedPairsBuilderSkipping(spec, c.Snapshot.SIDMap, c.UnavailableSymbols)
		if err == nil && c.observe != nil {
			c.observe.pairs, c.observe.reset = tracker.report, tracker.resetStats
		}
		return builder, err
	}, func(c Config) error {
		_, err := resolvePairsSpec(build, c)
		return err
	})
	if err != nil {
		return err
	}
	bindDefinitionBuilder(name, builderName)
	return nil
}

func resolvePairsSpec(build func(*Params) (PairsSpec, error), c Config) (PairsSpec, error) {
	p := c.Params()
	spec, err := build(p)
	if err != nil {
		return spec, err
	}
	if err := p.Err(); err != nil {
		return spec, err
	}
	if spec.Field == "" {
		spec.Field = "close"
	}
	if len(spec.Pairs) == 0 {
		return spec, errors.New("runner: pairs strategy declares no pairs")
	}
	if spec.Window < 3 {
		return spec, errors.New("runner: pairs window must be at least 3")
	}
	if err := spec.Band.Validate(); err != nil {
		return spec, err
	}
	if spec.Gross == 0 {
		spec.Gross = 1 / float64(len(spec.Pairs))
	}
	if !(spec.Gross > 0) || spec.Gross*float64(len(spec.Pairs)) > 1+1e-9 {
		return spec, errors.New("runner: pairs gross per pair must be positive and keep total gross within 1")
	}
	for _, pair := range spec.Pairs {
		if pair.Y == "" || pair.X == "" || pair.Y == pair.X {
			return spec, errors.New("runner: each pair needs two distinct symbols")
		}
	}
	return spec, nil
}

type pairState struct {
	y, x int32
	// unavailable is the leg (symbol) that has no data at all inside this run's window and was left out of its
	// universe by the engine (BtFeatureLevel 11); the pair is skipped for the whole run: never traded, no spread.
	unavailable string
	spread      *statistics.RollingSpread
	trader      *statistics.BandTrader
	beta        float64 // hedge ratio frozen at entry
	pos         int
	stats       pairStats // result bookkeeping only; never read by the trading logic
}

func newPairsBuilder(spec PairsSpec, sidMap map[int32]string) (PortfolioBuilder, error) {
	b, _, err := newTrackedPairsBuilder(spec, sidMap)
	return b, err
}

// newTrackedPairsBuilder is newPairsBuilder plus the tracker that reports per-pair results after the run. The
// tracker is write-only state of the builder: weights are computed exactly as without it.
func newTrackedPairsBuilder(spec PairsSpec, sidMap map[int32]string) (PortfolioBuilder, *pairsTracker, error) {
	return newTrackedPairsBuilderSkipping(spec, sidMap, nil)
}

// newTrackedPairsBuilderSkipping is newTrackedPairsBuilder for a run whose universe may lack legs the engine left out
// because they have no stored data inside the run window (unavailable: symbol -> reason). Such a pair was chosen on
// information from before the window (point in time), so it is not an error and not removed from the strategy: it is
// skipped for this run and the other pairs trade as usual. A pair with a leg that is neither in the universe nor in
// unavailable is still an error. Every pair keeps its Gross share: skipped pairs leave their exposure idle, the
// remaining pairs are never scaled up (no hidden leverage when pairs drop out).
func newTrackedPairsBuilderSkipping(spec PairsSpec, sidMap map[int32]string, unavailable map[string]string) (PortfolioBuilder, *pairsTracker, error) {
	sids := make(map[string]int32, len(sidMap))
	for sid, symbol := range sidMap {
		sids[symbol] = sid
	}
	states := make([]*pairState, len(spec.Pairs))
	for i, pair := range spec.Pairs {
		y, okY := sids[pair.Y]
		x, okX := sids[pair.X]
		if !okY || !okX {
			var missing []string
			if !okY {
				missing = append(missing, pair.Y)
			}
			if !okX {
				missing = append(missing, pair.X)
			}
			for _, sym := range missing {
				if _, gone := unavailable[sym]; !gone {
					return nil, nil, fmt.Errorf("runner: pair %s/%s has a symbol missing from this run's universe", pair.Y, pair.X)
				}
			}
			states[i] = &pairState{y: -1, x: -1, unavailable: strings.Join(missing, ",")}
			continue
		}
		rs, err := statistics.NewRollingSpread(spec.Window)
		if err != nil {
			return nil, nil, err
		}
		bt, err := statistics.NewBandTrader(spec.Band)
		if err != nil {
			return nil, nil, err
		}
		states[i] = &pairState{y: y, x: x, spread: rs, trader: bt}
	}
	tracker := &pairsTracker{states: states, names: spec.Pairs}
	tracker.resetStats()
	allUnavailable := true
	for _, st := range states {
		if st.unavailable == "" {
			allUnavailable = false
			break
		}
	}
	build := func(frame factor.Frame, universe factor.Universe, target factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		tradable := make(map[int32]bool, len(universe.Tradable))
		for _, sid := range universe.Tradable {
			tradable[sid] = true
		}
		values := frame.Values[pairsLogOutput]
		var diags []factor.Diagnostic
		ready := false
		navPrev := tracker.lastNAV
		tracker.lastNAV = target.Budget.NAV
		tracker.lastMS = frame.DecisionTime
		if allUnavailable {
			// No pair can trade in this run: an explicit flat target at every decision (equity stays at the initial value).
			target.Diagnostics = []factor.Diagnostic{{Code: "pairs-all-unavailable", Detail: "every pair has a leg without data in the run window"}}
			p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
			return p, nil, err
		}
		for _, st := range states {
			if st.unavailable != "" {
				continue
			}
			ly, okY := values[st.y]
			lx, okX := values[st.x]
			if !okY || !okX || ly.Validity != factor.Valid || lx.Validity != factor.Valid {
				diags = append(diags, factor.Diagnostic{Code: "pair-input-missing", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				st.stats.paused++
				st.stats.prevOK = false
				if st.pos != 0 {
					st.stats.closeEpisode(frame.DecisionTime, 0, "leg_missing")
					// 067: a leg has no data this period (PIT universe drop / delisting): close the pair and pause it;
					// it may reopen when both legs are back
					st.pos, st.beta = 0, 0
					st.trader.Reset()
					ready = true // the closed pair must reach the ledger even if no other pair is ready
					diags = append(diags, factor.Diagnostic{Code: "pair-closed-missing-leg", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				}
				continue
			}
			st.stats.accrue(ly.Value, lx.Value, navPrev)
			point, ok := st.spread.Observe(lx.Value, ly.Value)
			if !ok {
				diags = append(diags, factor.Diagnostic{Code: "pair-warmup", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				continue
			}
			ready = true
			st.stats.bars++
			if !tradable[st.y] || !tradable[st.x] {
				// Never open on an untradable leg; an existing position keeps its state
				// but the target below drops to flat only through the band rules.
				if st.pos == 0 {
					continue
				}
			}
			prev := st.pos
			st.pos = st.trader.Step(point.Z)
			if st.pos != 0 && (prev == 0 || prev != st.pos) {
				st.beta = point.Beta
			}
			st.stats.observeStep(prev, st.pos, point.Z, st.beta, frame.DecisionTime, spec.Band)
		}
		if !ready {
			return nil, append(diags, factor.Diagnostic{Code: "pairs-not-ready", Detail: "no pair has a full window; keep previous portfolio"}), nil
		}
		weights := map[int32]float64{}
		for _, st := range states {
			var wy, wx float64
			if st.pos != 0 {
				norm := 1 + math.Abs(st.beta)
				s := float64(st.pos) * spec.Gross / norm
				wy, wx = s, -s*st.beta
				weights[st.y] += wy
				weights[st.x] += wx
			}
			st.stats.setWeights(wy, wx)
		}
		for sid, w := range weights {
			if w == 0 {
				delete(weights, sid)
			}
		}
		target.Diagnostics = slices.Clone(diags)
		p, err := factor.NewTargetPortfolio(target, weights)
		return p, nil, err
	}
	return build, tracker, nil
}

// pairsLegResolvers maps a registered pairs portfolio builder name to its spec constructor, so the runner can
// derive the pair legs of a run (070: per-pair PIT).
var pairsLegResolvers sync.Map

// registeredPairLegs returns the (Y, X) SIDs of the run's pairs strategy, nil when the run is not a pairs one.
func registeredPairLegs(c Config) [][2]int32 {
	name := c.Manifest.Portfolio.Builder
	if name == "" {
		name = boundDefinitionBuilder(c.Definition)
	}
	v, ok := pairsLegResolvers.Load(name)
	if !ok {
		return nil
	}
	build, _ := v.(func(*Params) (PairsSpec, error))
	if build == nil {
		return nil
	}
	spec, err := resolvePairsSpec(build, c)
	if err != nil {
		return nil
	}
	sids := make(map[string]int32, len(c.Snapshot.SIDMap))
	for sid, symbol := range c.Snapshot.SIDMap {
		sids[symbol] = sid
	}
	var out [][2]int32
	for _, p := range spec.Pairs {
		y, okY := sids[p.Y]
		x, okX := sids[p.X]
		if okY && okX {
			out = append(out, [2]int32{y, x})
		}
	}
	return out
}
