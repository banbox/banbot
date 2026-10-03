package factor

import (
	"errors"
	"maps"
	"math"
	"slices"
	"sort"
)

type PortfolioMode string

const (
	Full  PortfolioMode = "full"
	Patch PortfolioMode = "patch"
)

// FrozenBudget is strategy NAV, not account equity, margin or leverage.
type FrozenBudget struct {
	Version  string
	Currency string
	NAV      float64
}
type PortfolioSpec struct {
	StrategyID      string
	AccountID       string
	DecisionTime    int64
	ExecutableAt    int64
	ExpireAt        int64
	PlanSequence    uint64
	SnapshotID      string
	PlanHash        string
	FactorPlanHash  string
	UniverseVersion string
	Budget          FrozenBudget
	Mode            PortfolioMode
	Diagnostics     []Diagnostic
}
type Diagnostic struct {
	Code   string
	Detail string
}

// TargetPortfolio owns its weights and identity; all getters return copies.
type TargetPortfolio struct {
	spec    PortfolioSpec
	targets map[int32]float64
	id      string
}

func NewTargetPortfolio(spec PortfolioSpec, targets map[int32]float64) (*TargetPortfolio, error) {
	if spec.Mode == "" {
		spec.Mode = Full
	}
	if spec.StrategyID == "" || spec.AccountID == "" || spec.SnapshotID == "" || spec.PlanHash == "" || spec.FactorPlanHash == "" || spec.UniverseVersion == "" || spec.Budget.Version == "" || spec.Budget.Currency == "" || spec.PlanSequence == 0 || spec.PlanSequence > math.MaxInt64 {
		return nil, errors.New("factor: incomplete portfolio identity")
	}
	if spec.DecisionTime <= 0 || spec.ExecutableAt <= spec.DecisionTime || spec.ExpireAt < spec.ExecutableAt {
		return nil, errors.New("factor: invalid portfolio execution window")
	}
	if spec.Mode != Full && spec.Mode != Patch {
		return nil, errors.New("factor: invalid portfolio mode")
	}
	if spec.Budget.NAV <= 0 || math.IsNaN(spec.Budget.NAV) || math.IsInf(spec.Budget.NAV, 0) {
		return nil, errors.New("factor: frozen strategy NAV must be finite and positive")
	}
	for sid, w := range targets {
		if sid <= 0 || math.IsNaN(w) || math.IsInf(w, 0) {
			return nil, errors.New("factor: invalid portfolio weight")
		}
	}
	spec.Diagnostics = slices.Clone(spec.Diagnostics)
	p := &TargetPortfolio{spec: spec, targets: maps.Clone(targets)}
	var err error
	p.id, err = contentHash(struct {
		Spec    PortfolioSpec
		Targets map[int32]float64
	}{spec, p.targets})
	return p, err
}
func (p *TargetPortfolio) Spec() PortfolioSpec {
	s := p.spec
	s.Diagnostics = slices.Clone(s.Diagnostics)
	return s
}
func (p *TargetPortfolio) Targets() map[int32]float64 { return maps.Clone(p.targets) }
func (p *TargetPortfolio) ID() string                 { return p.id }
func (p *TargetPortfolio) Notional(sid int32) float64 { return p.targets[sid] * p.spec.Budget.NAV }

// EffectiveTargets applies Full/Patch only to this account and strategy's
// prior effective scope. Pass the previously applied effective portfolio.
func (p *TargetPortfolio) EffectiveTargets(previous *TargetPortfolio) (map[int32]float64, error) {
	result := make(map[int32]float64)
	if previous != nil {
		if previous.spec.StrategyID != p.spec.StrategyID || previous.spec.AccountID != p.spec.AccountID {
			return nil, errors.New("factor: portfolio owner mismatch")
		}
		if previous.spec.Budget.Currency != p.spec.Budget.Currency {
			return nil, errors.New("factor: portfolio budget currency changed")
		}
		if previous.spec.PlanSequence >= p.spec.PlanSequence {
			return nil, errors.New("factor: stale portfolio sequence")
		}
		for sid, w := range previous.targets {
			if p.spec.Mode == Full {
				result[sid] = 0
			} else {
				result[sid] = w
			}
		}
	}
	for sid, w := range p.targets {
		result[sid] = w
	}
	return result, nil
}

// TopBottomK chooses disjoint tails of eligible scores, with stable SID ties.
// Invalid scores never shrink the declared universe. A skipped decision emits
// a diagnostic and no replacement portfolio, preserving existing positions.
func TopBottomK(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int) (*TargetPortfolio, []Diagnostic, error) {
	return TopBottomKNotional(frame, scoreName, universe, spec, k, .5, .5)
}

// TopBottomKNotional records explicit strategy NAV fractions for each tail.
func TopBottomKNotional(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int, longNotional, shortNotional float64) (*TargetPortfolio, []Diagnostic, error) {
	if longNotional < 0 || shortNotional < 0 || longNotional+shortNotional <= 0 || math.IsNaN(longNotional+shortNotional) || math.IsInf(longNotional+shortNotional, 0) {
		return nil, nil, errors.New("factor: invalid portfolio notional fractions")
	}
	if k <= 0 {
		return nil, nil, errors.New("factor: top/bottom K must be positive")
	}
	if frame.SnapshotID != spec.SnapshotID || frame.PlanHash != spec.FactorPlanHash || frame.DecisionTime != spec.DecisionTime || universe.Version != spec.UniverseVersion {
		return nil, nil, errors.New("factor: portfolio/frame identity mismatch")
	}
	tradable := make(map[int32]bool, len(universe.Tradable))
	for _, sid := range universe.Tradable {
		tradable[sid] = true
	}
	type ranked struct {
		sid   int32
		score float64
	}
	var points []ranked
	for _, sid := range sortedSIDs(universe.Investable) {
		n, ok := frame.Values[scoreName][sid]
		if tradable[sid] && ok && n.Validity == Valid && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
			points = append(points, ranked{sid, n.Value})
		}
	}
	if len(points) < 2*k {
		return nil, []Diagnostic{{"insufficient-scores", "fewer than 2*K valid investable and tradable scores"}}, nil
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].score != points[j].score {
			return points[i].score < points[j].score
		}
		return points[i].sid < points[j].sid
	})
	if points[0].score == points[len(points)-1].score {
		return nil, []Diagnostic{{"constant-scores", "all eligible scores equal; keep previous portfolio"}}, nil
	}
	weights := make(map[int32]float64, 2*k)
	w := shortNotional / float64(k)
	for _, p := range points[:k] {
		weights[p.sid] = -w
	}
	// Stable SID ascending at both tails, without overlapping the short tail.
	longs := slices.Clone(points[k:])
	sort.Slice(longs, func(i, j int) bool {
		if longs[i].score != longs[j].score {
			return longs[i].score > longs[j].score
		}
		return longs[i].sid < longs[j].sid
	})
	for _, p := range longs[:k] {
		weights[p.sid] = longNotional / float64(k)
	}
	p, err := NewTargetPortfolio(spec, weights)
	return p, nil, err
}
