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

const (
	ExposureScaleOutput = "exposure_scale"
	LongScaleOutput     = "long_scale"
	ShortScaleOutput    = "short_scale"
	VolatilityOutput    = "volatility"
	BetaOutput          = "beta"
)

// IsExposureScaleOutput reports whether name is one of the reserved dynamic
// regime exposure scaling columns ("exposure_scale", "long_scale", "short_scale").
func IsExposureScaleOutput(name string) bool {
	return name == ExposureScaleOutput || name == LongScaleOutput || name == ShortScaleOutput
}

// IsRiskOutput reports whether name is one of the default reserved risk columns
// ("volatility", "beta") excluded from implicit equal-weight score combination.
func IsRiskOutput(name string) bool {
	return name == VolatilityOutput || name == BetaOutput
}

// IsReservedOutput reports whether name is a reserved exposure-scaling or risk column.
func IsReservedOutput(name string) bool {
	return IsExposureScaleOutput(name) || IsRiskOutput(name)
}

func frameScaleValue(frame Frame, universe Universe, name string) (float64, bool) {
	col, exists := frame.Values[name]
	if !exists {
		return 1, false
	}
	var sids []int32
	if len(universe.Reference) > 0 || len(universe.Investable) > 0 {
		combined := make([]int32, 0, len(universe.Reference)+len(universe.Investable))
		combined = append(combined, universe.Reference...)
		combined = append(combined, universe.Investable...)
		sids = sortedSIDs(combined)
	} else {
		sids = mapSIDs(col)
	}
	sum := 0.0
	count := 0
	for _, sid := range sids {
		n, ok := col[sid]
		if ok && n.Validity == Valid && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
			sum += n.Value
			count++
		}
	}
	if count == 0 {
		return 1, true
	}
	mean := sum / float64(count)
	return max(0.0, min(1.0, mean)), true
}

// ExposureScales extracts dynamic [0, 1] side notional multipliers from the
// reserved "exposure_scale", "long_scale" and "short_scale" frame columns.
// When all entries of a present column are invalid, its multiplier defaults to 1.0.
func ExposureScales(frame Frame, universe Universe) (longScale, shortScale float64, active bool) {
	longScale, shortScale = 1.0, 1.0
	if s, ok := frameScaleValue(frame, universe, ExposureScaleOutput); ok {
		longScale *= s
		shortScale *= s
		active = true
	}
	if ls, ok := frameScaleValue(frame, universe, LongScaleOutput); ok {
		longScale *= ls
		active = true
	}
	if ss, ok := frameScaleValue(frame, universe, ShortScaleOutput); ok {
		shortScale *= ss
		active = true
	}
	return longScale, shortScale, active
}

// ScoreGate configures optional absolute score thresholds and unfilled slot sizing.
// Pointer thresholds distinguish an omitted gate (nil) from an explicit 0.0 gate:
// long candidates require score >= *LongMinScore; short candidates require score <= *ShortMaxScore.
// UnfilledSlotPolicy defaults to "cash" (each passing candidate receives sideNotional / k,
// leaving unfilled slots in cash; 0 passing candidates emits an empty portfolio to exit to cash),
// or "rescale" to concentrate sideNotional across passing candidates (subject to AssetCap).
type ScoreGate struct {
	LongMinScore       *float64
	ShortMaxScore      *float64
	UnfilledSlotPolicy string
	AssetCap           float64
}

// TopBottomK chooses disjoint tails of eligible scores, with stable SID ties.
// Invalid scores never shrink the declared universe. A skipped decision emits
// a diagnostic and no replacement portfolio, preserving existing positions.
func TopBottomK(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int) (*TargetPortfolio, []Diagnostic, error) {
	return TopBottomKNotional(frame, scoreName, universe, spec, k, .5, .5)
}

// TopBottomKNotional records explicit strategy NAV fractions for each tail.
func TopBottomKNotional(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int, longNotional, shortNotional float64) (*TargetPortfolio, []Diagnostic, error) {
	return TopBottomKGated(frame, scoreName, universe, spec, k, longNotional, shortNotional, ScoreGate{})
}

// TopBottomKGated applies optional absolute score gates and unfilled slot sizing to TopBottomKNotional.
func TopBottomKGated(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int, longNotional, shortNotional float64, gate ScoreGate) (*TargetPortfolio, []Diagnostic, error) {
	return TopBottomKDynamic(frame, scoreName, universe, spec, k, longNotional, shortNotional, gate, false)
}

// TopBottomKDynamic is used when dynamic regime exposure scaling may scale a positive base budget to zero.
func TopBottomKDynamic(frame Frame, scoreName string, universe Universe, spec PortfolioSpec, k int, longNotional, shortNotional float64, gate ScoreGate, allowZeroTotal bool) (*TargetPortfolio, []Diagnostic, error) {
	if gate.LongMinScore != nil && (math.IsNaN(*gate.LongMinScore) || math.IsInf(*gate.LongMinScore, 0)) {
		return nil, nil, errors.New("factor: non-finite long_min_score")
	}
	if gate.ShortMaxScore != nil && (math.IsNaN(*gate.ShortMaxScore) || math.IsInf(*gate.ShortMaxScore, 0)) {
		return nil, nil, errors.New("factor: non-finite short_max_score")
	}
	if gate.UnfilledSlotPolicy != "" && gate.UnfilledSlotPolicy != "cash" && gate.UnfilledSlotPolicy != "rescale" {
		return nil, nil, errors.New("factor: unknown unfilled slot policy")
	}
	if gate.AssetCap < 0 || math.IsNaN(gate.AssetCap) || math.IsInf(gate.AssetCap, 0) {
		return nil, nil, errors.New("factor: invalid asset cap")
	}
	if longNotional < 0 || shortNotional < 0 || (!allowZeroTotal && longNotional+shortNotional <= 0) || math.IsNaN(longNotional+shortNotional) || math.IsInf(longNotional+shortNotional, 0) {
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
	gateActive := gate.LongMinScore != nil || gate.ShortMaxScore != nil || gate.UnfilledSlotPolicy != "" || gate.AssetCap > 0
	if !gateActive && !allowZeroTotal {
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
	if len(points) == 0 {
		return nil, []Diagnostic{{"insufficient-scores", "no valid investable and tradable scores"}}, nil
	}
	if longNotional == 0 && shortNotional == 0 {
		p, err := NewTargetPortfolio(spec, map[int32]float64{})
		return p, []Diagnostic{{"exposure-scaled-zero", "dynamic exposure scale set side budgets to zero"}}, err
	}
	need := 0
	if longNotional > 0 {
		need += k
	}
	if shortNotional > 0 {
		need += k
	}
	if len(points) < need {
		return nil, []Diagnostic{{"insufficient-scores", "fewer than required valid investable and tradable scores"}}, nil
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].score != points[j].score {
			return points[i].score < points[j].score
		}
		return points[i].sid < points[j].sid
	})
	if len(points) > 1 && points[0].score == points[len(points)-1].score {
		s := points[0].score
		longEligible := longNotional > 0 && (gate.LongMinScore == nil || s >= *gate.LongMinScore)
		shortEligible := shortNotional > 0 && (gate.ShortMaxScore == nil || s <= *gate.ShortMaxScore)
		if longEligible || shortEligible {
			return nil, []Diagnostic{{"constant-scores", "all eligible scores equal; keep previous portfolio"}}, nil
		}
	}
	weights := make(map[int32]float64, need)
	used := make(map[int32]bool, need)
	var diagnostics []Diagnostic
	if shortNotional > 0 {
		var chosenShort []int32
		for i := 0; i < len(points) && len(chosenShort) < k; i++ {
			p := points[i]
			if gate.ShortMaxScore != nil && p.score > *gate.ShortMaxScore {
				break
			}
			chosenShort = append(chosenShort, p.sid)
			used[p.sid] = true
		}
		if len(chosenShort) > 0 {
			denom := float64(k)
			if gate.UnfilledSlotPolicy == "rescale" {
				denom = float64(len(chosenShort))
			}
			w := shortNotional / denom
			if gate.AssetCap > 0 && w > gate.AssetCap {
				w = gate.AssetCap
			}
			for _, sid := range chosenShort {
				weights[sid] = -w
			}
		}
		if len(chosenShort) < k {
			diagnostics = append(diagnostics, Diagnostic{"selection-underfilled", "score threshold left short slots unfilled"})
		}
	}
	if longNotional > 0 {
		longs := make([]ranked, 0, len(points))
		for _, p := range points {
			if !used[p.sid] {
				longs = append(longs, p)
			}
		}
		sort.Slice(longs, func(i, j int) bool {
			if longs[i].score != longs[j].score {
				return longs[i].score > longs[j].score
			}
			return longs[i].sid < longs[j].sid
		})
		var chosenLong []int32
		for i := 0; i < len(longs) && len(chosenLong) < k; i++ {
			p := longs[i]
			if gate.LongMinScore != nil && p.score < *gate.LongMinScore {
				break
			}
			chosenLong = append(chosenLong, p.sid)
			used[p.sid] = true
		}
		if len(chosenLong) > 0 {
			denom := float64(k)
			if gate.UnfilledSlotPolicy == "rescale" {
				denom = float64(len(chosenLong))
			}
			w := longNotional / denom
			if gate.AssetCap > 0 && w > gate.AssetCap {
				w = gate.AssetCap
			}
			for _, sid := range chosenLong {
				weights[sid] = w
			}
		}
		if len(chosenLong) < k {
			diagnostics = append(diagnostics, Diagnostic{"selection-underfilled", "score threshold left long slots unfilled"})
		}
	}
	p, err := NewTargetPortfolio(spec, weights)
	return p, diagnostics, err
}
