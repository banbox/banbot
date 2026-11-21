package factor

import (
	"errors"
	"math"
	"sort"
)

type rankedAsset struct {
	sid   int32
	score float64
}

func policyScores(frame Frame, universe Universe, scoreName string) []rankedAsset {
	tradable := map[int32]bool{}
	for _, sid := range universe.Tradable {
		tradable[sid] = true
	}
	var points []rankedAsset
	for _, sid := range sortedSIDs(universe.Investable) {
		n, ok := frame.Values[scoreName][sid]
		if ok && tradable[sid] && n.Validity == Valid && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
			points = append(points, rankedAsset{sid, n.Value})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].score != points[j].score {
			return points[i].score > points[j].score
		}
		return points[i].sid < points[j].sid
	})
	return points
}

// SelectPortfolio handles independent sides; lifecycle retention is applied by
// Propose, where accepted holdings and visible grouping evidence are available.
func SelectPortfolio(frame Frame, universe Universe, spec PortfolioSpec, c PortfolioPolicyConfig) (*TargetPortfolio, []Diagnostic, error) {
	return SelectPortfolioScore(frame, universe, spec, c, "score", nil)
}
func SelectPortfolioScore(frame Frame, universe Universe, spec PortfolioSpec, c PortfolioPolicyConfig, scoreName string, groups map[int32]string) (*TargetPortfolio, []Diagnostic, error) {
	return SelectPortfolioScoreCooldown(frame, universe, spec, c, scoreName, groups, nil)
}

func SelectPortfolioScoreCooldown(frame Frame, universe Universe, spec PortfolioSpec, c PortfolioPolicyConfig, scoreName string, groups map[int32]string, cooldown map[int32]bool) (*TargetPortfolio, []Diagnostic, error) {
	if c.Selection.MissingScores == "" {
		c.Selection.MissingScores = "skip"
	}
	if frame.SnapshotID != spec.SnapshotID || frame.PlanHash != spec.FactorPlanHash || frame.DecisionTime != spec.DecisionTime || universe.Version != spec.UniverseVersion {
		return nil, nil, errors.New("factor: selector identity mismatch")
	}
	points := policyScores(frame, universe, scoreName)
	lk, sk := c.Selection.LongK, c.Selection.ShortK
	if c.Selection.LongQuantile > 0 {
		lk = int(math.Ceil(float64(len(points)) * c.Selection.LongQuantile))
	}
	if c.Selection.ShortQuantile > 0 {
		sk = int(math.Ceil(float64(len(points)) * c.Selection.ShortQuantile))
	}
	if c.LongNotional == 0 {
		lk = 0
	}
	if c.ShortNotional == 0 {
		sk = 0
	}
	if len(points) == 0 {
		return nil, []Diagnostic{{"insufficient-scores", "ordinary selection skipped; missing/constant scores do not become dropout"}}, nil
	}
	if lk == 0 && sk == 0 {
		return NewTargetPortfolioWithDiagnostics(spec, map[int32]float64{}, []Diagnostic{{"exposure-scaled-zero", "side budgets scaled to zero"}})
	}
	allGatedOut := false
	if len(points) > 1 && points[0].score == points[len(points)-1].score {
		s := points[0].score
		longOut := lk == 0 || c.Selection.LongMinScore != nil && s < *c.Selection.LongMinScore
		shortOut := sk == 0 || c.Selection.ShortMaxScore != nil && s > *c.Selection.ShortMaxScore
		allGatedOut = longOut && shortOut
	}
	if len(points) > 1 && points[0].score == points[len(points)-1].score && !allGatedOut || len(points) < lk+sk && c.Selection.MissingScores == "skip" {
		return nil, []Diagnostic{{"insufficient-scores", "ordinary selection skipped; missing/constant scores do not become dropout"}}, nil
	}
	if len(points) < lk+sk && c.Selection.MissingScores == "cash" {
		return NewTargetPortfolioWithDiagnostics(spec, map[int32]float64{}, []Diagnostic{{"insufficient-scores", "explicit cash selection"}})
	}
	weights := map[int32]float64{}
	var diagnostics []Diagnostic
	used := map[int32]bool{}
	selectSide := func(count int, reverse bool, budget float64, minScore, maxScore *float64) {
		if count <= 0 || budget == 0 {
			return
		}
		ordered := append([]rankedAsset(nil), points...)
		if reverse {
			sort.Slice(ordered, func(i, j int) bool {
				if ordered[i].score != ordered[j].score {
					return ordered[i].score < ordered[j].score
				}
				return ordered[i].sid < ordered[j].sid
			})
		}
		chosen := []int32{}
		groupCount := map[string]int{}
		for i := 0; i < len(ordered) && len(chosen) < count; i++ {
			sid := ordered[i].sid
			score := ordered[i].score
			if !reverse && minScore != nil && score < *minScore {
				break
			}
			if reverse && maxScore != nil && score > *maxScore {
				break
			}
			if used[sid] || cooldown[sid] {
				continue
			}
			group := groups[sid]
			if cap, ok := c.Selection.GroupQuota[group]; ok && groupCount[group] >= cap {
				continue
			}
			chosen = append(chosen, sid)
			used[sid] = true
			groupCount[group]++
		}
		if len(chosen) > 0 {
			denom := float64(len(chosen))
			hasGate := (!reverse && minScore != nil) || (reverse && maxScore != nil)
			if c.Selection.UnfilledSlotPolicy == "cash" || (c.Selection.UnfilledSlotPolicy == "" && hasGate) {
				denom = float64(count)
			}
			w := budget / denom
			if c.Allocation.AssetCap > 0 && math.Abs(w) > c.Allocation.AssetCap {
				w = math.Copysign(c.Allocation.AssetCap, w)
			}
			for _, sid := range chosen {
				weights[sid] = w
			}
		}
		if len(chosen) < count {
			diagnostics = append(diagnostics, Diagnostic{"selection-underfilled", "eligible group quotas, score threshold or shortened pool cannot fill requested side count"})
		}
	}
	selectSide(lk, false, c.LongNotional, c.Selection.LongMinScore, nil)
	selectSide(sk, true, -c.ShortNotional, nil, c.Selection.ShortMaxScore)
	p, err := NewTargetPortfolio(spec, weights)
	return p, diagnostics, err
}
func NewTargetPortfolioWithDiagnostics(spec PortfolioSpec, weights map[int32]float64, diags []Diagnostic) (*TargetPortfolio, []Diagnostic, error) {
	p, e := NewTargetPortfolio(spec, weights)
	return p, diags, e
}

// AllocateSelected applies deterministic side budgets and visible score/risk
// data. No missing volatility is silently treated as zero risk.
func AllocateSelected(selected map[int32]float64, c PortfolioPolicyConfig, nav float64, scores, vol map[int32]float64) (map[int32]float64, error) {
	ctx := PortfolioContext{
		Spec:       PortfolioSpec{Budget: FrozenBudget{NAV: nav}},
		Volatility: vol,
	}
	return AllocateSelectedContext(selected, c, ctx, scores)
}

// AllocateSelectedContext extends AllocateSelected with covariance/return history
// and solver hooks for ERC (risk-parity), HRP, min-variance, and mean-variance.
func AllocateSelectedContext(selected map[int32]float64, c PortfolioPolicyConfig, ctx PortfolioContext, scores map[int32]float64) (map[int32]float64, error) {
	nav := ctx.Spec.Budget.NAV
	vol := ctx.Volatility
	method := c.Allocation.Method
	if method == "" {
		method = "equal"
	}
	if method == "risk-parity" {
		method = "erc"
	}
	result := map[int32]float64{}
	for _, sign := range []float64{1, -1} {
		budget := c.LongNotional
		hasGate := c.Selection.LongMinScore != nil
		if sign < 0 {
			budget = c.ShortNotional
			hasGate = c.Selection.ShortMaxScore != nil
		}
		if c.Selection.UnfilledSlotPolicy == "cash" || (c.Selection.UnfilledSlotPolicy == "" && hasGate) {
			selectedSum := 0.0
			for _, sid := range mapSIDs(selected) {
				if w := selected[sid]; w*sign > 0 {
					selectedSum += math.Abs(w)
				}
			}
			if selectedSum < budget {
				budget = selectedSum
			}
		}
		budget *= 1 - c.Allocation.ReserveRatio
		var chosen []int32
		for _, sid := range mapSIDs(selected) {
			if selected[sid]*sign > 0 {
				chosen = append(chosen, sid)
			}
		}
		if len(chosen) == 0 {
			continue
		}
		if c.Allocation.IsCovarianceMethod() {
			weights, err := allocateSideCovariance(chosen, sign, method, c.Allocation, ctx, scores)
			if err != nil {
				return nil, err
			}
			for i, sid := range chosen {
				value := budget * weights[i]
				if c.Allocation.AssetCap > 0 {
					value = math.Min(value, c.Allocation.AssetCap)
				}
				result[sid] = sign * value
			}
			continue
		}
		raw := map[int32]float64{}
		total := 0.0
		fallbackEqual := false
		if (method == "inverse-volatility" || method == "vol-target") && ctx.AutoVolatilityFallback {
			for _, sid := range chosen {
				sigma := vol[sid]
				if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
					fallbackEqual = true
					break
				}
			}
		}
		for _, sid := range chosen {
			v := 1.0
			switch method {
			case "score":
				v = math.Abs(scores[sid])
			case "inverse-volatility", "vol-target":
				if !fallbackEqual {
					sigma := vol[sid]
					if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
						return nil, errors.New("factor: allocation requires positive visible volatility")
					}
					v = 1 / sigma
				}
			case "fixed-notional":
				v = c.Allocation.FixedNotional / nav
			}
			raw[sid] = v
			total += v
		}
		for _, sid := range chosen {
			value := 0.0
			if total > 0 {
				value = budget * raw[sid] / total
			}
			if method == "fixed-notional" {
				value = math.Min(value, raw[sid])
			}
			if method == "vol-target" && !fallbackEqual {
				value = math.Min(value, c.Allocation.VolTarget*raw[sid]/float64(len(raw)))
			}
			if c.Allocation.AssetCap > 0 {
				value = math.Min(value, c.Allocation.AssetCap)
			}
			result[sid] = sign * value
		}
	}
	return result, nil
}
func mapSIDs[V any](m map[int32]V) []int32 {
	ids := make([]int32, 0, len(m))
	for sid := range m {
		ids = append(ids, sid)
	}
	return sortedSIDs(ids)
}
