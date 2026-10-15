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
	if len(points) == 0 || len(points) > 1 && points[0].score == points[len(points)-1].score || len(points) < lk+sk && c.Selection.MissingScores == "skip" {
		return nil, []Diagnostic{{"insufficient-scores", "ordinary selection skipped; missing/constant scores do not become dropout"}}, nil
	}
	if len(points) < lk+sk && c.Selection.MissingScores == "cash" {
		return NewTargetPortfolioWithDiagnostics(spec, map[int32]float64{}, []Diagnostic{{"insufficient-scores", "explicit cash selection"}})
	}
	weights := map[int32]float64{}
	var diagnostics []Diagnostic
	used := map[int32]bool{}
	selectSide := func(count int, reverse bool, budget float64) {
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
		for i := 0; i < len(points) && len(chosen) < count; i++ {
			sid := ordered[i].sid
			if used[sid] {
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
			for _, sid := range chosen {
				weights[sid] = budget / float64(len(chosen))
			}
		}
		if len(chosen) < count {
			diagnostics = append(diagnostics, Diagnostic{"selection-underfilled", "eligible group quotas or shortened pool cannot fill requested side count"})
		}
	}
	selectSide(lk, false, c.LongNotional)
	selectSide(sk, true, -c.ShortNotional)
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
	result := map[int32]float64{}
	for _, sign := range []float64{1, -1} {
		budget := c.LongNotional
		if sign < 0 {
			budget = c.ShortNotional
		}
		budget *= 1 - c.Allocation.ReserveRatio
		raw := map[int32]float64{}
		total := 0.0
		for _, sid := range mapSIDs(selected) {
			w := selected[sid]
			if w*sign <= 0 {
				continue
			}
			v := 1.0
			switch c.Allocation.Method {
			case "score":
				v = math.Abs(scores[sid])
			case "inverse-volatility", "vol-target":
				sigma := vol[sid]
				if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
					return nil, errors.New("factor: allocation requires positive visible volatility")
				}
				v = 1 / sigma
			case "fixed-notional":
				v = c.Allocation.FixedNotional / nav
			}
			raw[sid] = v
			total += v
		}
		for _, sid := range mapSIDs(raw) {
			value := 0.0
			if total > 0 {
				value = budget * raw[sid] / total
			}
			if c.Allocation.Method == "fixed-notional" {
				value = math.Min(value, raw[sid])
			}
			if c.Allocation.Method == "vol-target" {
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
