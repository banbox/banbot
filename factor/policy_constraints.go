package factor

import (
	"fmt"
	"math"
	"slices"
	"sort"
)

func (p *LifecyclePolicy) applyRetention(c PortfolioContext, s LifecycleState, desired map[int32]float64, cfg PortfolioPolicyConfig) {
	points := policyScores(c.Frame, c.Universe, c.ScoreName)
	rankLong, rankShort := map[int32]int{}, map[int32]int{}
	scoreBySID := map[int32]float64{}
	for i, v := range points {
		rankLong[v.sid] = i + 1
		scoreBySID[v.sid] = v.score
	}
	shortPoints := slices.Clone(points)
	sort.Slice(shortPoints, func(i, j int) bool {
		if shortPoints[i].score != shortPoints[j].score {
			return shortPoints[i].score < shortPoints[j].score
		}
		return shortPoints[i].sid < shortPoints[j].sid
	})
	for i, v := range shortPoints {
		rankShort[v.sid] = i + 1
	}
	passesGate := func(sid int32, direction int) bool {
		if direction > 0 && cfg.Selection.LongMinScore != nil {
			return scoreBySID[sid] >= *cfg.Selection.LongMinScore
		}
		if direction < 0 && cfg.Selection.ShortMaxScore != nil {
			return scoreBySID[sid] <= *cfg.Selection.ShortMaxScore
		}
		return true
	}
	retained := map[int32]bool{}
	for _, sid := range mapSIDs(s.Assets) {
		a := s.Assets[sid]
		if a == nil || c.GridTime < a.CooldownUntil || a.Forced {
			continue
		}
		rank := rankLong[sid]
		budget := cfg.LongNotional
		k := cfg.Selection.LongK
		if a.Direction < 0 {
			rank = rankShort[sid]
			budget = cfg.ShortNotional
			k = cfg.Selection.ShortK
		}
		if rank > 0 && budget > 0 && passesGate(sid, a.Direction) && cfg.Selection.RetainRank > 0 && rank <= cfg.Selection.RetainRank && !a.Exiting && a.Last.Value != "0" {
			if _, ok := desired[sid]; !ok && k > 0 {
				desired[sid] = float64(a.Direction) * budget / float64(k)
				retained[sid] = true
			}
		}
	}
	if cfg.Selection.Dropout > 0 {
		for _, side := range []int{1, -1} {
			var excluded []int32
			rank := rankLong
			sideBudget := cfg.LongNotional
			if side < 0 {
				rank = rankShort
				sideBudget = cfg.ShortNotional
			}
			if sideBudget <= 0 {
				continue
			}
			for _, sid := range mapSIDs(s.Assets) {
				a := s.Assets[sid]
				if a == nil || c.GridTime < a.CooldownUntil || a.Forced {
					continue
				}
				if _, ok := desired[sid]; ok || a.Direction != side || a.Last.Value == "0" || a.Exiting || rank[sid] == 0 || !passesGate(sid, side) {
					continue
				}
				excluded = append(excluded, sid)
			}
			sort.Slice(excluded, func(i, j int) bool {
				if rank[excluded[i]] != rank[excluded[j]] {
					return rank[excluded[i]] > rank[excluded[j]]
				}
				return excluded[i] < excluded[j]
			})
			for index, sid := range excluded {
				a := s.Assets[sid]
				if index < cfg.Selection.Dropout {
					continue
				}
				w, e := allocationWeight(a.Last, c, sid)
				if e == nil {
					desired[sid] = w
					retained[sid] = true
				}
			}
		}
	}
	// Retained incumbents consume selector slots, replacing the weakest new SID.
	for _, side := range []int{1, -1} {
		limit := cfg.Selection.LongK
		if side < 0 {
			limit = cfg.Selection.ShortK
		}
		if limit <= 0 {
			continue
		}
		var candidates []int32
		count := 0
		for sid, w := range desired {
			if signed(w) == side {
				count++
				if !retained[sid] && s.Assets[sid] == nil {
					candidates = append(candidates, sid)
				}
			}
		}
		sortByRank := func(i, j int) bool {
			r1, r2 := rankLong[candidates[i]], rankLong[candidates[j]]
			if side < 0 {
				r1, r2 = rankShort[candidates[i]], rankShort[candidates[j]]
			}
			if r1 != r2 {
				return r1 > r2
			}
			return candidates[i] > candidates[j]
		}
		sort.Slice(candidates, sortByRank)
		for _, sid := range candidates {
			if count <= limit {
				break
			}
			delete(desired, sid)
			count--
		}
	}
}
func (p *LifecyclePolicy) allocateRemainder(c PortfolioContext, s LifecycleState, desired map[int32]float64, targets map[int32]Allocation, hard map[int32]bool, reasons *[]Diagnostic, cfg PortfolioPolicyConfig) {
	for _, side := range []int{1, -1} {
		budget := cfg.LongNotional
		if side < 0 {
			budget = cfg.ShortNotional
		}
		budget *= 1 - cfg.Allocation.ReserveRatio
		tail := 0.0
		for _, sid := range mapSIDs(targets) {
			if _, selected := desired[sid]; selected {
				continue
			}
			w, e := allocationWeight(targets[sid], c, sid)
			if e == nil && signed(w) == side {
				tail += math.Abs(w)
			}
		}
		available := math.Max(0, budget-tail)
		total := 0.0
		for _, sid := range mapSIDs(desired) {
			if signed(desired[sid]) == side && !hard[sid] && !(s.Assets[sid] != nil && c.GridTime < s.Assets[sid].CooldownUntil) {
				total += math.Abs(desired[sid])
			}
		}
		scale := 1.0
		if total > available && total > 0 {
			scale = available / total
			*reasons = append(*reasons, Diagnostic{"tail-budget", fmt.Sprintf("side %d tail %.6g leaves %.6g NAV fraction", side, tail, available)})
		}
		for _, sid := range mapSIDs(desired) {
			w := desired[sid]
			if signed(w) != side || hard[sid] {
				continue
			}
			if a := s.Assets[sid]; a != nil && c.GridTime < a.CooldownUntil {
				targets[sid] = Allocation{AbsoluteQuantity, "0"}
				continue
			}
			targets[sid] = Allocation{NAVFraction, floatDecimal(w * scale)}
		}
	}
}
func (p *LifecyclePolicy) constrain(c PortfolioContext, s LifecycleState, targets map[int32]Allocation, hard map[int32]bool, reasons *[]Diagnostic, cfg PortfolioPolicyConfig) error {
	weights := map[int32]float64{}
	for _, sid := range mapSIDs(targets) {
		w, err := allocationWeight(targets[sid], c, sid)
		if err != nil {
			return err
		}
		weights[sid] = w
		if w != 0 && len(cfg.Allocation.GroupCaps) > 0 {
			if _, ok := c.Groups[sid]; !ok {
				return fmt.Errorf("factor: group caps require visible group for SID %d", sid)
			}
		}
		if w != 0 && cfg.Allocation.BetaCap > 0 {
			beta, ok := c.Beta[sid]
			if !ok || math.IsNaN(beta) || math.IsInf(beta, 0) {
				return fmt.Errorf("factor: beta cap requires visible finite beta for SID %d", sid)
			}
		}
	}
	applyScale := func(sids []int32, scale float64, reason string) error {
		if scale >= 1 {
			return nil
		}
		for _, sid := range sids {
			a := targets[sid]
			if a.Basis == NAVFraction {
				a.Value = floatDecimal(weights[sid] * scale)
			} else {
				q, err := ScaleQuantity(floatDecimal(mustQuantity(a.Value)*scale), 1, 1, c.Positions[sid].Quantum)
				if err != nil {
					return err
				}
				a.Value = q
			}
			targets[sid] = a
			weights[sid] *= scale
		}
		*reasons = append(*reasons, Diagnostic{"constraint-scale", reason})
		return nil
	}
	if cap := cfg.Allocation.AssetCap; cap > 0 {
		for _, sid := range mapSIDs(weights) {
			if math.Abs(weights[sid]) > cap {
				if err := applyScale([]int32{sid}, cap/math.Abs(weights[sid]), fmt.Sprintf("SID %d asset cap", sid)); err != nil {
					return err
				}
			}
		}
	}
	for _, side := range []int{1, -1} {
		limit := cfg.LongNotional
		if side < 0 {
			limit = cfg.ShortNotional
		}
		limit *= 1 - cfg.Allocation.ReserveRatio
		total := 0.0
		var ids []int32
		for _, sid := range mapSIDs(weights) {
			if signed(weights[sid]) == side {
				ids = append(ids, sid)
				total += math.Abs(weights[sid])
			}
		}
		if total > limit {
			if err := applyScale(ids, limit/total, "side budget includes protected and exit quantities"); err != nil {
				return err
			}
		}
	}
	groupNames := make([]string, 0, len(cfg.Allocation.GroupCaps))
	for group := range cfg.Allocation.GroupCaps {
		groupNames = append(groupNames, group)
	}
	slices.Sort(groupNames)
	for _, group := range groupNames {
		cap := cfg.Allocation.GroupCaps[group]
		total := 0.0
		var ids []int32
		for _, sid := range mapSIDs(weights) {
			if c.Groups[sid] == group {
				ids = append(ids, sid)
				total += math.Abs(weights[sid])
			}
		}
		if total > cap {
			if err := applyScale(ids, cap/total, "group cap "+group); err != nil {
				return err
			}
		}
	}
	net, beta := 0.0, 0.0
	for _, sid := range mapSIDs(weights) {
		net += weights[sid]
		beta += weights[sid] * c.Beta[sid]
	}
	scale := 1.0
	if cfg.Allocation.NetCap > 0 && math.Abs(net) > cfg.Allocation.NetCap {
		scale = math.Min(scale, cfg.Allocation.NetCap/math.Abs(net))
	}
	if cfg.Allocation.BetaCap > 0 && math.Abs(beta) > cfg.Allocation.BetaCap {
		scale = math.Min(scale, cfg.Allocation.BetaCap/math.Abs(beta))
	}
	if err := applyScale(mapSIDs(weights), scale, "net/beta cap"); err != nil {
		return err
	}
	if c.CapitalLimit > 0 {
		gross := 0.0
		for _, sid := range mapSIDs(weights) {
			gross += math.Abs(weights[sid])
		}
		limit := c.CapitalLimit / c.Spec.Budget.NAV
		if gross > limit {
			if err := applyScale(mapSIDs(weights), limit/gross, "strategy capital limit"); err != nil {
				return err
			}
		}
	}
	if limit := cfg.Allocation.TurnoverLimit; limit > 0 {
		previous := map[int32]float64{}
		previousAllocations := c.previousAllocations
		if c.Previous != nil {
			previousAllocations = c.Previous.Allocations()
		}
		for _, sid := range mapSIDs(previousAllocations) {
			w, err := allocationWeight(previousAllocations[sid], c, sid)
			if err != nil {
				return err
			}
			previous[sid] = w
		}
		turnover := 0.0
		for _, sid := range mapSIDs(weights) {
			if !hard[sid] {
				turnover += .5 * math.Abs(weights[sid]-previous[sid])
			}
		}
		if turnover > limit {
			ratio := limit / turnover
			for _, sid := range mapSIDs(weights) {
				if hard[sid] {
					continue
				}
				next := previous[sid] + ratio*(weights[sid]-previous[sid])
				a := targets[sid]
				if a.Basis == NAVFraction {
					a.Value = floatDecimal(next)
				} else {
					mark := c.Marks[sid]
					q, err := ScaleQuantity(floatDecimal(next*c.Spec.Budget.NAV/mark), 1, 1, c.Positions[sid].Quantum)
					if err != nil {
						return err
					}
					if life := s.Assets[sid]; life != nil && life.Exiting {
						quantity := c.Positions[sid].Quantity
						if quantity == "" {
							quantity = "0"
						}
						q, err = ClampQuantityMagnitude(q, quantity)
						if err != nil {
							return err
						}
					}
					a.Value = q
				}
				targets[sid] = a
			}
			*reasons = append(*reasons, Diagnostic{"turnover-limited", "ordinary estimated half-L1 turnover scaled; hard exits exempt"})
			// Turnover interpolation can revive a previously over-budget position.
			// Recheck risk limits after smoothing, with explicit conflict reporting.
			before := len(*reasons)
			riskCfg := ClonePortfolioPolicyConfig(cfg)
			riskCfg.Allocation.TurnoverLimit = 0
			if err := p.constrain(c, s, targets, hard, reasons, riskCfg); err != nil {
				return err
			}
			if len(*reasons) > before {
				*reasons = append(*reasons, Diagnostic{"constraint-infeasible", "turnover preference conflicts with risk caps; risk caps take precedence"})
			}
		}
	}
	return nil
}
