package runner

import (
	"slices"
	"sort"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/orm"
)

func (c Config) EffectiveRollingUniverse() *factor.RollingUniverseConfig {
	if c.RollingUniverse != nil {
		return c.RollingUniverse
	}
	return c.Manifest.Portfolio.Universe
}

func newRunnerRollingUniverse(c Config, plan *factor.Plan) (*factor.RollingUniverseSelector, error) {
	raw := c.EffectiveRollingUniverse()
	if raw == nil || !raw.Enabled() {
		return nil, nil
	}
	inputs := plan.Inputs()
	volSource, volTF := "", ""
	for _, in := range inputs {
		if in.Source == orm.SeriesSourceKline && !in.AsOfLatest {
			volSource, volTF = in.Source, in.TimeFrame
			break
		}
	}
	if volSource == "" && len(inputs) > 0 {
		volSource, volTF = inputs[0].Source, inputs[0].TimeFrame
	}
	return factor.NewRollingUniverseSelector(*raw, c.Snapshot.Universe, c.Snapshot.SIDMap, c.DecisionInterval, volSource, volTF)
}

func seedArchiveRollingUniverse(c Config, chunk Chunk, rolling *factor.RollingUniverseSelector) error {
	if rolling == nil || c.HistoricalInput != nil || chunk.Path == "" {
		return nil
	}
	store, err := factor.OpenVersionStore(chunk.Path, c.MaxRecords)
	if err != nil {
		return err
	}
	rows, err := store.Records()
	if err != nil {
		return err
	}
	var pre []factor.VersionRecord
	cutoff := chunk.From + c.DecisionDelayMS
	for _, r := range rows {
		if r.EventTime > 0 && r.EventTime < chunk.From && r.AvailableAt <= cutoff && (c.Snapshot.ReplayTime == 0 || r.IngestedAt <= cutoff) {
			pre = append(pre, r)
		}
	}
	if len(pre) == 0 {
		return nil
	}
	sortHistoricalRecords(pre)
	rolling.Observe(pre, chunk.From, cutoff, c.Snapshot.ReplayTime)
	return nil
}

// rollingAliveSIDs returns the candidate SIDs whose required plan inputs are present,
// closed and within declared age at (grid, now, replay).
func rollingAliveSIDs(plan *factor.Plan, candidates []int32, visible []factor.VersionRecord, grid, now, replay int64) map[int32]bool {
	inputs := plan.Inputs()
	if len(inputs) == 0 {
		alive := make(map[int32]bool, len(candidates))
		for _, sid := range candidates {
			alive[sid] = true
		}
		return alive
	}
	candSet := make(map[int32]bool, len(candidates))
	for _, sid := range candidates {
		candSet[sid] = true
	}
	latest := make(map[factor.StreamKey]factor.VersionRecord, len(visible))
	for _, row := range visible {
		if !candSet[row.Series.Sid] {
			continue
		}
		if row.EventTime > grid || row.AvailableAt > now || (replay != 0 && row.IngestedAt > replay) {
			continue
		}
		key := factor.StreamKey{SID: row.Series.Sid, Source: row.Series.Source, TimeFrame: row.Series.TimeFrame}
		prev, ok := latest[key]
		if !ok || row.EventTime > prev.EventTime || (row.EventTime == prev.EventTime && row.Revision > prev.Revision) {
			latest[key] = row
		}
	}
	alive := make(map[int32]bool, len(candidates))
	for _, sid := range candidates {
		okAll := true
		for _, in := range inputs {
			row, ok := latest[factor.StreamKey{SID: sid, Source: in.Source, TimeFrame: in.TimeFrame}]
			if !ok {
				okAll = false
				break
			}
			if in.AsOfLatest {
				if row.EventTime > grid || (in.MaxAge > 0 && grid-row.EventTime > in.MaxAge) {
					okAll = false
					break
				}
			} else if row.EventTime != grid || !row.Series.Closed {
				okAll = false
				break
			}
		}
		if okAll {
			alive[sid] = true
		}
	}
	return alive
}

// rollingDeadHeldSIDs identifies instruments that are outside the current Investable set,
// lack visible data at the current grid (delisted/halted), and still carry open book/target state.
func rollingDeadHeldSIDs(book *backtest.Book, pending, executed *factor.TargetPortfolio, policy *policyRun, investable []int32, alive map[int32]bool) []int32 {
	if book == nil {
		return nil
	}
	invSet := make(map[int32]bool, len(investable))
	for _, sid := range investable {
		invSet[sid] = true
	}
	dead := map[int32]bool{}
	check := func(sid int32) {
		if !invSet[sid] && !alive[sid] {
			dead[sid] = true
		}
	}
	for sid, qty := range book.State().Quantities {
		if qty != 0 {
			check(sid)
		}
	}
	if pending != nil {
		for sid := range pending.Targets() {
			check(sid)
		}
	}
	if executed != nil {
		for sid, w := range executed.Targets() {
			if w != 0 {
				check(sid)
			}
		}
	}
	if policy != nil && policy.previous != nil {
		for sid, a := range policy.previous.Allocations() {
			if a.Value != "" && a.Value != "0" {
				check(sid)
			}
		}
	}
	if len(dead) == 0 {
		return nil
	}
	out := make([]int32, 0, len(dead))
	for sid := range dead {
		out = append(out, sid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ensureRollingGracefulExit ensures non-policy portfolios cleanly liquidate any held instrument
// that rotated out of Universe.Investable, even under Patch mode or when selection returned nil.
func ensureRollingGracefulExit(p *factor.TargetPortfolio, executed *factor.TargetPortfolio, book *backtest.Book, universe factor.Universe, spec factor.PortfolioSpec) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
	inv := make(map[int32]bool, len(universe.Investable))
	for _, sid := range universe.Investable {
		inv[sid] = true
	}
	heldDropped := map[int32]bool{}
	if executed != nil {
		for sid, w := range executed.Targets() {
			if w != 0 && !inv[sid] {
				heldDropped[sid] = true
			}
		}
	}
	if book != nil {
		for sid, qty := range book.State().Quantities {
			if qty != 0 && !inv[sid] {
				heldDropped[sid] = true
			}
		}
	}
	if len(heldDropped) == 0 {
		return p, nil, nil
	}
	if p != nil {
		ps := p.Spec()
		if ps.Mode == factor.Full {
			// Full mode EffectiveTargets already zeroes every omitted previous holding.
			// If book held a position not in executed, include an explicit 0 only when needed.
			if executed != nil {
				ex := executed.Targets()
				needExplicit := false
				for sid := range heldDropped {
					if _, inExec := ex[sid]; !inExec {
						needExplicit = true
						break
					}
				}
				if !needExplicit {
					return p, nil, nil
				}
			} else {
				return p, nil, nil
			}
		}
		targets := p.Targets()
		changed := false
		for sid := range heldDropped {
			if w, exists := targets[sid]; !exists || w != 0 {
				targets[sid] = 0
				changed = true
			}
		}
		if !changed {
			return p, nil, nil
		}
		next, err := factor.NewTargetPortfolio(ps, targets)
		return next, []factor.Diagnostic{{Code: "rolling-universe-exit", Detail: "zeroed holdings removed from rolling investable universe"}}, err
	}
	// p == nil (e.g. insufficient-scores or constant-scores): preserve still-investable targets while zeroing dropped ones.
	targets := map[int32]float64{}
	if spec.Mode == factor.Patch {
		for sid := range heldDropped {
			targets[sid] = 0
		}
	} else if executed != nil {
		for sid, w := range executed.Targets() {
			if inv[sid] && w != 0 {
				targets[sid] = w
			} else if heldDropped[sid] {
				targets[sid] = 0
			}
		}
	} else {
		for sid := range heldDropped {
			targets[sid] = 0
		}
	}
	next, err := factor.NewTargetPortfolio(spec, targets)
	if err != nil {
		return nil, nil, err
	}
	return next, []factor.Diagnostic{{Code: "rolling-universe-exit", Detail: "liquidating holdings removed from rolling investable universe"}}, nil
}

func sameSIDSlice(a, b []int32) bool {
	return slices.Equal(a, b)
}
