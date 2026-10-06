package factor

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
)

func cohortOrder(cohorts []PortfolioCohort) {
	sort.Slice(cohorts, func(i, j int) bool {
		if cohorts[i].Expires != cohorts[j].Expires {
			return cohorts[i].Expires < cohorts[j].Expires
		}
		return cohorts[i].ID < cohorts[j].ID
	})
}

// ReconcileCohortContributions distributes only observed aggregate fills.
// Expired contributions absorb reductions first; increases are allocated in
// proportion to accepted unfilled demand, including unresolved expired entry.
func ReconcileCohortContributions(cohorts []PortfolioCohort, positions map[int32]PositionEvidence, grid int64) error {
	cohortOrder(cohorts)
	sidSet := map[int32]bool{}
	for _, cohort := range cohorts {
		for _, x := range cohort.Contributions {
			sidSet[x.SID] = true
		}
	}
	for _, sid := range mapSIDs(sidSet) {
		actual, e := positionQuantity(positions[sid])
		if e != nil {
			return e
		}
		type ref struct {
			x *CohortContribution
			c *PortfolioCohort
		}
		var refs []ref
		sum := 0.0
		for i := range cohorts {
			for j := range cohorts[i].Contributions {
				x := &cohorts[i].Contributions[j]
				if x.SID == sid {
					refs = append(refs, ref{x, &cohorts[i]})
					sum += mustQuantity(x.Filled)
				}
			}
		}
		// The optional ledger evidence resolves multiple pending plans sharing
		// one aggregate lot. Entry fills are attributed to their original plan;
		// settled reductions still release earliest expiry first.
		events := append([]PositionFillEvidence(nil), positions[sid].FillEvents...)
		sort.SliceStable(events, func(i, j int) bool { return events[i].LedgerCursor < events[j].LedgerCursor })
		for _, event := range events {
			if event.PlanSequence == 0 {
				continue
			}
			amount, err := decimalFloat(event.Quantity)
			if err != nil {
				return err
			}
			if amount == 0 {
				continue
			}
			direction := signed(sum)
			if direction == 0 {
				for _, r := range refs {
					if direction = signed(mustQuantity(r.x.Planned)); direction != 0 {
						break
					}
				}
			}
			if amount*float64(direction) > 0 {
				demand := 0.0
				for _, r := range refs {
					if r.c.PlanSequence == event.PlanSequence || slices.Contains(r.x.EntrySequences, event.PlanSequence) {
						planned := r.x.Planned
						if original, ok := r.x.EntryPlanned[event.PlanSequence]; ok {
							planned = original
						}
						d := mustQuantity(planned) - mustQuantity(r.x.Filled)
						if d*float64(direction) > 0 {
							demand += math.Abs(d)
						}
					}
				}
				if demand+1e-10 < math.Abs(amount) {
					return fmt.Errorf("factor: origin fill exceeds original cohort demand: SID %d plan %d amount %.12g demand %.12g grid %d; reconciliation required", sid, event.PlanSequence, amount, demand, grid)
				}
				for _, r := range refs {
					if r.c.PlanSequence != event.PlanSequence && !slices.Contains(r.x.EntrySequences, event.PlanSequence) {
						continue
					}
					planned := r.x.Planned
					if original, ok := r.x.EntryPlanned[event.PlanSequence]; ok {
						planned = original
					}
					d := mustQuantity(planned) - mustQuantity(r.x.Filled)
					if d*float64(direction) > 0 {
						r.x.Filled = floatDecimal(mustQuantity(r.x.Filled) + amount*math.Abs(d)/demand)
					}
				}
			} else {
				remaining := math.Abs(amount)
				for _, r := range refs {
					q := mustQuantity(r.x.Filled)
					take := math.Min(math.Abs(q), remaining)
					r.x.Filled = floatDecimal(q - math.Copysign(take, q))
					remaining -= take
					if remaining <= 1e-12 {
						break
					}
				}
				if remaining > 1e-10 {
					return errors.New("factor: cohort reduction exceeds settled contributions")
				}
			}
			sum += amount
		}
		delta := actual - sum
		if math.Abs(delta) < 1e-12 {
			if !positions[sid].PendingUnknown && !positions[sid].IncreasingPending {
				for _, r := range refs {
					r.x.Outstanding = false
				}
			}
			continue
		}
		side := signed(actual)
		if side == 0 {
			side = signed(sum)
		}
		if delta*float64(side) > 0 {
			// Unresolved entry belongs to accepted old batches. A newly created
			// batch cannot absorb their delayed fills just because it shares SID.
			outstanding := false
			for _, r := range refs {
				if r.x.Outstanding && (mustQuantity(r.x.Planned)-mustQuantity(r.x.Filled))*float64(side) > 0 {
					outstanding = true
				}
			}
			total := 0.0
			for _, r := range refs {
				d := mustQuantity(r.x.Planned) - mustQuantity(r.x.Filled)
				if d*float64(side) > 0 && (!outstanding || r.x.Outstanding) && (grid <= r.c.EntryUntil || r.x.Outstanding || positions[sid].IncreasingPending || positions[sid].PendingUnknown) {
					total += math.Abs(d)
				}
			}
			if total > 0 {
				remaining := math.Abs(delta)
				for _, r := range refs {
					d := mustQuantity(r.x.Planned) - mustQuantity(r.x.Filled)
					if d*float64(side) <= 0 || outstanding && !r.x.Outstanding || !(grid <= r.c.EntryUntil || r.x.Outstanding || positions[sid].IncreasingPending || positions[sid].PendingUnknown) {
						continue
					}
					share := math.Min(math.Abs(d), math.Abs(delta)*math.Abs(d)/total)
					r.x.Filled = floatDecimal(mustQuantity(r.x.Filled) + float64(side)*share)
					remaining -= share
				}
				if remaining > 1e-10 {
					return errors.New("factor: aggregate cohort fill exceeds accepted demand; reconciliation required")
				}
			} else {
				return errors.New("factor: cohort has unexplained aggregate fill; explicit reconciliation required")
			}
		} else {
			remaining := math.Abs(delta)
			for _, r := range refs {
				v := mustQuantity(r.x.Filled)
				take := math.Min(math.Abs(v), remaining)
				r.x.Filled = floatDecimal(v - math.Copysign(take, v))
				remaining -= take
				if remaining <= 1e-12 {
					break
				}
			}
		}
		for _, r := range refs {
			if !positions[sid].PendingUnknown && !positions[sid].IncreasingPending {
				r.x.Outstanding = false
			}
		}
	}
	return nil
}

// TransferCohortContributions moves settled ownership, without creating fills.
func TransferCohortContributions(cohorts []PortfolioCohort, grid int64) float64 {
	cohortOrder(cohorts)
	moved := 0.0
	for i := range cohorts {
		if cohorts[i].Expires > grid {
			continue
		}
		for j := range cohorts[i].Contributions {
			old := &cohorts[i].Contributions[j]
			q := mustQuantity(old.Filled)
			if q == 0 {
				continue
			}
			for k := range cohorts {
				if cohorts[k].Created != grid || cohorts[k].Expires <= grid {
					continue
				}
				for l := range cohorts[k].Contributions {
					next := &cohorts[k].Contributions[l]
					d := mustQuantity(next.Planned) - mustQuantity(next.Filled)
					if next.SID != old.SID || signed(d) != signed(q) {
						continue
					}
					amount := math.Min(math.Abs(q), math.Abs(d))
					signedAmount := math.Copysign(amount, q)
					next.Filled = floatDecimal(mustQuantity(next.Filled) + signedAmount)
					old.Filled = floatDecimal(mustQuantity(old.Filled) - signedAmount)
					q -= signedAmount
					moved += amount
				}
			}
		}
	}
	return moved
}
func (p *LifecyclePolicy) proposeCohort(c PortfolioContext, s LifecycleState, due bool, round int64) (PortfolioProposal, error) {
	var reasons []Diagnostic
	if err := ReconcileCohortContributions(s.Cohorts, c.Positions, c.GridTime); err != nil {
		return PortfolioProposal{}, err
	}
	expired := false
	for _, cohort := range s.Cohorts {
		if cohort.Expires <= c.GridTime {
			expired = true
		}
	}
	selected := map[int32]float64{}
	selectionValid := !c.RiskOnly
	if c.RiskOnly {
	} else if c.Ideal != nil {
		selected = c.Ideal.Targets()
	} else {
		ideal, diag, err := SelectPortfolioScore(c.Frame, c.Universe, c.Spec, p.config, c.ScoreName, c.Groups)
		if err != nil {
			return PortfolioProposal{}, err
		}
		reasons = append(reasons, diag...)
		if ideal == nil {
			selectionValid = false
		} else {
			selected = ideal.Targets()
		}
	}
	if due && selectionValid {
		for sid, w := range selected {
			if life := s.Assets[sid]; life != nil && life.Direction != 0 && signed(w) != life.Direction {
				actual, _ := positionQuantity(c.Positions[sid])
				pending, _ := pendingQuantity(c.Positions[sid])
				if actual != 0 || pending != 0 || c.Positions[sid].PendingUnknown {
					delete(selected, sid)
					reasons = append(reasons, Diagnostic{"reverse-await-flat", fmt.Sprintf("SID %d cohort direction change waits for settled flat position", sid)})
				}
			}
		}
		scores := map[int32]float64{}
		for _, v := range policyScores(c.Frame, c.Universe, c.ScoreName) {
			scores[v.sid] = v.score
		}
		var err error
		if !c.PreserveIdealWeights {
			selected, err = AllocateSelected(selected, p.config, c.Spec.Budget.NAV, scores, c.Volatility)
			if err != nil {
				return PortfolioProposal{}, err
			}
		}
		n := p.config.Transition.PeriodBars / p.config.Rebalance.EveryBars
		if n > 1024 {
			return PortfolioProposal{}, errors.New("factor: cohort count exceeds bounded state limit")
		}
		count := 1
		if len(s.Cohorts) == 0 && !s.HasRound && p.config.Transition.Startup == "seed-all" {
			count = n
			reasons = append(reasons, Diagnostic{"seed-all", "initial batches use current ranking; no historical fills inferred"})
		}
		for index := 0; index < count; index++ {
			expiry := c.GridTime + int64(p.config.Transition.PeriodBars)*c.BarMillis
			if count > 1 {
				expiry = c.GridTime + int64(index+1)*int64(p.config.Rebalance.EveryBars)*c.BarMillis
			}
			batch := PortfolioCohort{ID: fmt.Sprintf("%d-%d", c.Spec.PlanSequence, index), Created: c.GridTime, Expires: expiry, EntryUntil: c.GridTime + int64(p.config.Transition.EntryWindowBars)*c.BarMillis, PlanSequence: c.Spec.PlanSequence}
			for _, sid := range mapSIDs(selected) {
				if c.ForceExit[sid] != "" {
					continue
				}
				mark := c.Marks[sid]
				if mark <= 0 {
					return PortfolioProposal{}, fmt.Errorf("factor: SID %d requires current mark for cohort sizing", sid)
				}
				weight := selected[sid] / float64(n)
				planned, err := ScaleQuantity(floatDecimal(weight*c.Spec.Budget.NAV/mark), 1, 1, c.Positions[sid].Quantum)
				if err != nil {
					return PortfolioProposal{}, err
				}
				batch.Contributions = append(batch.Contributions, CohortContribution{SID: sid, Planned: planned, Filled: "0", Weight: weight})
			}
			if len(batch.Contributions) > 0 {
				s.Cohorts = append(s.Cohorts, batch)
			}
		}
		s.LastRound = round
		s.HasRound = true
	}
	moved := TransferCohortContributions(s.Cohorts, c.GridTime)
	if moved > 0 {
		reasons = append(reasons, Diagnostic{"cohort-internal-transfer", fmt.Sprintf("%.12g settled units transferred without external fills or fees", moved)})
	}
	targets := map[int32]Allocation{}
	hard := map[int32]bool{}
	forcedSIDs := map[int32]bool{}
	total := map[int32]float64{}
	investable := map[int32]bool{}
	for _, sid := range c.Universe.Investable {
		investable[sid] = true
	}
	retained := make([]PortfolioCohort, 0, len(s.Cohorts))
	for _, cohort := range s.Cohorts {
		keep := false
		for j := range cohort.Contributions {
			x := &cohort.Contributions[j]
			e := c.Positions[x.SID]
			if c.GridTime >= cohort.EntryUntil && (e.PendingUnknown || e.IncreasingPending) {
				x.Outstanding = true
				reasons = append(reasons, Diagnostic{"exit-await-reconcile", fmt.Sprintf("SID %d expired cohort entry demand must be cancelled", x.SID)})
			}
			filled := mustQuantity(x.Filled)
			forced := c.ForceExit[x.SID] != "" || !investable[x.SID]
			if a := s.Assets[x.SID]; a != nil {
				h := p.holding(c, x.SID)
				maxAge := holdingMillis(h.MaxBars, h.MaxDuration, c.BarMillis)
				forced = forced || maxAge > 0 && a.FirstFillTime > 0 && c.GridTime-a.FirstFillTime >= maxAge
			}
			if forced {
				hard[x.SID] = true
				forcedSIDs[x.SID] = true
				expired = true
			}
			if cohort.Expires <= c.GridTime || forced {
				hard[x.SID] = true
				if cohort.Expires > c.GridTime || filled != 0 || x.Outstanding || e.PendingUnknown || e.IncreasingPending {
					keep = true
				}
				targets[x.SID] = Allocation{AbsoluteQuantity, "0"}
				continue
			}
			keep = true
			quantity := filled
			if p.config.Transition.Sizing == "current-nav" && due && selectionValid {
				mark := c.Marks[x.SID]
				if mark <= 0 {
					return PortfolioProposal{}, errors.New("factor: current-nav cohort requires visible mark")
				}
				quantity = x.Weight * c.Spec.Budget.NAV / mark
				x.Planned = floatDecimal(quantity)
			} else if c.GridTime < cohort.EntryUntil && !c.RiskOnly {
				quantity = mustQuantity(x.Planned)
			} else if e.IncreasingPending || e.PendingUnknown {
				reasons = append(reasons, Diagnostic{"exit-await-reconcile", fmt.Sprintf("SID %d expired cohort entry demand must be cancelled", x.SID)})
			}
			total[x.SID] += quantity
		}
		if keep {
			retained = append(retained, cohort)
		}
	}
	s.Cohorts = retained
	for _, sid := range pruneReleasedLifecycle(c, &s) {
		delete(targets, sid)
		delete(total, sid)
	}
	for _, sid := range mapSIDs(total) {
		if forcedSIDs[sid] {
			targets[sid] = Allocation{AbsoluteQuantity, "0"}
			continue
		}
		quantity, err := ScaleQuantity(floatDecimal(total[sid]), 1, 1, c.Positions[sid].Quantum)
		if err != nil {
			return PortfolioProposal{}, err
		}
		targets[sid] = Allocation{AbsoluteQuantity, quantity}
	}
	for _, sid := range mapSIDs(s.Assets) {
		if _, ok := targets[sid]; !ok {
			targets[sid] = Allocation{AbsoluteQuantity, "0"}
		}
	}
	emit := expired || due && selectionValid
	if emit {
		if err := p.constrain(c, s, targets, hard, &reasons); err != nil {
			return PortfolioProposal{}, err
		}
		for sid, target := range targets {
			a := s.Assets[sid]
			if a == nil {
				a = &AssetLifecycle{Direction: signed(mustQuantity(target.Value))}
				s.Assets[sid] = a
			}
			a.Last = target
		}
		for i := range s.Cohorts {
			cohort := &s.Cohorts[i]
			if cohort.Expires <= c.GridTime || c.GridTime >= cohort.EntryUntil && p.config.Transition.Sizing != "current-nav" {
				continue
			}
			for j := range cohort.Contributions {
				x := &cohort.Contributions[j]
				actual, _ := positionQuantity(c.Positions[x.SID])
				goal := mustQuantity(targets[x.SID].Value)
				if math.Abs(goal) <= math.Abs(actual) || math.Abs(mustQuantity(x.Planned)) <= math.Abs(mustQuantity(x.Filled)) {
					continue
				}
				if !slices.Contains(x.EntrySequences, c.Spec.PlanSequence) {
					x.EntrySequences = append(x.EntrySequences, c.Spec.PlanSequence)
				}
				if x.EntryPlanned == nil {
					x.EntryPlanned = map[uint64]string{}
				}
				x.EntryPlanned[c.Spec.PlanSequence] = x.Planned
				if len(x.EntrySequences) > 1024 {
					return PortfolioProposal{}, errors.New("factor: cohort entry provenance exceeds bounded limit")
				}
			}
		}
	}
	return p.finish(c, s, targets, reasons, emit)
}
