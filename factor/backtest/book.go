// Package backtest provides a quantity-based strategy book. Marks drift between
// decisions; only explicit target execution changes quantities.
package backtest

import (
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"maps"
	"math"
	"sort"
)

type Quote = execution.Quote
type Funding = execution.FundingObservation
type State struct {
	Cash, NAV, Fees, Slippage, Funding, Turnover float64
	Quantities                                   map[int32]float64
}
type Book struct {
	cash, fees, slippage, funding, turnover float64
	quantities                              map[int32]float64
	marks                                   map[int32]Quote
	previous                                *factor.TargetPortfolio
	fundingIDs                              map[string]bool
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func NewBook(nav float64) (*Book, error) {
	if nav <= 0 || !finite(nav) {
		return nil, errors.New("backtest: invalid initial strategy NAV")
	}
	return &Book{cash: nav, quantities: map[int32]float64{}, marks: map[int32]Quote{}, fundingIDs: map[string]bool{}}, nil
}
func (b *Book) Mark(sid int32, q Quote, now int64) error {
	if sid <= 0 || q.Price <= 0 || !finite(q.Price) || q.AtMS > now || q.AvailableAt > now || q.AtMS < 0 || q.AvailableAt < q.AtMS {
		return errors.New("backtest: invalid/unavailable quote")
	}
	if old, ok := b.marks[sid]; ok && q.AtMS < old.AtMS {
		return errors.New("backtest: mark moved backwards")
	}
	b.marks[sid] = q
	return nil
}
func (b *Book) State() State {
	nav := b.cash
	for _, sid := range sortedKeys(b.quantities) {
		qty := b.quantities[sid]
		nav += qty * b.marks[sid].Price
	}
	return State{b.cash, nav, b.fees, b.slippage, b.funding, b.turnover, maps.Clone(b.quantities)}
}
func (b *Book) Execute(p *factor.TargetPortfolio, quotes map[int32]Quote, now int64, fee, slip float64) error {
	if p == nil || fee < 0 || slip < 0 || slip >= 1 || !finite(fee) || !finite(slip) {
		return errors.New("backtest: invalid target/costs")
	}
	spec := p.Spec()
	if now < spec.ExecutableAt || now >= spec.ExpireAt {
		return errors.New("backtest: target outside execution window")
	}
	effective, err := p.EffectiveTargets(b.previous)
	if err != nil {
		return err
	}
	targets := effective
	if spec.Mode == factor.Patch {
		// Patch leaves omitted quantities unchanged despite price or NAV drift.
		targets = p.Targets()
	}
	// Validate the whole portfolio before changing any cash or quantities.
	for sid := range targets {
		q, ok := quotes[sid]
		if !ok || q.AtMS <= spec.DecisionTime || q.AtMS < spec.ExecutableAt || q.AtMS > now || q.AvailableAt > now || q.AvailableAt < q.AtMS || q.Price <= 0 || !finite(q.Price) {
			return errors.New("backtest: target requires a strictly later observable price")
		}
	}
	for _, sid := range sortedKeys(targets) {
		w := targets[sid]
		q := quotes[sid]
		qty := w * spec.Budget.NAV / q.Price
		delta := qty - b.quantities[sid]
		price := q.Price
		if delta > 0 {
			price *= 1 + slip
		} else if delta < 0 {
			price *= 1 - slip
		}
		notional := math.Abs(delta * q.Price)
		cost := math.Abs(delta*price) * fee
		b.cash -= delta*price + cost
		b.fees += cost
		b.slippage += math.Abs(delta * (price - q.Price))
		b.turnover += notional
		b.quantities[sid] = qty
		b.marks[sid] = q
	}
	b.previous, err = factor.NewTargetPortfolio(spec, effective)
	return err
}

// Funding is an explicit realized settlement, never an inferred venue schedule.
// Entries must be delivered at their settlement event with prior availability.
func (b *Book) ApplyFunding(f Funding, now int64) error {
	if f.ID == "" || f.SID <= 0 || f.AtMS != now || f.AvailableAt > now || f.AvailableAt < 0 || !finite(f.Rate) {
		return errors.New("backtest: funding settlement unavailable or delivered late")
	}
	if b.fundingIDs[f.ID] {
		return errors.New("backtest: duplicate funding settlement")
	}
	q, ok := b.marks[f.SID]
	if !ok {
		return errors.New("backtest: funding requires valuation mark")
	}
	cost := b.quantities[f.SID] * q.Price * f.Rate
	b.cash -= cost
	b.funding += cost
	b.fundingIDs[f.ID] = true
	return nil
}

// EndChunk drops deduplication IDs only after the caller advances past all
// settlements in an immutable, non-overlapping chunk.
func (b *Book) EndChunk() { clear(b.fundingIDs) }
func sortedKeys(values map[int32]float64) []int32 {
	keys := make([]int32, 0, len(values))
	for sid := range values {
		keys = append(keys, sid)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
