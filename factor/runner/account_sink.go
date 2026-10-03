package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
	"math"
	"sort"
)

type ExecutionConfig struct {
	StorePath, SenderLeaseDir, HistoryPath                            string
	Instruments                                                       map[int32]execution.Instrument
	MarginRate, MaxAccountMargin, MaxVirtualGross, StrategyGrossLimit decimal.Decimal
}

// AccountSink routes through the same process-owned ledger/coordinator as P4.
// It retains every other strategy's absolute positions, including zero targets
// needed to close this strategy's prior Full scope.
type AccountSink struct {
	Account                         *execution.SharedAccountBorrow
	AccountID, StrategyID, Currency string
	Instruments                     map[int32]execution.Instrument
	FundingInstruments              map[int32]execution.Instrument
	Risk                            execution.PortfolioRisk
	Paper                           *PaperAdapter
	VisibleQuote                    func(context.Context, string, int64) (execution.VisibleQuote, error)
	Clock                           func() int64
	AuthoritativeFunding            bool
	QuoteTTLMS                      int64
	observed                        map[string]execution.VisibleQuote
	marks                           map[string]decimal.Decimal
	previous                        *factor.TargetPortfolio
	paperMarket                     *paperMarket
}

func (s *AccountSink) RegisterExecution() error {
	cap, ok := s.Risk.StrategyGrossLimits[execution.StrategyID(s.StrategyID)]
	if !ok || !cap.IsPositive() {
		return errors.New("runner: explicit factor strategy policy required")
	}
	risk := s.Risk
	risk.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{execution.StrategyID(s.StrategyID): cap}
	if err := s.Account.RegisterRiskPolicy("shared-risk-v1", risk); err != nil {
		return err
	}
	instruments := make([]execution.Instrument, 0, len(s.Instruments))
	for _, i := range s.Instruments {
		instruments = append(instruments, i)
	}
	return s.Account.RegisterAccountQuotes(instruments, func(ctx context.Context, id string, now int64) (execution.VisibleQuote, error) {
		if s.paperMarket != nil {
			return s.paperMarket.quote(ctx, id, now)
		}
		if s.VisibleQuote != nil {
			return s.VisibleQuote(ctx, id, now)
		}
		q, ok := s.observed[id]
		if !ok {
			return q, errors.New("runner: account observed quote missing")
		}
		return q, nil
	}, s.Clock)
}

func (s *AccountSink) ObserveQuote(ctx context.Context, sid int32, q backtest.Quote, now int64) error {
	if q.Price <= 0 || math.IsNaN(q.Price) || math.IsInf(q.Price, 0) {
		return errors.New("runner: invalid account valuation price")
	}
	i, ok := s.Instruments[sid]
	if !ok {
		return fmt.Errorf("runner: missing instrument SID %d", sid)
	}
	if s.marks == nil {
		s.marks = map[string]decimal.Decimal{}
	}
	s.marks[i.ID] = decimal.NewFromFloat(q.Price)
	if s.observed == nil {
		s.observed = map[string]execution.VisibleQuote{}
	}
	bid, ask := q.Price, q.Price
	if q.Bid > 0 && q.Ask >= q.Bid {
		bid, ask = q.Bid, q.Ask
	}
	s.observed[i.ID] = execution.VisibleQuote{Bid: decimal.NewFromFloat(bid), Ask: decimal.NewFromFloat(ask), AtMS: q.AtMS, ReceivedMS: q.AvailableAt, ValidUntilMS: q.AtMS + s.QuoteTTLMS, Bar: q.AtMS}
	if s.paperMarket != nil {
		s.paperMarket.observe(i.ID, s.observed[i.ID])
	}
	return s.Account.AdvancePaperQuote(ctx, i.ID, s.observed[i.ID], now)
}
func (s *AccountSink) StrategyNAV(ctx context.Context, now int64) (float64, error) {
	state, err := s.StrategyState(ctx, now)
	return state.NAV, err
}
func (s *AccountSink) StrategyState(ctx context.Context, now int64) (backtest.State, error) {
	snap, err := s.Account.Snapshot(ctx)
	if err != nil {
		return backtest.State{}, err
	}
	risk, err := s.Account.AccountRisk(ctx, now)
	if err != nil {
		return backtest.State{}, err
	}
	id := execution.StrategyID(s.StrategyID)
	cash := snap.SyntheticStrategyCash[id]
	nav := cash
	state := backtest.State{Cash: cash.InexactFloat64(), Quantities: map[int32]float64{}}
	byID := map[string]int32{}
	for sid, i := range s.Instruments {
		byID[i.ID] = sid
	}
	for _, lot := range snap.Lots {
		if lot.Strategy != id {
			continue
		}
		sid, known := byID[lot.Instrument.ID]
		if !known {
			return state, errors.New("runner: strategy lot instrument has no declared SID")
		}
		mark, ok := risk.Marks[lot.Instrument.ID]
		if !ok {
			return state, errors.New("runner: strategy NAV mark unavailable")
		}
		nav = nav.Add(lot.Unrealized(mark))
		state.Quantities[sid] += decimal.NewFromInt(lot.SignedSteps).Mul(lot.Instrument.QuantityStep).Mul(lot.Instrument.ContractSize).InexactFloat64()
	}
	state.NAV = nav.InexactFloat64()
	totals, err := s.Account.StrategyTotals(ctx, id)
	if err != nil {
		return state, err
	}
	state.Fees = totals.Fees.InexactFloat64()
	state.Funding = totals.Funding.Neg().InexactFloat64()
	if s.Paper != nil {
		state.Slippage, state.Turnover = s.Paper.StrategyCosts(id)
	}
	return state, nil
}
func (s *AccountSink) ProcessSnapshot(ctx context.Context, p *factor.TargetPortfolio, quotes map[int32]backtest.Quote, now int64) error {
	if p == nil {
		return errors.New("runner: nil target portfolio")
	}
	sp := p.Spec()
	if sp.AccountID != s.AccountID || sp.StrategyID != s.StrategyID || sp.Budget.Currency != s.Currency || now < sp.ExecutableAt || now >= sp.ExpireAt {
		return errors.New("runner: portfolio/account budget identity or execution window mismatch")
	}
	effective, err := p.EffectiveTargets(s.previous)
	if err != nil {
		return err
	}
	targetsToExecute := effective
	if sp.Mode == factor.Patch {
		// Carry omitted absolute targets below; never resize them using this budget.
		targetsToExecute = p.Targets()
	}
	request := execution.StrategyRebalance{Strategy: execution.StrategyID(s.StrategyID), PlanID: p.ID(), DecisionMS: now, ExpiresMS: sp.ExpireAt, Mode: execution.StrategyTargetsFull}
	if sp.Mode == factor.Patch {
		request.Mode = execution.StrategyTargetsPatch
	}
	sids := make([]int32, 0, len(targetsToExecute))
	for sid := range targetsToExecute {
		sids = append(sids, sid)
	}
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	for _, sid := range sids {
		i, ok := s.Instruments[sid]
		if !ok || i.SettlementCurrency != s.Currency {
			return errors.New("runner: instrument/budget settlement mismatch")
		}
		q, ok := quotes[sid]
		if !ok || q.AtMS < sp.ExecutableAt || q.AtMS <= sp.DecisionTime || q.AvailableAt > now || q.AvailableAt < q.AtMS || q.AtMS > now || q.Price <= 0 || math.IsNaN(q.Price) || math.IsInf(q.Price, 0) {
			return errors.New("runner: unavailable executable quote")
		}
		price := decimal.NewFromFloat(q.Price)
		bid, ask := price, price
		if q.Bid > 0 && q.Ask >= q.Bid && !math.IsInf(q.Bid, 0) && !math.IsInf(q.Ask, 0) {
			bid, ask = decimal.NewFromFloat(q.Bid), decimal.NewFromFloat(q.Ask)
		} else if s.Paper == nil && s.VisibleQuote == nil {
			return errors.New("runner: live execution requires a visible bid/ask spread")
		}
		visible := execution.VisibleQuote{Bid: bid, Ask: ask, AtMS: q.AtMS, ReceivedMS: q.AvailableAt, ValidUntilMS: sp.ExpireAt, Bar: q.AtMS}
		if s.VisibleQuote != nil {
			if s.Clock == nil {
				return errors.New("runner: live quote fetch requires a real completion clock")
			}
			visible, err = s.VisibleQuote(ctx, i.ID, now)
			if err != nil {
				return err
			}
			now = max(now, s.Clock())
			visible.ValidUntilMS = min(visible.ValidUntilMS, sp.ExpireAt)
			if !visible.Bid.IsPositive() || visible.Ask.LessThan(visible.Bid) || visible.AtMS < sp.ExecutableAt || visible.AtMS > visible.ReceivedMS || visible.ReceivedMS > now || visible.ValidUntilMS <= now || now >= sp.ExpireAt {
				return errors.New("runner: fetched bid/ask is stale, unavailable or outside target window")
			}
			ticks := visible.Bid.Add(visible.Ask).Mul(decimal.RequireFromString("0.5")).Div(i.PriceTick).Floor()
			price = ticks.Mul(i.PriceTick)
			if price.LessThan(visible.Bid) || price.GreaterThan(visible.Ask) {
				return errors.New("runner: bid/ask has no compatible execution tick")
			}
			request.DecisionMS = now
		}
		qty := decimal.NewFromFloat(targetsToExecute[sid]).Mul(decimal.NewFromFloat(sp.Budget.NAV)).Div(price).Div(i.ContractSize)
		steps, err := execution.QuantitySteps(qty, i.QuantityStep)
		if err != nil {
			return err
		}
		request.Requests = append(request.Requests, execution.InstrumentRebalance{Instrument: i, Targets: []execution.ExecutableTarget{{Strategy: execution.StrategyID(s.StrategyID), Lot: execution.VirtualLotID(fmt.Sprintf("factor:%d", sid)), SignedSteps: steps}}, Quote: visible})
	}
	if err = s.Account.RebalanceStrategyContext(ctx, request, now); err != nil {
		return err
	}
	s.previous, err = factor.NewTargetPortfolio(sp, effective)
	return err
}

func (s *AccountSink) ObserveFunding(ctx context.Context, f backtest.Funding, now int64) error {
	if s.AuthoritativeFunding {
		return errors.New("runner: live funding requires authoritative cash metadata")
	}
	if f.AtMS != now || f.AvailableAt > now {
		return errors.New("runner: late/unavailable funding needs explicit historical reconciliation")
	}
	i, ok := s.Instruments[f.SID]
	if !ok {
		return errors.New("runner: funding instrument absent")
	}
	mark, ok := s.marks[i.ID]
	if !ok {
		return errors.New("runner: funding mark unavailable")
	}
	snap, err := s.Account.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err = validateFundingAttribution(snap, i); err != nil {
		return err
	}
	postings := []execution.CashPosting{}
	byStrategy := map[execution.StrategyID]decimal.Decimal{}
	delta := decimal.Zero
	for _, lot := range snap.Lots {
		if lot.Instrument.ID == i.ID {
			amount := i.Notional(lot.SignedSteps, mark).Mul(decimal.NewFromFloat(f.Rate)).Neg().Round(i.MoneyScale)
			byStrategy[lot.Strategy] = byStrategy[lot.Strategy].Add(amount)
			delta = delta.Add(amount)
		}
	}
	ids := make([]string, 0, len(byStrategy))
	for id := range byStrategy {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		postings = append(postings, execution.CashPosting{Strategy: execution.StrategyID(id), Amount: byStrategy[execution.StrategyID(id)]})
	}
	if len(postings) == 0 {
		postings = append(postings, execution.CashPosting{Amount: decimal.Zero})
	}
	var applied bool
	if applied, err = s.Account.CashEventApplied(execution.CashEvent{ID: f.ID, Kind: execution.Funding, AccountDelta: delta, Postings: postings, AtMS: now}); err != nil {
		return err
	}
	if applied && s.Paper != nil {
		s.Paper.ApplyCash(delta)
	}
	return nil
}

// ObserveFundingRecord retains the venue's actual cash delta and settlement
// identity. Decimal strings cross the source boundary without float rounding.
func (s *AccountSink) ObserveFundingRecord(ctx context.Context, r factor.VersionRecord, now int64) error {
	if !s.AuthoritativeFunding {
		return errors.New("runner: authoritative funding is not enabled")
	}
	i, ok := s.Instruments[r.Series.Sid]
	if funding, found := s.FundingInstruments[r.Series.Sid]; found {
		i, ok = funding, true
	}
	if !ok {
		return errors.New("runner: funding instrument absent")
	}
	id, ok := r.Series.Values["settlement_id"].(string)
	if !ok || id == "" || r.EventTime < 0 || r.EventTime > now || r.AvailableAt > now || r.IngestedAt > now {
		return errors.New("runner: incomplete/unavailable authoritative funding settlement")
	}
	parse := func(key string) (decimal.Decimal, error) {
		raw, ok := r.Series.Values[key].(string)
		if !ok {
			return decimal.Zero, fmt.Errorf("runner: funding %s requires an exact decimal string", key)
		}
		return decimal.NewFromString(raw)
	}
	mark, err := parse("mark")
	if err != nil {
		return err
	}
	rate, err := parse("rate")
	if err != nil {
		return err
	}
	amount, err := parse("account_amount")
	if err != nil {
		return err
	}
	_, err = s.Account.ApplyFunding(execution.FundingSettlement{ID: id, Instrument: i, Mark: mark, Rate: rate, AccountAmount: amount, AtMS: r.EventTime})
	return err
}
func validateFundingAttribution(snap execution.AccountSnapshot, i execution.Instrument) error {
	for _, lot := range snap.ExternalPositions {
		if lot.Instrument.ID == i.ID && lot.SignedSteps != 0 {
			return errors.New("runner: external position funding requires explicit unassigned reconciliation")
		}
	}
	virtual, actual := decimal.Zero, decimal.Zero
	for _, lot := range snap.Lots {
		if lot.Instrument.ID == i.ID {
			virtual = virtual.Add(decimal.NewFromInt(lot.SignedSteps))
		}
	}
	for _, lot := range snap.ActualPositions {
		if lot.Instrument.ID == i.ID {
			actual = actual.Add(decimal.NewFromInt(lot.SignedSteps))
		}
	}
	if !virtual.Equal(actual) {
		return errors.New("runner: unassigned account funding exposure requires explicit reconciliation")
	}
	return nil
}
