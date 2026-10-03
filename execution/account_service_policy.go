package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"math"
	"time"
)

type accountMarket struct {
	instrument Instrument
	quote      func(context.Context, string, int64) (VisibleQuote, error)
	clock      func() int64
}

func (s *SharedAccount) sendTime(now int64) int64 {
	for _, market := range s.markets {
		if market.clock != nil {
			now = max(now, market.clock())
		}
	}
	return now
}

// ValidateAccountBindings checks restored contributors and descriptors before
// admission. Holdings outside a factor universe still require an active owner.
func (b *SharedAccountBorrow) ValidateAccountBindings(strategies map[StrategyID]bool) error {
	return b.use(func(s *SharedAccount) error {
		snapshot, err := s.store.Snapshot(b.ctx)
		if err != nil {
			return err
		}
		check := func(i Instrument) error {
			m, ok := s.markets[i.ID]
			if !ok || !sameAccountInstrument(m.instrument, i) {
				return fmt.Errorf("execution: restored account instrument mapping missing or changed: %s", i.ID)
			}
			return nil
		}
		checkStrategy := func(id StrategyID) error {
			if !strategies[id] {
				return fmt.Errorf("execution: restored contributor has no configured live job: %s", id)
			}
			return nil
		}
		for _, lot := range snapshot.Lots {
			if err := check(lot.Instrument); err != nil {
				return err
			}
			if err := checkStrategy(lot.Strategy); err != nil {
				return err
			}
		}
		for _, position := range snapshot.ActualPositions {
			if err := check(position.Instrument); err != nil {
				return err
			}
		}
		for _, order := range snapshot.Orders {
			if order.State == OrderCanceled || order.State == OrderFilled {
				continue
			}
			if err := check(order.Intent.Instrument); err != nil {
				return err
			}
			for _, allocation := range order.Intent.Allocations {
				if err := checkStrategy(allocation.Strategy); err != nil {
					return err
				}
			}
		}
		plan, err := s.store.LatestPlan(b.ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, i := range plan.Instruments {
			if err := check(i); err != nil {
				return err
			}
		}
		for _, target := range plan.Targets {
			if err := checkStrategy(target.Strategy); err != nil {
				return err
			}
		}
		return nil
	})
}

func joinedOperationContext(ownerCtx, callerCtx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(15 * time.Second)
	if callerDeadline, ok := callerCtx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	ctx, cancel := context.WithDeadline(ownerCtx, deadline)
	stop := context.AfterFunc(callerCtx, cancel)
	if callerCtx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

// VisibleQuote joins read-only quote IO to the account owner cancellation and
// admission boundary. Quote providers must use this context and not reenter it.
func (b *SharedAccountBorrow) VisibleQuote(ctx context.Context, id string, now int64) (VisibleQuote, error) {
	var q VisibleQuote
	err := b.use(func(s *SharedAccount) error {
		market, ok := s.markets[id]
		if !ok {
			return fmt.Errorf("execution: account quote provider missing: %s", id)
		}
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ownerCtx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			ownerCtx, doneCaller := joinedOperationContext(ownerCtx, ctx)
			defer doneCaller()
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			q, err = market.quote(ownerCtx, id, now)
			return err
		})
	})
	return q, err
}

func (b *SharedAccountBorrow) AccountRisk(ctx context.Context, now int64) (PortfolioRisk, error) {
	var risk PortfolioRisk
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ownerCtx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			ownerCtx, doneCaller := joinedOperationContext(ownerCtx, ctx)
			defer doneCaller()
			if err := ctx.Err(); err != nil {
				return err
			}
			request := CombinedRebalance{DecisionMS: now, ExpiresMS: math.MaxInt64}
			var err error
			risk, _, err = s.accountRisk(ownerCtx, &request)
			return err
		})
	})
	return risk, err
}

func sameAccountInstrument(a, b Instrument) bool {
	return a.ID == b.ID && a.Version == b.Version && a.Valuation == b.Valuation && a.SettlementCurrency == b.SettlementCurrency && a.MoneyScale == b.MoneyScale && a.MinSteps == b.MinSteps && a.QuantityStep.Equal(b.QuantityStep) && a.ContractSize.Equal(b.ContractSize) && a.PriceTick.Equal(b.PriceTick) && a.MinNotional.Equal(b.MinNotional)
}

func (b *SharedAccountBorrow) RegisterRiskPolicy(version string, risk PortfolioRisk) error {
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			return s.store.RegisterAccountPolicy(ctx, AccountPolicy{Version: version, Currency: s.owner.Token().Key.SettlementDomain, MarginRate: risk.MarginRate, MaxAccountMargin: risk.MaxAccountMargin, MaxVirtualGross: risk.MaxVirtualGross, StrategyGrossLimits: risk.StrategyGrossLimits})
		})
	})
}

// RegisterAccountQuotes supplies real visible quotes for account instruments,
// independently of any individual factor universe. It must be rebound on restart.
func (b *SharedAccountBorrow) RegisterAccountQuotes(instruments []Instrument, quote func(context.Context, string, int64) (VisibleQuote, error), clock func() int64) error {
	if quote == nil {
		return errors.New("execution: account quote provider required")
	}
	return b.use(func(s *SharedAccount) error {
		for _, i := range instruments {
			if err := i.Validate(); err != nil {
				return err
			}
			if i.SettlementCurrency != s.owner.Token().Key.SettlementDomain {
				return errors.New("execution: account instrument currency mismatch")
			}
			if old, ok := s.markets[i.ID]; ok && !sameAccountInstrument(old.instrument, i) {
				return errors.New("execution: account instrument descriptor mismatch")
			}
		}
		if s.markets == nil {
			s.markets = map[string]accountMarket{}
		}
		for _, i := range instruments {
			if _, ok := s.markets[i.ID]; !ok {
				s.markets[i.ID] = accountMarket{instrument: i, quote: quote, clock: clock}
			}
		}
		return nil
	})
}
func (s *SharedAccount) accountRisk(ctx context.Context, request *CombinedRebalance) (PortfolioRisk, int64, error) {
	p, err := s.store.AccountPolicy(ctx)
	if err != nil {
		return PortfolioRisk{}, 0, fmt.Errorf("execution: account policy unavailable: %w", err)
	}
	risk := PortfolioRisk{MarginRate: p.MarginRate, MaxAccountMargin: p.MaxAccountMargin, MaxVirtualGross: p.MaxVirtualGross, StrategyGrossLimits: p.StrategyGrossLimits, Marks: map[string]decimal.Decimal{}}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return risk, 0, err
	}
	latest, err := s.store.LatestPlan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return risk, 0, err
	}
	instruments := map[string]Instrument{}
	strategies := map[StrategyID]bool{}
	for _, lot := range snapshot.Lots {
		instruments[lot.Instrument.ID] = lot.Instrument
		strategies[lot.Strategy] = true
	}
	for _, position := range snapshot.ActualPositions {
		instruments[position.Instrument.ID] = position.Instrument
	}
	for _, order := range snapshot.Orders {
		if order.State == OrderCanceled || order.State == OrderFilled {
			continue
		}
		instruments[order.Intent.Instrument.ID] = order.Intent.Instrument
		for _, a := range order.Intent.Allocations {
			strategies[a.Strategy] = true
		}
	}
	for _, i := range latest.Instruments {
		instruments[i.ID] = i
	}
	for _, t := range latest.Targets {
		strategies[t.Strategy] = true
	}
	for _, r := range request.Requests {
		instruments[r.Instrument.ID] = r.Instrument
		for _, t := range r.Targets {
			strategies[t.Strategy] = true
		}
	}
	for _, t := range request.CarryTargets {
		strategies[t.Strategy] = true
	}
	for id := range strategies {
		if cap, ok := p.StrategyGrossLimits[id]; !ok || !cap.IsPositive() {
			return risk, 0, fmt.Errorf("execution: account strategy policy missing: %s", id)
		}
	}
	validUntil := request.ExpiresMS
	for id, i := range instruments {
		market, ok := s.markets[id]
		if !ok || !sameAccountInstrument(market.instrument, i) {
			return risk, 0, fmt.Errorf("execution: account mark provider/descriptor missing: %s", id)
		}
		q, err := market.quote(ctx, id, request.DecisionMS)
		if err != nil {
			return risk, 0, err
		}
		if market.clock != nil {
			request.DecisionMS = max(request.DecisionMS, market.clock())
		}
		if !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS <= 0 || q.AtMS > q.ReceivedMS || q.ReceivedMS > request.DecisionMS || q.ValidUntilMS <= request.DecisionMS {
			return risk, 0, fmt.Errorf("execution: account mark stale or unavailable: %s", id)
		}
		risk.Marks[id] = q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
		validUntil = min(validUntil, q.ValidUntilMS)
	}
	if validUntil <= request.DecisionMS {
		return risk, 0, errors.New("execution: account marks expired during collection")
	}
	return risk, validUntil, nil
}
