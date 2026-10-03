package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"math"
	"sync"
)

type paperMarket struct {
	mu     sync.Mutex
	quotes map[string]execution.VisibleQuote
}

func (m *paperMarket) quote(_ context.Context, id string, _ int64) (execution.VisibleQuote, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.quotes[id]
	if !ok {
		return q, errors.New("runner: shared paper quote unavailable")
	}
	return q, nil
}
func (m *paperMarket) observe(id string, q execution.VisibleQuote) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.quotes[id]
	if !ok || old.AtMS <= q.AtMS {
		m.quotes[id] = q
	}
}

// NewPaperSinks creates one account owner/venue per account within this run,
// allocates each strategy's budget and retains any unallocated account cash.
func NewPaperSinks(ctx context.Context, configs []Config) ([]Sink, func() error, error) {
	return NewPaperSinksWithAccounts(ctx, configs, nil)
}

func NewPaperSinksWithAccounts(ctx context.Context, configs []Config, factory PaperAccountFactory) ([]Sink, func() error, error) {
	sinks := make([]Sink, len(configs))
	groups := map[string][]int{}
	for i, c := range configs {
		groups[c.AccountID] = append(groups[c.AccountID], i)
	}
	var cleanups []func() error
	cleanup := func() error {
		var errs []error
		for i := len(cleanups) - 1; i >= 0; i-- {
			errs = append(errs, cleanups[i]())
		}
		return errors.Join(errs...)
	}
	for _, indexes := range groups {
		first := configs[indexes[0]]
		total := decimal.Zero
		ids := map[string]bool{}
		for _, i := range indexes {
			c := configs[i]
			if c.InitialNAV <= 0 || math.IsNaN(c.InitialNAV) || math.IsInf(c.InitialNAV, 0) || math.IsNaN(c.AccountInitialNAV) || math.IsInf(c.AccountInitialNAV, 0) {
				return nil, nil, errors.Join(errors.New("runner: invalid shared paper capital"), cleanup())
			}
			e, base := c.Execution, first.Execution
			if ids[c.StrategyID] {
				return nil, nil, errors.Join(fmt.Errorf("runner: duplicate simulated strategy %s", c.StrategyID), cleanup())
			}
			ids[c.StrategyID] = true
			if c.Manifest.Currency != first.Manifest.Currency || c.Manifest.Costs != first.Manifest.Costs || !e.MarginRate.Equal(base.MarginRate) || !e.MaxAccountMargin.Equal(base.MaxAccountMargin) || !e.MaxVirtualGross.Equal(base.MaxVirtualGross) || e.StorePath != base.StorePath || e.SenderLeaseDir != base.SenderLeaseDir || e.HistoryPath != base.HistoryPath {
				return nil, nil, errors.Join(errors.New("runner: incompatible shared paper account policy"), cleanup())
			}
			total = total.Add(decimal.NewFromFloat(c.InitialNAV))
		}
		capital := first.AccountInitialNAV
		if capital == 0 {
			capital = total.InexactFloat64()
		}
		for _, i := range indexes {
			if configs[i].AccountInitialNAV != 0 && configs[i].AccountInitialNAV != capital {
				return nil, nil, errors.Join(errors.New("runner: conflicting shared account initial capital"), cleanup())
			}
		}
		if decimal.NewFromFloat(capital).LessThan(total) {
			return nil, nil, errors.Join(errors.New("runner: strategy allocations exceed account capital"), cleanup())
		}
		first.AccountInitialNAV = capital
		sink, close, err := NewPaperSinkWithAccount(ctx, first, factory)
		if err != nil {
			return nil, nil, errors.Join(err, cleanup())
		}
		cleanups = append(cleanups, close)
		market := &paperMarket{quotes: map[string]execution.VisibleQuote{}}
		sink.paperMarket = market
		sinks[indexes[0]] = sink
		for _, i := range indexes[1:] {
			c := configs[i]
			e := c.Execution
			borrow := sink.Account.Service().Borrow()
			cleanups = append(cleanups, func() error { borrow.Release(); return nil })
			nav := decimal.NewFromFloat(c.InitialNAV)
			if err = borrow.CashEvent(execution.CashEvent{ID: "paper-strategy-allocation:" + c.StrategyID, Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: nav.Neg()}, {Strategy: execution.StrategyID(c.StrategyID), Amount: nav}}}); err != nil {
				return nil, nil, errors.Join(err, cleanup())
			}
			other := &AccountSink{Account: borrow, AccountID: c.AccountID, StrategyID: c.StrategyID, Currency: c.Manifest.Currency, Instruments: e.Instruments, Paper: sink.Paper, paperMarket: market, QuoteTTLMS: c.DecisionInterval + c.ExpiryMS, Risk: execution.PortfolioRisk{MarginRate: e.MarginRate, MaxAccountMargin: e.MaxAccountMargin, MaxVirtualGross: e.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{execution.StrategyID(c.StrategyID): e.StrategyGrossLimit}}}
			if err = other.RegisterExecution(); err != nil {
				return nil, nil, errors.Join(err, cleanup())
			}
			sinks[i] = other
		}
	}
	return sinks, cleanup, nil
}
