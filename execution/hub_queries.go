package execution

import (
	"context"
	"errors"
	"math"
	"time"
)

// DefaultLimitPrice runs liquidity-based pricing in the execution owner, never
// in the robot. Paper venues have no depth/volume transport and use their mark.
func (h *Hub) DefaultLimitPrice(ctx context.Context, key AccountKey, instrument string, side OrderSide, seconds int) (float64, error) {
	if !canonicalID(instrument) || (side != Buy && side != Sell) || seconds < 0 {
		return 0, errors.New("execution: invalid hub limit-price request")
	}
	var price float64
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(s *SharedAccount) error {
			if provider, ok := s.opts.Adapter.(interface {
				DefaultLimitPrice(context.Context, string, OrderSide, int) (float64, error)
			}); ok {
				var err error
				price, err = s.ReadLimitPrice(ctx, provider.DefaultLimitPrice, instrument, side, seconds)
				return err
			}
			if _, paper := s.opts.Adapter.(*PaperAdapter); !paper {
				return errors.New("execution: owner limit-price provider unavailable")
			}
			now := time.Now().UnixMilli()
			quote, err := s.ReadQuote(ctx, instrument, now)
			if err != nil {
				return err
			}
			if _, _, err := pendingQuote(map[string]VisibleQuote{instrument: quote}, instrument, now); err != nil {
				return err
			}
			value := quote.Bid
			if side == Buy {
				value = quote.Ask
			}
			price, _ = value.Float64()
			return nil
		})
	})
	if err == nil && (price <= 0 || math.IsNaN(price) || math.IsInf(price, 0)) {
		err = errors.New("execution: owner returned invalid limit price")
	}
	if err != nil {
		return 0, err
	}
	return price, nil
}

func (h *Hub) AccountRisk(ctx context.Context, key AccountKey) (PortfolioRisk, error) {
	var risk PortfolioRisk
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		risk, err = account.AccountRisk(ctx, time.Now().UnixMilli())
		return err
	})
	return risk, err
}

func (h *Hub) StrategyTotals(ctx context.Context, key AccountKey, strategy StrategyID) (StrategyTotals, error) {
	var totals StrategyTotals
	if !canonicalID(string(strategy)) {
		return totals, errors.New("execution: invalid hub strategy identity")
	}
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		totals, err = account.StrategyTotals(ctx, strategy)
		return err
	})
	return totals, err
}

func (h *Hub) VisibleQuote(ctx context.Context, key AccountKey, instrument string) (VisibleQuote, error) {
	var quote VisibleQuote
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		quote, err = account.VisibleQuote(ctx, instrument, time.Now().UnixMilli())
		return err
	})
	return quote, err
}

func (h *Hub) InstrumentSettled(ctx context.Context, key AccountKey, instrument Instrument, option bool) (bool, error) {
	if err := instrument.Validate(); err != nil {
		return false, err
	}
	var settled bool
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		if option {
			settled, err = account.OptionSettled(ctx, instrument)
		} else {
			settled, err = account.TerminalSettled(ctx, instrument)
		}
		return err
	})
	return settled, err
}

// HubWriterLease is read-only evidence for a fresh session's explicit CAS claim.
type HubWriterLease struct{ Epoch, Sequence, Revision uint64 }

func (h *Hub) WriterLease(ctx context.Context, key AccountKey, strategy StrategyID) (HubWriterLease, error) {
	var result HubWriterLease
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(s *SharedAccount) error {
			if !canonicalID(string(strategy)) {
				return errors.New("execution: invalid strategy identity")
			}
			state, err := s.store.strategyWriter(ctx, strategy)
			result = HubWriterLease{state.Epoch, state.Sequence, state.Revision}
			return err
		})
	})
	return result, err
}
