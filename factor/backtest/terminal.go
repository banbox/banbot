package backtest

import (
	"errors"
	"github.com/shopspring/decimal"
)

// TerminalSettlement is explicit permanent retirement; CashValue is per share.
type TerminalSettlement struct {
	Source, SourceVersion            string
	SID                              int32
	CashValue                        decimal.Decimal
	EventMS, PublishedMS, ReceivedMS int64
}

func (b *Book) ApplyTerminalSettlement(e TerminalSettlement, now int64) (bool, error) {
	if e.Source == "" || e.SourceVersion == "" || e.SID <= 0 || e.CashValue.IsNegative() || e.EventMS <= 0 || e.PublishedMS < e.EventMS || e.ReceivedMS < e.PublishedMS || now < e.ReceivedMS {
		return false, errors.New("backtest: invalid/unobservable terminal settlement")
	}
	if old, ok := b.terminals[e.SID]; ok {
		if old.Source != e.Source || old.SourceVersion != e.SourceVersion || !old.CashValue.Equal(e.CashValue) || old.EventMS != e.EventMS || old.PublishedMS != e.PublishedMS {
			return false, errors.New("backtest: conflicting terminal settlement")
		}
		return false, nil
	}
	value := e.CashValue.InexactFloat64()
	amount := b.quantities[e.SID] * value
	if !finite(value) || !finite(amount) || !finite(b.cash+amount) {
		return false, errors.New("backtest: unrepresentable terminal cash")
	}
	if b.terminals == nil {
		b.terminals = map[int32]TerminalSettlement{}
	}
	b.cash += amount
	delete(b.quantities, e.SID)
	delete(b.firstFill, e.SID)
	delete(b.marks, e.SID)
	remaining := b.fillFacts[:0]
	for _, fill := range b.fillFacts {
		if fill.sid != e.SID {
			remaining = append(remaining, fill)
		}
	}
	b.fillFacts = remaining
	b.terminals[e.SID] = e
	b.ledgerCursor++
	return true, nil
}
func (b *Book) TerminalQuoteExempt(sid int32, zero bool) (bool, error) {
	if _, ok := b.terminals[sid]; !ok {
		return false, nil
	}
	if !zero {
		return false, errors.New("backtest: target resurrects terminal instrument")
	}
	return true, nil
}
