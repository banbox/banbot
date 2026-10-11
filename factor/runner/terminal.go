package runner

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
)

func terminalRecord(r factor.VersionRecord, now int64) (backtest.TerminalSettlement, error) {
	value, ok := r.Series.Values["terminal_cash_value"].(string)
	if !ok {
		return backtest.TerminalSettlement{}, errors.New("runner: terminal cash value must be an explicit decimal string")
	}
	cash, err := decimal.NewFromString(value)
	if err != nil {
		return backtest.TerminalSettlement{}, err
	}
	if r.Series.Source == "" || r.SourceVersion == "" || r.Series.TimeFrame != "event" || r.Series.TimeMS != r.EventTime || r.Series.Sid <= 0 || cash.IsNegative() || r.EventTime <= 0 || r.AvailableAt < r.EventTime || r.IngestedAt < r.AvailableAt || now < r.IngestedAt {
		return backtest.TerminalSettlement{}, errors.New("runner: terminal event not observable or invalid chronology")
	}
	return backtest.TerminalSettlement{Source: r.Series.Source, SourceVersion: r.SourceVersion, SID: r.Series.Sid, CashValue: cash, EventMS: r.EventTime, PublishedMS: r.AvailableAt, ReceivedMS: r.IngestedAt}, nil
}
func (s *AccountSink) ObserveTerminalSettlementRecord(ctx context.Context, r factor.VersionRecord, now int64) error {
	e, err := terminalRecord(r, now)
	if err != nil {
		return err
	}
	i, ok := s.Instruments[e.SID]
	if !ok || s.Paper == nil {
		return errors.New("runner: terminal settlement needs declared paper instrument")
	}
	_, err = s.Account.ApplyTerminalSettlement(execution.TerminalSettlement{Source: e.Source, SourceVersion: e.SourceVersion, Instrument: i, CashValue: e.CashValue, EventMS: e.EventMS, PublishedMS: e.PublishedMS, ReceivedMS: e.ReceivedMS, AtMS: now})
	return err
}
func terminalQuoteExempt(ctx context.Context, sink Sink, book *backtest.Book, sid int32, zero bool) (bool, error) {
	if book != nil {
		return book.TerminalQuoteExempt(sid, zero)
	}
	account, ok := sink.(*AccountSink)
	if !ok {
		return false, nil
	}
	i, ok := account.Instruments[sid]
	if !ok {
		return false, nil
	}
	done, err := account.Account.TerminalSettled(ctx, i)
	if err != nil || !done {
		return false, err
	}
	if !zero {
		return false, errors.New("runner: target resurrects terminal instrument")
	}
	return true, nil
}
