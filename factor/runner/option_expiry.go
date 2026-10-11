package runner

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/shopspring/decimal"
)

// ObserveOptionExpiryRecord consumes an explicitly configured settlement
// source. SID identifies the option, EventTime is exact expiry, AvailableAt is
// fixing publication. Decimal-string fixing avoids binary float cash rounding.
func (s *AccountSink) ObserveOptionExpiryRecord(ctx context.Context, r factor.VersionRecord, now int64) error {
	i, ok := s.Instruments[r.Series.Sid]
	if !ok || !i.IsOption() || s.Paper == nil {
		return errors.New("runner: expiry requires a declared paper cash option")
	}
	fixingString, ok := r.Series.Values["fixing"].(string)
	if !ok {
		return errors.New("runner: option fixing must be a decimal string")
	}
	fixing, err := decimal.NewFromString(fixingString)
	if err != nil {
		return err
	}
	if r.IngestedAt > now || r.AvailableAt > now {
		return errors.New("runner: option fixing not yet observable")
	}
	_, err = s.Account.ApplyOptionExpiry(execution.OptionExpiry{Instrument: i, Fixing: fixing, FixingAtMS: r.EventTime, PublishedMS: r.AvailableAt, AtMS: now})
	return err
}

func (s *AccountSink) settledOption(ctx context.Context, i execution.Instrument, now int64) (bool, error) {
	if done, err := s.Account.TerminalSettled(ctx, i); err != nil || done {
		return done, err
	}
	if !i.IsOption() || now < i.Option.ExpiryMS {
		return false, nil
	}
	return s.Account.OptionSettled(ctx, i)
}

// optionQuoteExempt uses committed account settlement, never an elapsed clock
// or absent quote, to release a zero allocation's execution quote requirement.
// The serialized sink checks the same evidence again during admission.
func optionQuoteExempt(ctx context.Context, sink Sink, sid int32, zero bool, now int64) (bool, error) {
	account, ok := sink.(*AccountSink)
	if !ok {
		return false, nil
	}
	i, ok := account.Instruments[sid]
	if !ok {
		return false, nil
	}
	settled, err := account.settledOption(ctx, i, now)
	if err != nil || !settled {
		return false, err
	}
	if !zero {
		return false, errors.New("runner: target attempts to resurrect settled option")
	}
	return true, nil
}
