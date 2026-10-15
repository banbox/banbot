package execution

import (
	"context"
	"errors"
	"sync"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// LegacySender preserves the established TS settlement/recovery implementation
// while fencing its SDK writes with the physical account's owner and OS lease.
// Invoke joins the actual synchronous SDK call. It does not promise transport
// interruption: shutdown can wait for SDK retries/semaphore/network timeouts.
type LegacySender struct {
	owner   *AccountHandle
	mu      sync.Mutex
	closed  bool
	work    sync.WaitGroup
	release func() error
	once    sync.Once
	err     error
}

func NewLegacySender(owner *AccountHandle, domains []string, leaseDir string) (*LegacySender, error) {
	if owner == nil || len(domains) == 0 {
		return nil, errors.New("execution: legacy sender requires owner and declared settlement domains")
	}
	if err := owner.Token().Key.Validate(); err != nil {
		return nil, err
	}
	release, err := acquireSenderLeases(owner.Token().Key, domains, leaseDir)
	if err != nil {
		return nil, err
	}
	return &LegacySender{owner: owner, release: release}, nil
}

func (s *LegacySender) Invoke(ctx context.Context, call func() error) error {
	if s == nil || call == nil {
		return errors.New("execution: legacy sender/call is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrOwnerStopped
	}
	s.work.Add(1)
	s.mu.Unlock()
	defer s.work.Done()
	return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
		if err := ownerCtx.Err(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return call()
	})
}

func (s *LegacySender) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.work.Wait()
		s.err = s.release()
		s.owner.Release()
	})
	return s.err
}

// LegacyExchange gates every SDK v0.2.64 operation that can write venue state.
// Call is gated too because dynamically named APIs may change account state.
// Read methods retain the supplied exchange's existing implementation.
type LegacyExchange struct {
	banexg.BanExchange
	ctx            context.Context
	defaultAccount string
	senders        map[string]*LegacySender
	mu             sync.Mutex
	stopped        bool
	work           sync.WaitGroup
}

// Stop seals this runtime's admission. It leaves sibling runtimes and the
// process-owned sender alive. Join waits for this facade's actual SDK calls.
func (e *LegacyExchange) Stop() {
	e.mu.Lock()
	e.stopped = true
	e.mu.Unlock()
}

func (e *LegacyExchange) Join() { e.work.Wait() }

func NewLegacyExchange(ctx context.Context, exchange banexg.BanExchange, defaultAccount string, senders map[string]*LegacySender) (*LegacyExchange, error) {
	if ctx == nil || exchange == nil || len(senders) == 0 || senders[defaultAccount] == nil {
		return nil, errors.New("execution: legacy exchange requires a resolved default sender")
	}
	owned := make(map[string]*LegacySender, len(senders))
	for account, sender := range senders {
		if !canonicalID(account) || sender == nil || sender.owner.Token().Key.Account != account {
			return nil, errors.New("execution: legacy exchange account/sender mismatch")
		}
		owned[account] = sender
	}
	return &LegacyExchange{BanExchange: exchange, ctx: ctx, defaultAccount: defaultAccount, senders: owned}, nil
}

func (e *LegacyExchange) UnderlyingExchange() banexg.BanExchange { return e.BanExchange }

func (e *LegacyExchange) FetchOHLCVArchive(ctx context.Context, symbol, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.OHLCVArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchOHLCVArchive(ctx, symbol, timeframe, startMS, endMS)
}

func IsLegacyExchange(exchange banexg.BanExchange) bool {
	_, ok := exchange.(*LegacyExchange)
	return ok
}

func (e *LegacyExchange) invoke(params map[string]any, call func(map[string]any) error) *errs.Error {
	e.mu.Lock()
	if e.stopped || e.ctx.Err() != nil {
		e.mu.Unlock()
		return errs.New(errs.CodeRunTime, ErrOwnerStopped)
	}
	e.work.Add(1)
	e.mu.Unlock()
	defer e.work.Done()
	account := e.defaultAccount
	if value, present := params[banexg.ParamAccount]; present {
		var ok bool
		account, ok = value.(string)
		if !ok || !canonicalID(account) {
			return errs.NewMsg(errs.CodeParamInvalid, "execution: unresolved sender account")
		}
	}
	sender := e.senders[account]
	if sender == nil {
		return errs.NewMsg(errs.CodeParamInvalid, "execution: account has no owned sender")
	}
	args := make(map[string]any, len(params)+1)
	for key, value := range params {
		args[key] = value
	}
	args[banexg.ParamAccount] = account
	if err := sender.Invoke(e.ctx, func() error { return call(args) }); err != nil {
		var sdkErr *errs.Error
		if errors.As(err, &sdkErr) {
			return sdkErr
		}
		return errs.New(errs.CodeRunTime, err)
	}
	return nil
}

func (e *LegacyExchange) CreateOrder(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
	var result *banexg.Order
	err := e.invoke(params, func(args map[string]any) error {
		var err *errs.Error
		result, err = e.BanExchange.CreateOrder(symbol, kind, side, amount, price, args)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

func (e *LegacyExchange) CancelOrder(id, symbol string, params map[string]any) (*banexg.Order, *errs.Error) {
	var result *banexg.Order
	err := e.invoke(params, func(args map[string]any) error {
		var err *errs.Error
		result, err = e.BanExchange.CancelOrder(id, symbol, args)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

func (e *LegacyExchange) EditOrder(symbol, id, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
	var result *banexg.Order
	err := e.invoke(params, func(args map[string]any) error {
		var err *errs.Error
		result, err = e.BanExchange.EditOrder(symbol, id, side, amount, price, args)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

func (e *LegacyExchange) SetLeverage(leverage float64, symbol string, params map[string]any) (map[string]any, *errs.Error) {
	var result map[string]any
	err := e.invoke(params, func(args map[string]any) error {
		var err *errs.Error
		result, err = e.BanExchange.SetLeverage(leverage, symbol, args)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

func (e *LegacyExchange) Call(method string, params map[string]any) (*banexg.HttpRes, *errs.Error) {
	var result *banexg.HttpRes
	err := e.invoke(params, func(args map[string]any) error {
		var err *errs.Error
		result, err = e.BanExchange.Call(method, args)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}
