package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"maps"
	"math"
	"os"
	"path/filepath"
)

// NewPaperSink defaults to an in-memory ledger and local simulated venue.
// Explicit disk paths opt into a fresh durable diagnostic ledger.
type PaperAccountFactory func(execution.AccountKey, execution.SharedExecutionOptions) (*execution.SharedAccountBorrow, error)

func NewPaperSink(ctx context.Context, c Config) (*AccountSink, func() error, error) {
	return NewPaperSinkWithAccount(ctx, c, nil)
}

// NewPaperSinkWithAccount lets the outer process retain account ownership;
// standalone callers use the same logic with a run-local registry.
func NewPaperSinkWithAccount(ctx context.Context, c Config, factory PaperAccountFactory) (*AccountSink, func() error, error) {
	e := c.Execution
	accountNAV := c.AccountInitialNAV
	if accountNAV == 0 {
		accountNAV = c.InitialNAV
	}
	if c.InitialNAV <= 0 || accountNAV < c.InitialNAV || math.IsNaN(c.InitialNAV+accountNAV) || math.IsInf(c.InitialNAV+accountNAV, 0) {
		return nil, nil, errors.New("runner: invalid account/strategy paper capital")
	}
	if len(e.Instruments) == 0 || !e.MarginRate.IsPositive() || !e.MaxAccountMargin.IsPositive() || !e.MaxVirtualGross.IsPositive() || !e.StrategyGrossLimit.IsPositive() {
		return nil, nil, errors.New("runner: paper execution requires instrument units and risk limits")
	}
	memory := e.StorePath == "" && e.SenderLeaseDir == ""
	if e.HistoryPath != "" {
		if !memory || !filepath.IsAbs(e.HistoryPath) {
			return nil, nil, errors.New("runner: cold history needs memory execution and an absolute new file path")
		}
		if err := os.MkdirAll(filepath.Dir(e.HistoryPath), 0o700); err != nil {
			return nil, nil, err
		}
	}
	var path, lease string
	var err error
	if !memory {
		if e.StorePath == "" || e.SenderLeaseDir == "" {
			return nil, nil, errors.New("runner: durable paper replay needs both store and lease paths")
		}
		path, err = filepath.Abs(e.StorePath)
		if err != nil {
			return nil, nil, err
		}
		lease, err = filepath.Abs(e.SenderLeaseDir)
		if err != nil {
			return nil, nil, err
		}
		if _, err = os.Stat(path); err == nil {
			return nil, nil, errors.New("runner: paper replay requires a new ledger path")
		} else if !os.IsNotExist(err) {
			return nil, nil, err
		}
		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, nil, err
		}
		if err = os.MkdirAll(lease, 0755); err != nil {
			return nil, nil, err
		}
	}
	adapter, err := NewPaperAdapter(decimal.NewFromFloat(accountNAV), decimal.NewFromFloat(c.Manifest.Costs.FeeRate), decimal.NewFromFloat(c.Manifest.Costs.SlippageRate))
	if err != nil {
		return nil, nil, err
	}
	registry := &execution.AccountRegistry{}
	var runID [16]byte
	if _, err = rand.Read(runID[:]); err != nil {
		return nil, nil, err
	}
	key := execution.AccountKey{VenueSessionIdentity: "factor-paper:" + hex.EncodeToString(runID[:]), Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	opts := execution.SharedExecutionOptions{Memory: memory, HistoryPath: e.HistoryPath, StorePath: path, SenderLeaseDir: lease, Adapter: adapter, AuthoritativeSnapshot: true}
	var service *execution.SharedAccount
	var borrow *execution.SharedAccountBorrow
	if factory != nil {
		borrow, err = factory(key, opts)
	} else {
		var handle *execution.AccountHandle
		handle, err = registry.Acquire(key)
		if err == nil {
			service, err = execution.NewSharedAccount(handle, opts)
		}
		if err == nil {
			borrow = service.Borrow()
		}
	}
	if err != nil {
		registry.Close()
		return nil, nil, err
	}
	if borrow == nil {
		return nil, nil, errors.New("runner: paper account factory returned no borrow")
	}
	cleanup := func() error {
		borrow.Release()
		if service == nil {
			return nil
		}
		registry.Close()
		return service.Close()
	}
	nav := decimal.NewFromFloat(c.InitialNAV)
	capital := decimal.NewFromFloat(accountNAV)
	if err = borrow.CashEvent(execution.CashEvent{ID: "paper-initial-capital", Kind: execution.ExternalCashChange, AccountDelta: capital, Postings: []execution.CashPosting{{Amount: capital}}}); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	if err = borrow.CashEvent(execution.CashEvent{ID: "paper-strategy-allocation", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: nav.Neg()}, {Strategy: execution.StrategyID(c.StrategyID), Amount: nav}}}); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	if err = borrow.Reconcile("paper-initial-reconciliation", 0); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	sink := &AccountSink{Account: borrow, AccountID: c.AccountID, StrategyID: c.StrategyID, Currency: c.Manifest.Currency, Instruments: e.Instruments, Paper: adapter, Risk: execution.PortfolioRisk{MarginRate: e.MarginRate, MaxAccountMargin: e.MaxAccountMargin, MaxVirtualGross: e.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{execution.StrategyID(c.StrategyID): e.StrategyGrossLimit}}}
	// Archive valuation uses the last visible observation through one decision
	// interval plus the configured arrival window. Execution still requires a
	// strictly later quote; older valuation observations fail closed.
	sink.QuoteTTLMS = c.DecisionInterval + c.ExpiryMS
	sink.PolicySIDMap = maps.Clone(c.Snapshot.SIDMap)
	if err := sink.RegisterExecution(); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	return sink, cleanup, nil
}
