package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

func fundingCapability(exchange banexg.BanExchange) (banexg.FundingCashCapability, bool) {
	if capability, ok := exchange.(banexg.FundingCashCapability); ok {
		return capability, true
	}
	if wrapped, ok := exchange.(interface{ UnderlyingExchange() banexg.BanExchange }); ok {
		return fundingCapability(wrapped.UnderlyingExchange())
	}
	return nil, false
}

// RecoverCash commits funding before balance reconciliation. A watermark is
// advanced only after every settlement in a complete bounded window commits.
// Interrupted pages replay through the immutable settlement IDs on restart.
func (a *BanexgAdapter) RecoverCash(ctx context.Context, account *SharedAccountBorrow) error {
	capability, ok := fundingCapability(a.exchange)
	if !ok {
		return nil // Custom verified integrations can own funding via data sources.
	}
	if account.AccountKey() != a.config.Account {
		return errors.New("execution: funding recovery account mismatch")
	}
	var since int64 = -1
	var floor int64
	err := account.WithState(func(service *SharedAccount) error {
		for cursor := int64(0); ; {
			page, err := service.store.EventsAfter(ctx, cursor, 512)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				return nil
			}
			for _, event := range page {
				if event.ID == "live-capital-bootstrap-v1" || strings.HasPrefix(event.ID, "funding-watermark/") {
					var cash CashEvent
					if err := json.Unmarshal(event.Payload, &cash); err != nil {
						return err
					}
					if event.ID == "live-capital-bootstrap-v1" {
						floor = cash.AtMS
					}
					since = max(since, cash.AtMS)
				}
			}
			cursor = page[len(page)-1].Checkpoint
		}
	})
	if err != nil || since < 0 {
		return err
	}
	now := time.Now().UnixMilli()
	// The overlap tolerates delayed publication by the venue. Older late
	// settlements still fail the existing historical attribution checks.
	since = max(floor, since-60000)
	for since <= now {
		until := min(now, since+7*24*time.Hour.Milliseconds())
		var rows []banexg.FundingCash
		err := account.WithState(func(service *SharedAccount) error {
			return service.owner.DoLocal(service.owner.Token(), func(ownerCtx context.Context) error {
				ownerCtx, borrowDone := joinedOperationContext(ownerCtx, account.Context())
				defer borrowDone()
				operationCtx, done := joinedOperationContext(ownerCtx, ctx)
				defer done()
				operationCtx, cancel := context.WithTimeout(operationCtx, 15*time.Second)
				defer cancel()
				return a.invoke(operationCtx, func() error {
					result, err := capability.FetchFundingCash(operationCtx, a.config.Account.Account, a.config.Account.SettlementDomain, since, until)
					rows = result
					if err != nil {
						return err
					}
					return nil
				})
			})
		})
		if err != nil {
			return err
		}
		for _, row := range rows {
			i, ok := a.bySymbol[row.Symbol]
			if !ok || row.Currency != a.config.Account.SettlementDomain || row.AtMS < since || row.AtMS > until || row.ID == "" {
				return errors.New("execution: unexplained funding settlement identity")
			}
			amount, err := decimal.NewFromString(row.Amount)
			if err != nil {
				return err
			}
			mark, err := decimal.NewFromString(row.Mark)
			if err != nil {
				return err
			}
			rate, err := decimal.NewFromString(row.Rate)
			if err != nil {
				return err
			}
			if _, err := account.ApplyFunding(FundingSettlement{ID: row.ID, Instrument: i, AccountAmount: amount, Mark: mark, Rate: rate, AtMS: row.AtMS}); err != nil {
				return err
			}
		}
		// Do not use an in-memory cursor: a crash may happen after any commit.
		if err := account.CashEvent(CashEvent{ID: "funding-watermark/" + decimal.NewFromInt(until).String(), Kind: Reconciliation, Postings: []CashPosting{{Amount: decimal.Zero}}, AtMS: until}); err != nil {
			return err
		}
		since = until + 1
	}
	return nil
}
