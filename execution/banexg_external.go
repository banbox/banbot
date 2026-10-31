package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// ExternalOrderHint contains identity only. Streamed quantities and fees are
// deliberately excluded; only a complete terminal query can supply economics.
type ExternalOrderHint struct {
	Symbol     string
	ExchangeID string
	ClientID   string
}

func (a *BanexgAdapter) RecoverExternal(ctx context.Context, report BanexgStreamReport) ([]ExternalPositionEvent, error) {
	hint := report.ExternalOrder
	if hint == nil || !canonicalID(hint.Symbol) || !canonicalID(hint.ExchangeID) || hint.ExchangeID != report.UnassignedExchangeID || report.Err != nil && !errors.Is(report.Err, sql.ErrNoRows) {
		return nil, errors.New("execution: external recovery requires an unassigned identity hint")
	}
	instrument, ok := a.bySymbol[hint.Symbol]
	if !ok || !a.proof.CompleteCumulativeReports || !a.proof.ContextBound || !a.proof.NetLinearPositions {
		return nil, errors.New("execution: external instrument or complete recovery capability is unproven")
	}
	if strings.HasPrefix(hint.ClientID, "exec-") {
		return nil, errors.New("execution: unknown execution client identity cannot be adopted")
	}
	if _, err := a.storedIdentity(ctx, hint.ClientID, hint.ExchangeID); !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Join(errors.New("execution: external identity is known or conflicting"), err)
	}
	params := a.params(ctx)
	params[banexg.ParamCompleteOrder] = true
	var raw *banexg.Order
	if err := a.invoke(ctx, func() error {
		order, err := a.exchange.FetchOrder(hint.Symbol, hint.ExchangeID, params)
		raw = order
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if raw == nil || raw.ID != hint.ExchangeID || raw.Symbol != hint.Symbol || hint.ClientID != "" && raw.ClientOrderID != hint.ClientID || strings.HasPrefix(raw.ClientOrderID, "exec-") {
		return nil, errors.New("execution: complete external order identity mismatch")
	}
	if _, err := a.storedIdentity(ctx, raw.ClientOrderID, raw.ID); !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Join(errors.New("execution: queried external order belongs to a known or conflicting identity"), err)
	}
	if raw.Status != banexg.OdStatusFilled && raw.Status != banexg.OdStatusCanceled && raw.Status != banexg.OdStatusExpired {
		return nil, errors.New("execution: active external orders remain quarantined")
	}
	if raw.Side != string(Buy) && raw.Side != string(Sell) {
		return nil, errors.New("execution: external order side is incomplete")
	}
	filled, err := quantitySteps(raw.Filled, instrument)
	if err != nil || filled <= 0 {
		return nil, errors.New("execution: external order has no complete filled quantity")
	}
	amount, err := quantitySteps(raw.Amount, instrument)
	if err != nil || filled > amount || raw.Status == banexg.OdStatusFilled && filled != amount {
		return nil, errors.New("execution: external order fill quantity mismatch")
	}
	cost, err := decimalBoundary(raw.Cost)
	if err != nil || !cost.IsPositive() || raw.Fee == nil || raw.Fee.Currency != instrument.SettlementCurrency {
		return nil, errors.New("execution: external cumulative cost or settlement fee is incomplete")
	}
	fee, err := decimalBoundary(raw.Fee.Cost)
	if err != nil {
		return nil, err
	}
	var totalSteps int64
	totalCost, totalFee := decimal.Zero, decimal.Zero
	seen := make(map[string]bool)
	events := make([]ExternalPositionEvent, 0, len(raw.Trades))
	for _, trade := range raw.Trades {
		if trade == nil || !canonicalID(trade.ID) || seen[trade.ID] || trade.Order != raw.ID || trade.Symbol != raw.Symbol || trade.Side != raw.Side || trade.Timestamp <= 0 || trade.Fee == nil || trade.Fee.Currency != instrument.SettlementCurrency {
			return nil, errors.New("execution: external trade identity, timestamp or fee is incomplete")
		}
		seen[trade.ID] = true
		steps, err := quantitySteps(trade.Amount, instrument)
		if err != nil || steps <= 0 || steps > filled-totalSteps {
			return nil, errors.New("execution: external trade quantity is incomplete")
		}
		price, err := decimalBoundary(trade.Price)
		if err != nil || !price.IsPositive() {
			return nil, errors.New("execution: external trade price is incomplete")
		}
		tradeCost, err := decimalBoundary(trade.Cost)
		if err != nil || !tradeCost.Equal(instrument.Notional(steps, price)) {
			return nil, errors.New("execution: external trade cost does not match exact units")
		}
		tradeFee, err := decimalBoundary(trade.Fee.Cost)
		if err != nil {
			return nil, err
		}
		totalSteps += steps
		totalCost = totalCost.Add(tradeCost)
		totalFee = totalFee.Add(tradeFee)
		events = append(events, ExternalPositionEvent{
			ID: "external-trade/" + rebalanceID(instrument.ID, raw.ID, trade.ID), Kind: ExternalCashChange,
			Instrument: instrument, Side: OrderSide(raw.Side), Steps: steps, Price: price, Fee: tradeFee, AtMS: trade.Timestamp,
		})
	}
	if totalSteps != filled || !totalCost.Equal(cost) || !totalFee.Equal(fee) {
		return nil, errors.New("execution: complete external trade history does not match cumulative economics")
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].AtMS != events[j].AtMS {
			return events[i].AtMS < events[j].AtMS
		}
		return events[i].ID < events[j].ID
	})
	return events, nil
}

// RecoverExternalReport queries and commits a complete batch under the account
// owner gate. Admission remains stopped until the following full reconciliation.
func (b *SharedAccountBorrow) RecoverExternalReport(callerCtx context.Context, report BanexgStreamReport) error {
	err := b.use(func(s *SharedAccount) error {
		if s.opts.TakeoverStrategy == "" {
			return errors.New("execution: external takeover owner is not configured")
		}
		recovery, ok := s.opts.Adapter.(interface {
			RecoverExternal(context.Context, BanexgStreamReport) ([]ExternalPositionEvent, error)
		})
		if !ok {
			return errors.New("execution: complete external order recovery is unsupported")
		}
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ctx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			ctx, callerDone := joinedOperationContext(ctx, callerCtx)
			defer callerDone()
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			events, err := recovery.RecoverExternal(ctx, report)
			if err != nil || len(events) == 0 {
				return errors.Join(errors.New("execution: external report has no complete terminal trade history"), err)
			}
			return s.store.atomically(ctx, func(ctx context.Context) error {
				if err := s.store.validateExternalRecoveryBaseline(ctx, events); err != nil {
					return err
				}
				var now int64
				for _, event := range events {
					if _, err := s.store.ApplyExternalPosition(ctx, event); err != nil {
						return err
					}
					now = max(now, event.AtMS)
				}
				_, err := s.store.AutoTakeoverExternal(ctx, s.opts.TakeoverStrategy, now)
				return err
			})
		})
	})
	if err == nil {
		b.service.notifyCommitted()
	}
	return err
}

// A complete order query proves its trades, not that their cash is absent from
// the imported opening balance. Historical unseen trades require manual review.
func (s *Store) validateExternalRecoveryBaseline(ctx context.Context, events []ExternalPositionEvent) error {
	var floor int64
	for cursor := int64(0); ; {
		page, err := s.EventsAfter(ctx, cursor, 512)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, event := range page {
			switch {
			case event.ID == "live-capital-bootstrap-v1":
				var baseline CashEvent
				if err := json.Unmarshal(event.Payload, &baseline); err != nil {
					return err
				}
				floor = max(floor, baseline.AtMS)
			case event.Kind == "LegacyMigration":
				var baseline LegacyMigration
				if err := json.Unmarshal(event.Payload, &baseline); err != nil {
					return err
				}
				floor = max(floor, baseline.VenueSnapshot.AtMS)
			}
		}
		cursor = page[len(page)-1].Checkpoint
	}
	return s.commit(ctx, func(tx *storeTxn) error {
		for _, event := range events {
			if event.AtMS > floor {
				continue
			}
			var kind, body string
			if err := tx.QueryRow(opReadEvent, s.accountID, event.ID).Scan(&kind, &body); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.New("execution: unseen external trade predates the financial baseline; manual recovery required")
				}
				return err
			}
			// ApplyExternalPosition verifies exact immutable payload on replay.
		}
		return nil
	})
}
