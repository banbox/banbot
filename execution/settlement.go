package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/shopspring/decimal"
)

type VisibleQuote struct {
	Bid          decimal.Decimal
	Ask          decimal.Decimal
	AtMS         int64
	ReceivedMS   int64
	ValidUntilMS int64
	Bar          int64
}

type InternalMatch struct {
	ID         string
	PlanID     string
	Instrument Instrument
	BuyIntent  VirtualIntentID
	SellIntent VirtualIntentID
	Steps      int64
	Quote      VisibleQuote
	AtMS       int64
}

// ApplyInternalMatch freezes an accepted visible quote and deterministically
// rounds its midpoint down to the instrument tick. It validates both original
// intents at that price and time. This is an attribution event, never a venue
// trade: actual position/cash and exchange fees stay unchanged.
func (s *Store) ApplyInternalMatch(ctx context.Context, match InternalMatch) (bool, error) {
	if err := match.Instrument.Validate(); err != nil {
		return false, err
	}
	q := match.Quote
	if !canonicalID(match.ID) || !canonicalID(match.PlanID) || match.Steps <= 0 || match.BuyIntent == match.SellIntent || !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS < 0 || q.AtMS > q.ReceivedMS || q.ReceivedMS > match.AtMS || q.ValidUntilMS <= match.AtMS || q.Bar < 0 {
		return false, errors.New("execution: invalid or invisible internal quote")
	}
	units, _ := q.Bid.Add(q.Ask).Mul(decimal.RequireFromString("0.5")).QuoRem(match.Instrument.PriceTick, 0)
	price := units.Mul(match.Instrument.PriceTick)
	if price.LessThan(q.Bid) || price.GreaterThan(q.Ask) {
		return false, errors.New("execution: rounded midpoint outside accepted quote")
	}
	body, err := payload(match)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, match.ID, "InternalFill", body)
		if err != nil || !fresh {
			return err
		}
		var planBody string
		if err := tx.QueryRow(opReadPlan, s.accountID, match.PlanID).Scan(&planBody); err != nil {
			return err
		}
		var plan Plan
		if err := json.Unmarshal([]byte(planBody), &plan); err != nil {
			return err
		}
		if match.AtMS < plan.DecisionMS || match.AtMS >= plan.ExpiresMS {
			return errors.New("execution: internal plan expired")
		}
		var latest int64
		if err := tx.QueryRow(opReadLatestSequence, s.accountID).Scan(&latest); err != nil {
			return err
		}
		if latest != plan.Sequence {
			return errors.New("execution: internal plan superseded")
		}
		var frozen bool
		if err := tx.QueryRow(opReadAccountFrozen, s.accountID).Scan(&frozen); err != nil {
			return err
		}
		if frozen {
			return errors.New("execution: frozen account cannot internally match")
		}
		var uncertain int
		if err := tx.QueryRow(opCountUncertainOrders, s.accountID, string(OrderUnknown), string(OrderSending), string(OrderCancelPending)).Scan(&uncertain); err != nil {
			return err
		}
		if uncertain > 0 {
			return errors.New("execution: uncertain order blocks internal matching")
		}
		var buy, sell EligibleIntent
		for n, id := range []VirtualIntentID{match.BuyIntent, match.SellIntent} {
			var intentBody string
			if err := tx.QueryRow(opReadPlanIntentRuntime, s.accountID, string(id), s.accountID, match.PlanID, string(id)).Scan(&intentBody); err != nil {
				return err
			}
			var intent EligibleIntent
			if err := json.Unmarshal([]byte(intentBody), &intent); err != nil {
				return err
			}
			if intent.Instrument != match.Instrument.ID || (n == 0 && intent.Side != Buy) || (n == 1 && intent.Side != Sell) {
				return errors.New("execution: internal intent side/instrument mismatch")
			}
			available, err := intent.Evaluate(price, match.AtMS, q.Bar)
			if err != nil {
				return err
			}
			var allocated int64
			if err := tx.QueryRow(opReadIntentConsumed, s.accountID, string(id), s.accountID, string(id)).Scan(&allocated); err != nil {
				return err
			}
			if match.Steps > available-allocated {
				return errors.New("execution: internal fill exceeds eligible/unallocated intent")
			}
			if n == 0 {
				buy = intent
			} else {
				sell = intent
			}
		}
		if buy.Strategy == sell.Strategy {
			return errors.New("execution: internal counterparties must be distinct strategies")
		}
		reclassified := decimal.Zero
		for _, intent := range []EligibleIntent{buy, sell} {
			lot, err := s.readLot(tx, intent.Strategy, intent.Lot, match.Instrument)
			if err != nil {
				return err
			}
			realized, err := updateLot(&lot, intent.Side, intent.Kind, match.Steps, match.Instrument.Notional(match.Steps, price), decimal.Zero)
			if err != nil {
				return err
			}
			if err := s.writeLot(tx, lot); err != nil {
				return err
			}
			if err := s.addCash(tx, intent.Strategy, realized); err != nil {
				return err
			}
			delta := match.Steps
			if intent.Side == Sell {
				delta = -delta
			}
			if err := s.ledger(tx, LedgerEntry{EventID: match.ID, Kind: "InternalFill", Strategy: intent.Strategy, Lot: intent.Lot, QuantityDelta: delta, RealizedPnL: realized, AtMS: match.AtMS}); err != nil {
				return err
			}
			if _, err := tx.Exec(opInsertInternalAllocation, s.accountID, match.ID, string(intent.ID), match.Steps); err != nil {
				return err
			}
			reclassified = reclassified.Add(realized)
		}
		if err := s.reclassify(tx, match.ID, reclassified, match.AtMS); err != nil {
			return err
		}
		if err := s.commitAccountEvent(tx, match.ID, decimal.Zero, nil); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

type FundingSettlement struct {
	ID            string
	Instrument    Instrument
	Mark          decimal.Decimal
	Rate          decimal.Decimal
	AccountAmount decimal.Decimal // authoritative venue settlement, signed cash delta
	AtMS          int64
}

func (s *Store) ApplyFunding(ctx context.Context, event FundingSettlement) (bool, error) {
	if err := event.Instrument.Validate(); err != nil {
		return false, err
	}
	if !canonicalID(event.ID) || !event.Mark.IsPositive() || event.AtMS < 0 {
		return false, errors.New("execution: invalid funding settlement")
	}
	body, err := payload(event)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, event.ID, "Funding", body)
		if err != nil || !fresh {
			return err
		}
		rows, err := tx.Query(opListActiveLots, s.accountID)
		if err != nil {
			return err
		}
		var lots []VirtualLot
		for rows.Next() {
			var b string
			if err := rows.Scan(&b); err != nil {
				rows.Close()
				return err
			}
			var lot VirtualLot
			if err := json.Unmarshal([]byte(b), &lot); err != nil {
				rows.Close()
				return err
			}
			if lot.Instrument.ID == event.Instrument.ID {
				lots = append(lots, lot)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		assigned := decimal.Zero
		for _, lot := range lots {
			a, _ := payload(lot.Instrument)
			b, _ := payload(event.Instrument)
			if a != b {
				return errors.New("execution: funding instrument revision mismatch")
			}
			amount := event.Instrument.Notional(lot.SignedSteps, event.Mark).Mul(event.Rate).Neg()
			lot.Funding = lot.Funding.Add(amount)
			if err := s.writeLot(tx, lot); err != nil {
				return err
			}
			if err := s.addCash(tx, lot.Strategy, amount); err != nil {
				return err
			}
			if err := s.addStrategyTotals(tx, lot.Strategy, decimal.Zero, amount); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: event.ID, Kind: "Funding", Strategy: lot.Strategy, Lot: lot.ID, CashDelta: amount, AtMS: event.AtMS}); err != nil {
				return err
			}
			assigned = assigned.Add(amount)
		}
		residual := event.AccountAmount.Sub(assigned)
		if !residual.IsZero() {
			if err := s.addCash(tx, "", residual); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: event.ID, Kind: "FundingReconciliation", CashDelta: residual, AtMS: event.AtMS}); err != nil {
				return err
			}
		}
		if err := s.commitAccountEvent(tx, event.ID, event.AccountAmount, nil); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

type ExternalPositionEvent struct {
	ID         string
	Kind       CashEventKind // ExternalCashChange (manual fill) or Liquidation
	Instrument Instrument
	Side       OrderSide
	Steps      int64
	Price      decimal.Decimal
	Fee        decimal.Decimal
	AtMS       int64
}

// ApplyExternalPosition isolates manual/forced trades in an explicit unassigned
// book and freezes risk. It never guesses a strategy from an instrument name.
func (s *Store) ApplyExternalPosition(ctx context.Context, event ExternalPositionEvent) (bool, error) {
	if err := event.Instrument.Validate(); err != nil {
		return false, err
	}
	if !canonicalID(event.ID) || event.Steps <= 0 || !event.Price.IsPositive() || event.AtMS < 0 || (event.Side != Buy && event.Side != Sell) || (event.Kind != ExternalCashChange && event.Kind != Liquidation) {
		return false, errors.New("execution: invalid external position event")
	}
	body, err := payload(event)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, event.ID, string(event.Kind), body)
		if err != nil || !fresh {
			return err
		}
		actual, err := s.actualPosition(tx, event.Instrument)
		if err != nil {
			return err
		}
		external := VirtualLot{ID: VirtualLotID(event.Instrument.ID), Instrument: event.Instrument}
		var b string
		err = tx.QueryRow(opReadExternalPosition, s.accountID, event.Instrument.ID).Scan(&b)
		if err == nil {
			if err := json.Unmarshal([]byte(b), &external); err != nil {
				return err
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		notional := event.Instrument.Notional(event.Steps, event.Price)
		realized, err := updateLot(&actual, event.Side, EntryIntent, event.Steps, notional, event.Fee)
		if err != nil {
			return err
		}
		virtualRealized, err := updateLot(&external, event.Side, EntryIntent, event.Steps, notional, event.Fee)
		if err != nil {
			return err
		}
		if err := s.writeActual(tx, actual); err != nil {
			return err
		}
		b, err = payload(external)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(opPutExternalPosition, s.accountID, event.Instrument.ID, external.SignedSteps, b); err != nil {
			return err
		}
		if err := s.addCash(tx, "", virtualRealized.Sub(event.Fee)); err != nil {
			return err
		}
		delta := event.Steps
		if event.Side == Sell {
			delta = -delta
		}
		if err := s.ledger(tx, LedgerEntry{EventID: event.ID, Kind: string(event.Kind), QuantityDelta: delta, CashDelta: event.Fee.Neg(), RealizedPnL: virtualRealized, Fee: event.Fee, AtMS: event.AtMS}); err != nil {
			return err
		}
		if err := s.reclassify(tx, event.ID, virtualRealized.Sub(realized), event.AtMS); err != nil {
			return err
		}
		freeze := true
		if err := s.commitAccountEvent(tx, event.ID, realized.Sub(event.Fee), &freeze); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

type AccountReconciliation struct {
	ID          string
	AccountCash decimal.Decimal
	Positions   map[string]int64
	AtMS        int64
}

// Reconcile validates, rather than overwrites, the ledger against an
// authoritative account snapshot. Unassigned open positions and uncertain
// orders keep admission frozen; their attribution needs an explicit migration.
func (s *Store) Reconcile(ctx context.Context, event AccountReconciliation) (bool, error) {
	if !canonicalID(event.ID) || event.AtMS < 0 {
		return false, errors.New("execution: invalid reconciliation")
	}
	body, err := payload(event)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, event.ID, "ReconciliationSnapshot", body)
		if err != nil || !fresh {
			return err
		}
		var value string
		if err := tx.QueryRow(opReadAccountCash, s.accountID).Scan(&value); err != nil {
			return err
		}
		cash, err := decimal.NewFromString(value)
		if err != nil {
			return err
		}
		if !cash.Equal(event.AccountCash) {
			return errors.New("execution: reconciliation settled cash mismatch")
		}
		rows, err := tx.Query(opListReconciliationPositions, s.accountID)
		if err != nil {
			return err
		}
		seen := make(map[string]bool)
		for rows.Next() {
			var b string
			if err := rows.Scan(&b); err != nil {
				rows.Close()
				return err
			}
			var position VirtualLot
			if err := json.Unmarshal([]byte(b), &position); err != nil {
				rows.Close()
				return err
			}
			if event.Positions[position.Instrument.ID] != position.SignedSteps {
				rows.Close()
				return errors.New("execution: reconciliation real position mismatch")
			}
			seen[position.Instrument.ID] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for id, n := range event.Positions {
			if !seen[id] && n != 0 {
				return errors.New("execution: unexplained venue position")
			}
		}
		var blocked int
		present, err := tableExists(tx, "exec_migration")
		if err != nil {
			return err
		}
		if present {
			if err := tx.QueryRow(opCountPendingMigrations, s.accountID).Scan(&blocked); err != nil {
				return err
			}
		}
		if blocked > 0 {
			return errors.New("execution: pending legacy migration blocks reconciliation")
		}
		if err := tx.QueryRow(opCountExternalPositions, s.accountID).Scan(&blocked); err != nil {
			return err
		}
		if blocked > 0 {
			return errors.New("execution: unassigned open position requires explicit attribution")
		}
		if err := tx.QueryRow(opCountUncertainOrders, s.accountID, string(OrderUnknown), string(OrderSending), string(OrderCancelPending)).Scan(&blocked); err != nil {
			return err
		}
		if blocked > 0 {
			return errors.New("execution: unresolved orders block reconciliation")
		}
		freeze := false
		if err := s.commitAccountEvent(tx, event.ID, decimal.Zero, &freeze); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}
