package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/shopspring/decimal"
)

// TerminalSettlement is an explicit permanent cash close, never a trade quote.
// CashValue is nonnegative per base unit, including zero bankruptcy recovery.
type TerminalSettlement struct {
	Source, SourceVersion                  string
	Instrument                             Instrument
	CashValue                              decimal.Decimal
	EventMS, PublishedMS, ReceivedMS, AtMS int64
}

func (e TerminalSettlement) validate() error {
	if err := e.Instrument.Validate(); err != nil {
		return err
	}
	if !canonicalID(e.Source) || !canonicalID(e.SourceVersion) || e.Instrument.IsOption() || e.CashValue.IsNegative() || e.EventMS <= 0 || e.PublishedMS < e.EventMS || e.ReceivedMS < e.PublishedMS || e.AtMS < e.ReceivedMS {
		return errors.New("execution: terminal settlement requires nonnegative cash value and observable publication/receipt chronology")
	}
	return nil
}
func (e TerminalSettlement) identity() (string, string) {
	key, _ := payload(struct {
		ID       string
		Terminal bool
	}{e.Instrument.ID, true})
	h := sha256.Sum256([]byte(key))
	// A later retry's processing clock is not part of semantic identity.
	body, _ := payload(struct {
		Source, SourceVersion string
		Instrument            Instrument
		CashValue             decimal.Decimal
		EventMS, PublishedMS  int64
	}{e.Source, e.SourceVersion, e.Instrument, e.CashValue, e.EventMS, e.PublishedMS})
	return "terminal-settlement:" + hex.EncodeToString(h[:]), body
}

// TerminalSettled checks durable semantic settlement identity, including cold
// memory history. It does not infer settlement from an elapsed wall clock.
func (s *Store) TerminalSettled(ctx context.Context, instrument Instrument) (bool, error) {

	if err := instrument.Validate(); err != nil {
		return false, err
	}
	id, _ := (TerminalSettlement{Instrument: instrument}).identity()
	settled := false
	err := s.commit(ctx, func(tx *storeTxn) error {
		var kind, body string
		err := tx.QueryRow(opReadEvent, s.accountID, id).Scan(&kind, &body)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if kind != "TerminalSettlement" {
			return errors.New("execution: conflicting terminal instrument settlement identity")
		}
		var old struct{ Instrument Instrument }
		if err := json.Unmarshal([]byte(body), &old); err != nil {
			return err
		}
		a, _ := payload(old.Instrument)
		b, _ := payload(instrument)
		if a != b {
			return errors.New("execution: settled terminal instrument descriptor changed")
		}
		settled = true
		return nil
	})
	return settled, err
}

func (s *Store) rejectTerminalInstrument(tx *storeTxn, instrument Instrument) error {

	id, _ := (TerminalSettlement{Instrument: instrument}).identity()
	var kind, body string
	err := tx.QueryRow(opReadEvent, s.accountID, id).Scan(&kind, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("execution: a settled terminal instrument cannot accept new position activity")
}

func (b *SharedAccountBorrow) TerminalSettled(ctx context.Context, instrument Instrument) (bool, error) {
	var settled bool
	err := b.use(func(s *SharedAccount) error {
		var err error
		settled, err = s.store.TerminalSettled(ctx, instrument)
		return err
	})
	return settled, err
}

// ApplyTerminalSettlement closes all attributed and actual positions atomically,
// including worthless contracts. Unresolved orders prevent settlement: a
// later fill must not silently resurrect an terminal position.
func (s *Store) ApplyTerminalSettlement(ctx context.Context, event TerminalSettlement) (bool, error) {
	if err := event.validate(); err != nil {
		return false, err
	}
	id, _ := event.identity()
	body, _ := payload(event)
	applied := false
	err := s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordTerminalEvent(tx, event, id, body)
		if err != nil || !fresh {
			return err
		}
		// Unsent intents only remain executable while both plan and intent are valid.
		// Submitted orders are checked independently below, even after plan expiry.
		var planBody string
		planErr := tx.QueryRow(opReadLatestPlan, s.accountID).Scan(&planBody)
		if planErr != nil && !errors.Is(planErr, sql.ErrNoRows) {
			return planErr
		}
		if planErr == nil {
			var plan Plan
			if err := json.Unmarshal([]byte(planBody), &plan); err != nil {
				return err
			}
			for _, declared := range append(plan.Intents, plan.CarriedIntents...) {
				if declared.Instrument != event.Instrument.ID {
					continue
				}
				var currentBody string
				if err := tx.QueryRow(opReadIntentRuntime, s.accountID, string(declared.ID)).Scan(&currentBody); err != nil {
					return err
				}
				var current EligibleIntent
				if err := json.Unmarshal([]byte(currentBody), &current); err != nil {
					return err
				}
				var filled int64
				if err := tx.QueryRow(opReadIntentFilled, s.accountID, string(declared.ID), s.accountID, string(declared.ID)).Scan(&filled); err != nil {
					return err
				}
				if plan.ExpiresMS > event.AtMS && filled < current.QuantitySteps && current.State != Canceled && current.State != Expired && (current.Conditions.ExpiresAtMS == 0 || current.Conditions.ExpiresAtMS > event.AtMS) {
					return errors.New("execution: pending terminal intent blocks settlement")
				}
			}
		}
		rows, err := tx.Query(opListActiveOrders, s.accountID)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var orderID string
			if err := rows.Scan(&orderID); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, orderID)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, orderID := range ids {
			order, err := s.readOrder(ctx, tx, orderID)
			if err != nil {
				return err
			}
			if order.Intent.Instrument.ID == event.Instrument.ID {
				return errors.New("execution: pending or uncertain terminal instrument order blocks terminal settlement")
			}
		}
		lots, err := optionRows(tx, opListActiveLots, s.accountID)
		if err != nil {
			return err
		}
		external, err := optionRows(tx, opListExternalPositions, s.accountID)
		if err != nil {
			return err
		}
		actual, err := s.actualPosition(tx, event.Instrument)
		if err != nil {
			return err
		}
		units, _ := payload(event.Instrument)
		var net int64
		for _, lot := range append(append([]VirtualLot(nil), lots...), external...) {
			if lot.Instrument.ID != event.Instrument.ID {
				continue
			}
			other, _ := payload(lot.Instrument)
			if other != units {
				return errors.New("execution: terminal instrument revision mismatch")
			}
			net, err = checkedSteps(net, lot.SignedSteps)
			if err != nil {
				return err
			}
		}
		if net != actual.SignedSteps {
			return errors.New("execution: terminal actual/attributed quantities do not reconcile")
		}
		payoff := event.CashValue
		realizedTotal := decimal.Zero
		settle := func(lot *VirtualLot, kind string) error {
			steps := lot.SignedSteps
			if steps == 0 {
				return nil
			}
			amount := event.Instrument.Notional(steps, payoff)
			side := Sell
			if steps < 0 {
				side = Buy
			}
			realized, err := updateLot(lot, side, ExitIntent, absSteps(steps), amount.Abs(), decimal.Zero)
			if err != nil {
				return err
			}
			if kind == "TerminalSettlement" {
				if err := s.writeLot(tx, *lot); err != nil {
					return err
				}
			} else {
				b, err := payload(lot)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(opPutExternalPosition, s.accountID, lot.Instrument.ID, lot.SignedSteps, b); err != nil {
					return err
				}
			}
			if err := s.addCash(tx, lot.Strategy, realized); err != nil {
				return err
			}
			realizedTotal = realizedTotal.Add(realized)
			return s.ledger(tx, LedgerEntry{EventID: id, Kind: kind, Strategy: lot.Strategy, Lot: lot.ID, QuantityDelta: -steps, CashDelta: realized, RealizedPnL: realized, AtMS: event.AtMS})
		}
		for _, lot := range lots {
			if lot.Instrument.ID == event.Instrument.ID {
				if err := settle(&lot, "TerminalSettlement"); err != nil {
					return err
				}
			}
		}
		for _, lot := range external {
			if lot.Instrument.ID == event.Instrument.ID {
				if err := settle(&lot, "ExternalTerminalSettlement"); err != nil {
					return err
				}
			}
		}
		steps := actual.SignedSteps
		amount := event.Instrument.Notional(steps, payoff)
		realized := decimal.Zero
		if steps != 0 {
			side := Sell
			if steps < 0 {
				side = Buy
			}
			realized, err = updateLot(&actual, side, ExitIntent, absSteps(steps), amount.Abs(), decimal.Zero)
			if err != nil {
				return err
			}
		}
		if err := s.writeActual(tx, actual); err != nil {
			return err
		}
		// Internal crosses can leave attributed and actual cost bases different.
		// Closing them must release the same cash bridge as an exchange fill.
		if err := s.reclassify(tx, id, realizedTotal.Sub(realized), event.AtMS); err != nil {
			return err
		}
		if err := s.ledger(tx, LedgerEntry{EventID: id, Kind: "RealAccountTerminalSettlement", QuantityDelta: -steps, CashDelta: realized, RealizedPnL: realized, AtMS: event.AtMS}); err != nil {
			return err
		}
		if err := s.commitAccountEvent(tx, id, realized, nil); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

// ApplyTerminalSettlement is process-local paper venue settlement. It validates before
// mutating; durable account dedup lives in Store, as with paper fill receipts.
func (a *PaperAdapter) ApplyTerminalSettlement(ctx context.Context, event TerminalSettlement) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := event.validate(); err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateTerminalSettlementLocked(event); err != nil {
		return false, err
	}
	return a.applyTerminalSettlementLocked(event)
}

func (a *PaperAdapter) applyTerminalSettlementLocked(event TerminalSettlement) (bool, error) {
	id, body := event.identity()
	if _, ok := a.terminalSettlements[id]; ok {
		return false, nil
	}
	p := a.positions[event.Instrument.ID]
	realized := decimal.Zero
	if p.steps != 0 {
		side := Sell
		if p.steps < 0 {
			side = Buy
		}
		var err error
		realized, err = p.applyTrade(side, ExitIntent, absSteps(p.steps), event.Instrument.Notional(absSteps(p.steps), event.CashValue), decimal.Zero)
		if err != nil {
			return false, err
		}
	}
	if a.terminalSettlements == nil {
		a.terminalSettlements = map[string]string{}
	}
	a.cash = a.cash.Add(realized)
	p.instrument = event.Instrument
	a.positions[event.Instrument.ID] = p
	a.terminalSettlements[id] = body
	return true, nil
}

// ApplyTerminalSettlement owns the ledger and paper mutation under the same account
// serialization boundary. Paper remains process-local and cannot be restarted
// from the ledger; this adds no restart or real venue settlement capability.
func (b *SharedAccountBorrow) ApplyTerminalSettlement(event TerminalSettlement) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			paper, ok := s.opts.Adapter.(*PaperAdapter)
			if !ok {
				return errors.New("execution: cash terminal instruments require a process-local paper adapter")
			}
			// Validate venue state before the durable commit without changing it.
			paper.mu.Lock()
			defer paper.mu.Unlock()
			if err := event.validate(); err != nil {
				return err
			}
			if err := paper.validateTerminalSettlementLocked(event); err != nil {
				return err
			}
			snapshot, err := s.store.Snapshot(ctx)
			if err != nil {
				return err
			}
			actualSteps := int64(0)
			for _, position := range snapshot.ActualPositions {
				if position.Instrument.ID == event.Instrument.ID {
					actualSteps = position.SignedSteps
				}
			}
			if actualSteps != paper.positions[event.Instrument.ID].steps || !snapshot.AccountSettledCash.Equal(paper.cash) {
				return errors.New("execution: terminal settlement requires reconciled paper cash and actual quantities")
			}
			applied, err = s.store.ApplyTerminalSettlement(ctx, event)
			if err != nil || !applied {
				return err
			}
			_, err = paper.applyTerminalSettlementLocked(event)
			return err
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied, err
}

func (a *PaperAdapter) validateTerminalSettlementLocked(event TerminalSettlement) error {
	id, body := event.identity()
	if old, ok := a.terminalSettlements[id]; ok && old != body {
		return errors.New("execution: conflicting paper terminal instrument fixing")
	}
	for _, order := range a.pending {
		if order.intent.Instrument.ID == event.Instrument.ID {
			return errors.New("execution: resting paper terminal instrument order blocks terminal settlement")
		}
	}
	p := a.positions[event.Instrument.ID]
	if p.steps != 0 {
		old, _ := payload(p.instrument)
		units, _ := payload(event.Instrument)
		if old != units {
			return errors.New("execution: paper terminal instrument revision mismatch")
		}
	}
	return nil
}

func terminalID(i Instrument) string {
	id, _ := (TerminalSettlement{Instrument: i}).identity()
	return id
}
func (s *Store) PositionSettled(ctx context.Context, i Instrument) (bool, error) {
	done, err := s.TerminalSettled(ctx, i)
	if err != nil || done {
		return done, err
	}
	return s.OptionSettled(ctx, i)
}

func (s *Store) recordTerminalEvent(tx *storeTxn, e TerminalSettlement, id, body string) (bool, error) {
	var kind, previous string
	err := tx.QueryRow(opReadEvent, s.accountID, id).Scan(&kind, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		return s.recordEvent(tx, id, "TerminalSettlement", body)
	}
	if err != nil {
		return false, err
	}
	var old TerminalSettlement
	if err := json.Unmarshal([]byte(previous), &old); err != nil {
		return false, err
	}
	_, a := old.identity()
	_, b := e.identity()
	if kind != "TerminalSettlement" || a != b {
		return false, errors.New("execution: conflicting terminal settlement retry")
	}
	return false, nil
}
