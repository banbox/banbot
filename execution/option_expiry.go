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

// OptionExpiry carries the exact-expiry fixing and its publication cutoff.
// AtMS is receipt/processing time, never substituted for the source fixing.
// Settlement IDs derive from contract and expiry, not a caller's message ID.
type OptionExpiry struct {
	Instrument  Instrument
	Fixing      decimal.Decimal
	FixingAtMS  int64
	PublishedMS int64
	AtMS        int64
}

func (e OptionExpiry) validate() error {
	if err := e.Instrument.Validate(); err != nil {
		return err
	}
	if !e.Instrument.IsOption() || e.Fixing.IsNegative() || e.FixingAtMS != e.Instrument.Option.ExpiryMS || e.PublishedMS < e.FixingAtMS || e.AtMS < e.PublishedMS {
		return errors.New("execution: expiry requires a published exact-expiry nonnegative fixing")
	}
	if e.Fixing.GreaterThan(e.Instrument.Option.UnderlyingStressMax) {
		return errors.New("execution: fixing exceeds declared research stress scenario bound")
	}
	return nil
}

func (e OptionExpiry) identity() (string, string) {
	key, _ := payload(struct {
		ID       string
		ExpiryMS int64
	}{e.Instrument.ID, e.Instrument.Option.ExpiryMS})
	h := sha256.Sum256([]byte(key))
	// A later retry's processing clock is not part of semantic identity.
	body, _ := payload(struct {
		Instrument              Instrument
		Fixing                  decimal.Decimal
		FixingAtMS, PublishedMS int64
	}{e.Instrument, e.Fixing, e.FixingAtMS, e.PublishedMS})
	return "option-expiry:" + hex.EncodeToString(h[:]), body
}

// OptionSettled checks durable semantic settlement identity, including cold
// memory history. It does not infer settlement from an elapsed wall clock.
func (s *Store) OptionSettled(ctx context.Context, instrument Instrument) (bool, error) {
	if !instrument.IsOption() {
		return false, nil
	}
	if err := instrument.Validate(); err != nil {
		return false, err
	}
	id, _ := (OptionExpiry{Instrument: instrument}).identity()
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
		if kind != "OptionExpiry" {
			return errors.New("execution: conflicting option settlement identity")
		}
		var old struct{ Instrument Instrument }
		if err := json.Unmarshal([]byte(body), &old); err != nil {
			return err
		}
		a, _ := payload(old.Instrument)
		b, _ := payload(instrument)
		if a != b {
			return errors.New("execution: settled option descriptor changed")
		}
		settled = true
		return nil
	})
	return settled, err
}

func (s *Store) rejectSettledOption(tx *storeTxn, instrument Instrument) error {
	if err := s.rejectTerminalInstrument(tx, instrument); err != nil {
		return err
	}
	if !instrument.IsOption() {
		return nil
	}
	id, _ := (OptionExpiry{Instrument: instrument}).identity()
	var kind, body string
	err := tx.QueryRow(opReadEvent, s.accountID, id).Scan(&kind, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("execution: a settled option cannot accept new position activity")
}

func (b *SharedAccountBorrow) OptionSettled(ctx context.Context, instrument Instrument) (bool, error) {
	var settled bool
	err := b.use(func(s *SharedAccount) error {
		var err error
		settled, err = s.store.OptionSettled(ctx, instrument)
		return err
	})
	return settled, err
}

func optionRows(tx *storeTxn, operation storageOperation, account string) ([]VirtualLot, error) {
	rows, err := tx.Query(operation, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lots []VirtualLot
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var lot VirtualLot
		if err := json.Unmarshal([]byte(body), &lot); err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

// ApplyOptionExpiry closes all attributed and actual positions atomically,
// including worthless contracts. Unresolved orders prevent settlement: a
// later fill must not silently resurrect an expired position.
func (s *Store) ApplyOptionExpiry(ctx context.Context, event OptionExpiry) (bool, error) {
	if err := event.validate(); err != nil {
		return false, err
	}
	id, body := event.identity()
	applied := false
	err := s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, id, "OptionExpiry", body)
		if err != nil || !fresh {
			return err
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
				return errors.New("execution: pending or uncertain option order blocks expiry")
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
				return errors.New("execution: expiry instrument revision mismatch")
			}
			net, err = checkedSteps(net, lot.SignedSteps)
			if err != nil {
				return err
			}
		}
		if net != actual.SignedSteps {
			return errors.New("execution: expiry actual/attributed quantities do not reconcile")
		}
		payoff := event.Instrument.OptionPayoff(event.Fixing)
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
			if kind == "OptionExpiry" {
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
			if err := s.addCash(tx, lot.Strategy, amount); err != nil {
				return err
			}
			return s.ledger(tx, LedgerEntry{EventID: id, Kind: kind, Strategy: lot.Strategy, Lot: lot.ID, QuantityDelta: -steps, CashDelta: amount, RealizedPnL: realized, AtMS: event.AtMS})
		}
		for _, lot := range lots {
			if lot.Instrument.ID == event.Instrument.ID {
				if err := settle(&lot, "OptionExpiry"); err != nil {
					return err
				}
			}
		}
		for _, lot := range external {
			if lot.Instrument.ID == event.Instrument.ID {
				if err := settle(&lot, "ExternalOptionExpiry"); err != nil {
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
		if err := s.ledger(tx, LedgerEntry{EventID: id, Kind: "RealAccountOptionExpiry", QuantityDelta: -steps, CashDelta: amount, RealizedPnL: realized, AtMS: event.AtMS}); err != nil {
			return err
		}
		if err := s.commitAccountEvent(tx, id, amount, nil); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

// ApplyOptionExpiry is process-local paper venue settlement. It validates before
// mutating; durable account dedup lives in Store, as with paper fill receipts.
func (a *PaperAdapter) ApplyOptionExpiry(ctx context.Context, event OptionExpiry) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := event.validate(); err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateOptionExpiryLocked(event); err != nil {
		return false, err
	}
	return a.applyOptionExpiryLocked(event), nil
}

func (a *PaperAdapter) applyOptionExpiryLocked(event OptionExpiry) bool {
	id, body := event.identity()
	if _, ok := a.optionExpiries[id]; ok {
		return false
	}
	if a.optionExpiries == nil {
		a.optionExpiries = map[string]string{}
	}
	p := a.positions[event.Instrument.ID]
	a.cash = a.cash.Add(event.Instrument.Notional(p.steps, event.Instrument.OptionPayoff(event.Fixing)))
	p.steps = 0
	p.costBasis = decimal.Zero
	p.instrument = event.Instrument
	a.positions[event.Instrument.ID] = p
	a.optionExpiries[id] = body
	return true
}

// ApplyOptionExpiry owns the ledger and paper mutation under the same account
// serialization boundary. Paper remains process-local and cannot be restarted
// from the ledger; this adds no restart or real venue settlement capability.
func (b *SharedAccountBorrow) ApplyOptionExpiry(event OptionExpiry) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			paper, ok := s.opts.Adapter.(*PaperAdapter)
			if !ok {
				return errors.New("execution: cash options require a process-local paper adapter")
			}
			// Validate venue state before the durable commit without changing it.
			paper.mu.Lock()
			defer paper.mu.Unlock()
			if err := event.validate(); err != nil {
				return err
			}
			if err := paper.validateOptionExpiryLocked(event); err != nil {
				return err
			}
			var err error
			applied, err = s.store.ApplyOptionExpiry(ctx, event)
			if err != nil || !applied {
				return err
			}
			paper.applyOptionExpiryLocked(event)
			return nil
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied, err
}

func (a *PaperAdapter) validateOptionExpiryLocked(event OptionExpiry) error {
	id, body := event.identity()
	if old, ok := a.optionExpiries[id]; ok && old != body {
		return errors.New("execution: conflicting paper option fixing")
	}
	for _, order := range a.pending {
		if order.intent.Instrument.ID == event.Instrument.ID {
			return errors.New("execution: resting paper option order blocks expiry")
		}
	}
	p := a.positions[event.Instrument.ID]
	if p.steps != 0 {
		old, _ := payload(p.instrument)
		units, _ := payload(event.Instrument)
		if old != units {
			return errors.New("execution: paper expiry instrument revision mismatch")
		}
	}
	return nil
}
