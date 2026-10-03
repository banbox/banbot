package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/shopspring/decimal"
)

type CommittedEvent struct {
	ID         string
	Kind       string
	Checkpoint int64
	Payload    json.RawMessage
	Ledger     []LedgerEntry
}

type OrderStateEvent struct {
	OrderID          string
	ExchangeID       string
	ClientID         string
	State            RealOrderState
	Allocations      []FillAllocation
	AllocationFilled map[string]int64
	FilledSteps      int64
	Attempt          int64
	Generation       uint64
}

// EventsAfter reads committed source events in their persisted account order.
// Projection consumers resume from their cursor and deduplicate by event ID;
// replay does not invoke adapters or create new intents.
func (s *Store) EventsAfter(ctx context.Context, checkpoint int64, limit int) ([]CommittedEvent, error) {
	if checkpoint < 0 || limit < 1 || limit > 10000 {
		return nil, errors.New("execution: invalid projection range")
	}
	var events []CommittedEvent
	err := s.commit(ctx, func(tx *storeTxn) error {
		rows, err := tx.Query(opListCommittedEvents, s.accountID, checkpoint, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var e CommittedEvent
			var body string
			if err := rows.Scan(&e.ID, &e.Kind, &e.Checkpoint, &body); err != nil {
				rows.Close()
				return err
			}
			e.Payload = json.RawMessage(body)
			events = append(events, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for n := range events {
			rows, err := tx.Query(opListEventPostings, s.accountID, events[n].ID)
			if err != nil {
				return err
			}
			for rows.Next() {
				entry := LedgerEntry{EventID: events[n].ID}
				var cash, fee, realized string
				if err := rows.Scan(&entry.ID, &entry.Kind, &entry.Strategy, &entry.Lot, &entry.QuantityDelta, &cash, &fee, &realized, &entry.AtMS); err != nil {
					rows.Close()
					return err
				}
				entry.CashDelta, err = decimal.NewFromString(cash)
				if err != nil {
					rows.Close()
					return err
				}
				entry.Fee, err = decimal.NewFromString(fee)
				if err != nil {
					rows.Close()
					return err
				}
				entry.RealizedPnL, err = decimal.NewFromString(realized)
				if err != nil {
					rows.Close()
					return err
				}
				events[n].Ledger = append(events[n].Ledger, entry)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return events, err
}

func (s *Store) ProjectionCursor(ctx context.Context, name string) (int64, error) {
	if !canonicalID(name) {
		return 0, errors.New("execution: invalid projection identity")
	}
	var cursor int64
	err := s.commit(ctx, func(tx *storeTxn) error {
		err := tx.QueryRow(opReadProjection, s.accountID, name).Scan(&cursor)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return cursor, err
}
func (s *Store) AdvanceProjection(ctx context.Context, name string, checkpoint int64) error {
	if !canonicalID(name) || checkpoint < 0 {
		return errors.New("execution: invalid projection cursor")
	}
	return s.commit(ctx, func(tx *storeTxn) error {
		var latest int64
		if err := tx.QueryRow(opReadAccountCheckpoint, s.accountID).Scan(&latest); err != nil {
			return err
		}
		if checkpoint > latest {
			return errors.New("execution: projection cannot advance beyond committed events")
		}
		var previous int64
		err := tx.QueryRow(opReadProjection, s.accountID, name).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if checkpoint < previous {
			return errors.New("execution: projection cursor regressed")
		}
		_, err = tx.Exec(opPutProjection, s.accountID, name, checkpoint)
		return err
	})
}
