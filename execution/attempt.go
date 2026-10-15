package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/shopspring/decimal"
)

type AttemptKind string

const (
	SubmitAttempt   AttemptKind = "Submit"
	CancelAttempt   AttemptKind = "Cancel"
	RecoveryAttempt AttemptKind = "Recovery"
)

type AttemptPhase string

const (
	AttemptStarted  AttemptPhase = "Started"
	AttemptResult   AttemptPhase = "Result"
	AttemptImported AttemptPhase = "Imported" // the complete retained v1/v2 row, including its last result
)

// OrderAttempt is immutable audit evidence. Started commits before transport;
// each observed result appends a separate event without rewriting that intent.
type OrderAttempt struct {
	OrderID    string
	Number     int64
	Kind       AttemptKind
	Generation uint64
	AtMS       int64
	Phase      AttemptPhase
	Result     string
}

func (s *Store) attemptIdentity(orderID string, number int64, phase AttemptPhase) string {
	return "attempt-" + rebalanceID(s.accountID, "/", orderID, "/", number, "/", phase)
}
func (s *Store) legacyAttemptIdentity(orderID string, number int64, phase AttemptPhase) string {
	return "attempt-" + legacyRebalanceID(s.accountID, "/", orderID, "/", number, "/", phase)
}
func (s *Store) recordOrderAttempt(tx *storeTxn, attempt OrderAttempt) error {
	if !canonicalID(attempt.OrderID) || attempt.Number <= 0 || attempt.AtMS < 0 || attempt.Kind != SubmitAttempt && attempt.Kind != CancelAttempt && attempt.Kind != RecoveryAttempt || attempt.Phase != AttemptStarted && attempt.Phase != AttemptResult && attempt.Phase != AttemptImported {
		return errors.New("execution: invalid order attempt audit")
	}
	body, err := payload(attempt)
	if err != nil {
		return err
	}
	id := s.attemptIdentity(attempt.OrderID, attempt.Number, attempt.Phase)
	if attempt.Phase == AttemptResult {
		id += "-" + rebalanceID(body)
	}
	legacyID := s.legacyAttemptIdentity(attempt.OrderID, attempt.Number, attempt.Phase)
	if attempt.Phase == AttemptResult {
		legacyID += "-" + legacyRebalanceID(body)
	}
	var oldKind, oldBody string
	if err := tx.QueryRow(opReadEvent, s.accountID, legacyID).Scan(&oldKind, &oldBody); err == nil {
		if oldKind != "OrderAttempt" || oldBody != body {
			return errors.New("execution: legacy attempt identity reused with different fact")
		}
		id = legacyID
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	fresh, err := s.recordEvent(tx, id, "OrderAttempt", body)
	if err != nil || !fresh {
		return err
	}
	return s.commitAccountEvent(tx, id, decimal.Zero, nil)
}

func (s *Store) readAttempt(tx *storeTxn, orderID string, number int64, phase AttemptPhase) (OrderAttempt, error) {
	var attempt OrderAttempt
	var kind, body string
	err := tx.QueryRow(opReadEvent, s.accountID, s.attemptIdentity(orderID, number, phase)).Scan(&kind, &body)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRow(opReadEvent, s.accountID, s.legacyAttemptIdentity(orderID, number, phase)).Scan(&kind, &body)
	}
	if err != nil {
		return attempt, err
	}
	if kind != "OrderAttempt" {
		return attempt, errors.New("execution: attempt identity belongs to another event")
	}
	err = json.Unmarshal([]byte(body), &attempt)
	return attempt, err
}

func (s *Store) recordAttemptResult(tx *storeTxn, orderID string, number int64, result string) error {
	order, err := s.readOrder(tx.ctx, tx, orderID)
	if err != nil {
		return err
	}
	if number < 0 {
		number = order.Attempt
	}
	if number == 0 {
		return nil
	}
	attempt, err := s.readAttempt(tx, orderID, number, AttemptStarted)
	if errors.Is(err, sql.ErrNoRows) {
		attempt, err = s.readAttempt(tx, orderID, number, AttemptImported)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// Legacy genesis may contain an attempt highwater without old audit rows.
		// Preserve that gap explicitly rather than inventing a submit timestamp.
		attempt = OrderAttempt{OrderID: orderID, Number: number, Kind: RecoveryAttempt, Generation: order.Generation}
	} else if err != nil {
		return err
	}
	attempt.Phase, attempt.Result = AttemptResult, result
	return s.recordOrderAttempt(tx, attempt)
}

func (s *Store) OrderAttempts(ctx context.Context, orderID string) ([]OrderAttempt, error) {
	if !canonicalID(orderID) {
		return nil, errors.New("execution: invalid order audit identity")
	}
	var attempts []OrderAttempt
	err := s.commit(ctx, func(tx *storeTxn) error {
		rows, err := tx.Query(opListAttemptEvents, s.accountID, orderID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				return err
			}
			var attempt OrderAttempt
			if err := json.Unmarshal([]byte(body), &attempt); err != nil {
				return err
			}
			if attempt.OrderID == orderID {
				attempts = append(attempts, attempt)
			}
		}
		return rows.Err()
	})
	return attempts, err
}
