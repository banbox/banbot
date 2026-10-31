package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"

	"github.com/shopspring/decimal"
)

// ExternalAttribution explicitly assigns the complete quarantined instrument
// book, including its historical economics, to one previously unused lot.
type ExternalAttribution struct {
	ID                  string
	Strategy            StrategyID
	Lot                 VirtualLotID
	Instrument          Instrument
	ExpectedSignedSteps int64
	AtMS                int64
}

// AttributeExternal changes attribution only. A separate authoritative
// reconciliation is still required before account risk can be unfrozen.
func (s *Store) AttributeExternal(ctx context.Context, request ExternalAttribution) (bool, error) {
	if err := request.Instrument.Validate(); err != nil {
		return false, err
	}
	if !canonicalID(request.ID) || !canonicalID(string(request.Strategy)) || !canonicalID(string(request.Lot)) || request.ExpectedSignedSteps == 0 || request.ExpectedSignedSteps == math.MinInt64 || request.AtMS < 0 {
		return false, errors.New("execution: invalid external attribution")
	}
	body, err := payload(request)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.atomically(ctx, func(ctx context.Context) error {
		return s.commit(ctx, func(tx *storeTxn) error {
			fresh, err := s.recordEvent(tx, request.ID, "ExternalAttribution", body)
			if err != nil || !fresh {
				return err
			}
			policy, err := s.AccountPolicy(ctx)
			if err != nil {
				return err
			}
			if !policy.StrategyGrossLimits[request.Strategy].IsPositive() || policy.Currency != request.Instrument.SettlementCurrency {
				return errors.New("execution: external attribution requires registered strategy risk policy")
			}
			if err := s.rejectSettledOption(tx, request.Instrument); err != nil {
				return err
			}
			var previous string
			err = tx.QueryRow(opReadLot, s.accountID, string(request.Strategy), string(request.Lot)).Scan(&previous)
			if err == nil {
				return errors.New("execution: external attribution destination lot already exists")
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			snapshot, err := s.Snapshot(ctx)
			if err != nil {
				return err
			}
			for _, order := range snapshot.Orders {
				if order.Intent.Instrument.ID == request.Instrument.ID || order.State == OrderUnknown || order.State == OrderSending || order.State == OrderCancelPending {
					return errors.New("execution: active or uncertain order blocks external attribution")
				}
			}
			var externalBody string
			if err := tx.QueryRow(opReadExternalPosition, s.accountID, request.Instrument.ID).Scan(&externalBody); err != nil {
				return err
			}
			var external VirtualLot
			if err := json.Unmarshal([]byte(externalBody), &external); err != nil {
				return err
			}
			instrumentBody, err := payload(external.Instrument)
			if err != nil {
				return err
			}
			requestedBody, _ := payload(request.Instrument)
			if instrumentBody != requestedBody || external.SignedSteps != request.ExpectedSignedSteps {
				return errors.New("execution: external attribution inventory or instrument mismatch")
			}
			lot := external
			lot.Strategy, lot.ID = request.Strategy, request.Lot
			if err := s.writeLot(tx, lot); err != nil {
				return err
			}
			// Transfer only the cash flows represented by this book, not unrelated
			// capital or reconciliation balances held in unassigned cash.
			cash := external.RealizedPnL.Sub(external.Fees).Add(external.Funding)
			if external.Instrument.IsOption() {
				if external.SignedSteps > 0 {
					cash = cash.Sub(external.CostBasis)
				} else {
					cash = cash.Add(external.CostBasis)
				}
			}
			if err := s.addCash(tx, "", cash.Neg()); err != nil {
				return err
			}
			if err := s.addCash(tx, request.Strategy, cash); err != nil {
				return err
			}
			if err := s.addStrategyTotals(tx, request.Strategy, external.Fees, external.Funding); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: request.ID, Kind: "ExternalAttribution", Strategy: request.Strategy, Lot: request.Lot, QuantityDelta: external.SignedSteps, CashDelta: cash, RealizedPnL: external.RealizedPnL, Fee: external.Fees, AtMS: request.AtMS}); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: request.ID, Kind: "ExternalAttribution", QuantityDelta: -external.SignedSteps, CashDelta: cash.Neg(), RealizedPnL: external.RealizedPnL.Neg(), Fee: external.Fees.Neg(), AtMS: request.AtMS}); err != nil {
				return err
			}
			// Retain an empty descriptor so later external fills start a new book
			// rather than transferring these historical cash flows a second time.
			empty := VirtualLot{ID: external.ID, Instrument: external.Instrument}
			emptyBody, err := payload(empty)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(opPutExternalPosition, s.accountID, request.Instrument.ID, int64(0), emptyBody); err != nil {
				return err
			}
			if err := s.commitAccountEvent(tx, request.ID, decimal.Zero, nil); err != nil {
				return err
			}
			applied = true
			return nil
		})
	})
	return applied && err == nil, err
}

func (b *SharedAccountBorrow) AttributeExternal(request ExternalAttribution) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			var err error
			applied, err = s.store.AttributeExternal(ctx, request)
			return err
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied, err
}
