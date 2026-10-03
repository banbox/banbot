package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/shopspring/decimal"
)

func checkedSteps(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b || a+b == math.MinInt64 {
		return 0, errors.New("execution: signed step overflow")
	}
	return a + b, nil
}

func (s *Store) recordEvent(tx *storeTxn, id, kind, body string) (bool, error) {
	var oldKind, oldBody string
	err := tx.QueryRow(opReadEvent, s.accountID, id).Scan(&oldKind, &oldBody)
	if err == nil {
		if kind != oldKind || body != oldBody {
			return false, errors.New("execution: event identity reused with different content")
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = tx.Exec(opInsertEvent, s.accountID, id, kind, body)
	return err == nil, err
}

func (s *Store) ledger(tx *storeTxn, entry LedgerEntry) error {
	_, err := tx.Exec(opInsertPosting, s.accountID, entry.EventID, entry.Kind, string(entry.Strategy), string(entry.Lot), entry.QuantityDelta, entry.CashDelta.String(), entry.Fee.String(), entry.RealizedPnL.String(), entry.AtMS)
	return err
}

func (s *Store) addCash(tx *storeTxn, strategy StrategyID, amount decimal.Decimal) error {
	var value string
	if strategy == "" {
		if err := tx.QueryRow(opReadUnassignedCash, s.accountID).Scan(&value); err != nil {
			return err
		}
		cash, err := decimal.NewFromString(value)
		if err != nil {
			return err
		}
		_, err = tx.Exec(opUpdateUnassignedCash, cash.Add(amount).String(), s.accountID)
		return err
	}
	err := tx.QueryRow(opReadStrategyCash, s.accountID, string(strategy)).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		value = "0"
	} else if err != nil {
		return err
	}
	cash, err := decimal.NewFromString(value)
	if err != nil {
		return err
	}
	_, err = tx.Exec(opPutStrategyCash, s.accountID, string(strategy), cash.Add(amount).String())
	return err
}

func (s *Store) addStrategyTotals(tx *storeTxn, strategy StrategyID, fee, funding decimal.Decimal) error {
	if strategy == "" {
		return nil
	}
	if _, err := tx.Exec(opEnsureStrategy, s.accountID, string(strategy), "0"); err != nil {
		return err
	}
	var feeValue, fundingValue string
	if err := tx.QueryRow(opReadStrategyTotals, s.accountID, string(strategy)).Scan(&feeValue, &fundingValue); err != nil {
		return err
	}
	oldFee, err := decimal.NewFromString(feeValue)
	if err != nil {
		return err
	}
	oldFunding, err := decimal.NewFromString(fundingValue)
	if err != nil {
		return err
	}
	_, err = tx.Exec(opUpdateStrategyTotals, oldFee.Add(fee).String(), oldFunding.Add(funding).String(), s.accountID, string(strategy))
	return err
}

func (s *Store) commitAccountEvent(tx *storeTxn, eventID string, delta decimal.Decimal, freeze *bool) error {
	var value string
	if err := tx.QueryRow(opReadAccountCash, s.accountID).Scan(&value); err != nil {
		return err
	}
	cash, err := decimal.NewFromString(value)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(opUpdateAccountCash, cash.Add(delta).String(), s.accountID); err != nil {
		return err
	}
	if freeze != nil {
		_, err = tx.Exec(opUpdateAccountFrozen, *freeze, s.accountID)
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(opCheckpointEvent, s.accountID, s.accountID, eventID)
	return err
}

func (s *Store) reclassify(tx *storeTxn, eventID string, amount decimal.Decimal, atMS int64) error {
	if amount.IsZero() {
		return nil
	}
	var value string
	if err := tx.QueryRow(opReadPnLReclassification, s.accountID).Scan(&value); err != nil {
		return err
	}
	old, err := decimal.NewFromString(value)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(opUpdatePnLReclassification, old.Add(amount).String(), s.accountID); err != nil {
		return err
	}
	return s.ledger(tx, LedgerEntry{EventID: eventID, Kind: "PnLReclassification", RealizedPnL: amount, AtMS: atMS})
}

func (s *Store) readLot(tx *storeTxn, strategy StrategyID, id VirtualLotID, instrument Instrument) (VirtualLot, error) {
	lot := VirtualLot{Strategy: strategy, ID: id, Instrument: instrument}
	var body string
	err := tx.QueryRow(opReadLot, s.accountID, string(strategy), string(id)).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return lot, nil
	}
	if err != nil {
		return lot, err
	}
	if err := json.Unmarshal([]byte(body), &lot); err != nil {
		return lot, err
	}
	a, _ := payload(instrument)
	b, _ := payload(lot.Instrument)
	if a != b {
		return lot, errors.New("execution: lot instrument/version changed")
	}
	return lot, nil
}

func (s *Store) writeLot(tx *storeTxn, lot VirtualLot) error {
	body, err := payload(lot)
	if err != nil {
		return err
	}
	_, err = tx.Exec(opPutLot, s.accountID, string(lot.Strategy), string(lot.ID), lot.SignedSteps, body)
	return err
}

func (s *Store) actualPosition(tx *storeTxn, instrument Instrument) (VirtualLot, error) {
	position := VirtualLot{ID: VirtualLotID(instrument.ID), Instrument: instrument}
	var body string
	err := tx.QueryRow(opReadActualPosition, s.accountID, instrument.ID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return position, nil
	}
	if err != nil {
		return position, err
	}
	if err := json.Unmarshal([]byte(body), &position); err != nil {
		return position, err
	}
	a, _ := payload(position.Instrument)
	b, _ := payload(instrument)
	if a != b {
		return position, errors.New("execution: actual instrument/version changed")
	}
	return position, nil
}

func (s *Store) writeActual(tx *storeTxn, position VirtualLot) error {
	body, err := payload(position)
	if err != nil {
		return err
	}
	_, err = tx.Exec(opPutActualPosition, s.accountID, position.Instrument.ID, body)
	return err
}

// updateLot accounts linear perpetual PnL: entry notional changes cost basis,
// never cash. Partial closure truncates the basis at declared MoneyScale; the
// exact nonnegative residual stays in the lot until its final closure.
func updateLot(lot *VirtualLot, side OrderSide, kind IntentKind, steps int64, notional, fee decimal.Decimal) (decimal.Decimal, error) {
	delta := steps
	if side == Sell {
		delta = -steps
	}
	old := lot.SignedSteps
	if kind == ExitIntent && (old == 0 || old > 0 && delta > 0 || old < 0 && delta < 0 || steps > absSteps(old)) {
		return decimal.Zero, errors.New("execution: exit exceeds own filled lot")
	}
	next, err := checkedSteps(old, delta)
	if err != nil {
		return decimal.Zero, err
	}
	realized := decimal.Zero
	if old == 0 || old > 0 && delta > 0 || old < 0 && delta < 0 {
		lot.CostBasis = lot.CostBasis.Add(notional)
	} else {
		closed := min(absSteps(old), steps)
		basis := lot.CostBasis
		if closed != absSteps(old) {
			basis, _ = lot.CostBasis.Mul(decimal.NewFromInt(closed)).QuoRem(decimal.NewFromInt(absSteps(old)), lot.Instrument.MoneyScale)
		}
		proceeds := notional
		if closed != steps {
			proceeds, _ = notional.Mul(decimal.NewFromInt(closed)).QuoRem(decimal.NewFromInt(steps), lot.Instrument.MoneyScale)
		}
		realized = proceeds.Sub(basis)
		if old < 0 {
			realized = basis.Sub(proceeds)
		}
		lot.CostBasis = lot.CostBasis.Sub(basis)
		if steps > closed {
			lot.CostBasis = notional.Sub(proceeds)
		}
	}
	lot.SignedSteps = next
	lot.RealizedPnL = lot.RealizedPnL.Add(realized)
	lot.Fees = lot.Fees.Add(fee)
	return realized, nil
}

// ApplyFill rejects mixing incremental and cumulative report streams for one
// order: without a shared exchange watermark that mix cannot be safely deduped.
// Adapters must choose one documented capability and normalize before calling.
// Every accepted fill atomically commits dedup, highwater, frozen allocations,
// lots, cash, fee residual, ledger and account checkpoint.
func (s *Store) ApplyFill(ctx context.Context, report FillReport) (bool, error) {
	if !canonicalID(report.EventID) || !canonicalID(report.OrderID) || report.Steps <= 0 || !report.Price.IsPositive() || report.AtMS < 0 || report.Cost.IsNegative() || report.AuthoritativeSnapshot && (!report.Cumulative || !report.Cost.IsPositive()) {
		return false, errors.New("execution: invalid fill report")
	}
	body, err := payload(report)
	if err != nil {
		return false, err
	}
	applied := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, report.EventID, "ExchangeFill", body)
		if err != nil || !fresh {
			return err
		}
		order, err := s.readOrder(ctx, tx, report.OrderID)
		if err != nil {
			return err
		}
		if order.State == OrderPrepared || order.State == OrderRejected {
			return errors.New("execution: fill for unsent/rejected order")
		}
		_, tickRemainder := report.Price.QuoRem(order.Intent.Instrument.PriceTick, 0)
		if !report.Cumulative && !tickRemainder.IsZero() {
			return errors.New("execution: fill price violates instrument tick")
		}
		mode := "incremental"
		if report.Cumulative {
			mode = "cumulative"
		}
		var oldMode string
		if err := tx.QueryRow(opReadOrderReportMode, s.accountID, report.OrderID).Scan(&oldMode); err != nil {
			return err
		}
		if oldMode != "" && oldMode != mode && !(oldMode == "incremental" && report.AuthoritativeSnapshot) {
			return errors.New("execution: mixed fill report streams require adapter normalization")
		}
		steps, fee := report.Steps, report.Fee
		cost := report.Cost
		if cost.IsZero() {
			cost = order.Intent.Instrument.Notional(report.Steps, report.Price)
		}
		if !report.Cumulative && !cost.Equal(order.Intent.Instrument.Notional(report.Steps, report.Price)) {
			return errors.New("execution: incremental fill cost does not match units/price")
		}
		if report.Cumulative {
			if report.Steps < order.FilledSteps {
				return errors.New("execution: cumulative fill highwater regressed")
			}
			steps -= order.FilledSteps
			fee = fee.Sub(order.ReportedFee)
			cost = cost.Sub(order.ReportedCost)
			if steps == 0 {
				if !cost.IsZero() {
					return errors.New("execution: cumulative cost changed without quantity")
				}
				if !fee.IsZero() {
					if err := s.feeCorrection(tx, order, report, fee); err != nil {
						return err
					}
					applied = true
				}
				if _, err := tx.Exec(opUpdateReportMode, mode, s.accountID, report.OrderID); err != nil {
					return err
				}
				return nil
			}
		}
		if !cost.IsPositive() {
			return errors.New("execution: cumulative cost highwater regressed")
		}
		if steps > order.Intent.Steps-order.FilledSteps {
			return errors.New("execution: fill exceeds frozen real order")
		}
		demands := make(map[string]int64)
		allocations := make(map[string]FillAllocation)
		for _, a := range order.Intent.Allocations {
			var filled int64
			if err := tx.QueryRow(opReadAllocationFilled, s.accountID, report.OrderID, a.ID).Scan(&filled); err != nil {
				return err
			}
			if a.Steps > filled {
				demands[a.ID] = a.Steps - filled
				allocations[a.ID] = a
			}
		}
		assigned, err := AllocateSteps(steps, demands)
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(assigned))
		for id := range assigned {
			keys = append(keys, id)
		}
		sort.Strings(keys)
		realizedTotal, allocatedFee := decimal.Zero, decimal.Zero
		costShares := make(map[string]decimal.Decimal, len(keys))
		costResidual := cost
		moneyUnit := decimal.New(1, -order.Intent.Instrument.MoneyScale)
		last := ""
		for _, id := range keys {
			if assigned[id] > 0 {
				last = id
				part, _ := cost.Mul(decimal.NewFromInt(assigned[id])).QuoRem(decimal.NewFromInt(steps), order.Intent.Instrument.MoneyScale)
				costShares[id] = part
				costResidual = costResidual.Sub(part)
			}
		}
		for _, id := range keys {
			units := assigned[id]
			if units == 0 {
				continue
			}
			a := allocations[id]
			partFee, _ := fee.Mul(decimal.NewFromInt(units)).QuoRem(decimal.NewFromInt(steps), order.Intent.Instrument.MoneyScale)
			lot, err := s.readLot(tx, a.Strategy, a.Lot, order.Intent.Instrument)
			if err != nil {
				return err
			}
			// Whole money units follow stable IDs; the last positive allocation
			// retains any exact subunit residual without rounding cost upward.
			partCost := costShares[id]
			if costResidual.GreaterThanOrEqual(moneyUnit) {
				partCost = partCost.Add(moneyUnit)
				costResidual = costResidual.Sub(moneyUnit)
			}
			if id == last {
				partCost = partCost.Add(costResidual)
			}
			realized, err := updateLot(&lot, a.Side, a.Kind, units, partCost, partFee)
			if err != nil {
				return err
			}
			if err := s.writeLot(tx, lot); err != nil {
				return err
			}
			if _, err := tx.Exec(opIncrementAllocationFilled, units, s.accountID, report.OrderID, id); err != nil {
				return err
			}
			delta := units
			if a.Side == Sell {
				delta = -units
			}
			cash := realized.Sub(partFee)
			if err := s.addCash(tx, a.Strategy, cash); err != nil {
				return err
			}
			if err := s.addStrategyTotals(tx, a.Strategy, partFee, decimal.Zero); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "ExchangeFill", Strategy: a.Strategy, Lot: a.Lot, QuantityDelta: delta, CashDelta: partFee.Neg(), RealizedPnL: realized, Fee: partFee, AtMS: report.AtMS}); err != nil {
				return err
			}
			realizedTotal = realizedTotal.Add(realized)
			allocatedFee = allocatedFee.Add(partFee)
		}
		residual := fee.Sub(allocatedFee)
		if !residual.IsZero() {
			if err := s.addCash(tx, "", residual.Neg()); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "FeeResidual", CashDelta: residual.Neg(), Fee: residual, AtMS: report.AtMS}); err != nil {
				return err
			}
		}
		filled := order.FilledSteps + steps
		if _, err := tx.Exec(opUpdateFillHighwater, filled, order.ReportedFee.Add(fee).String(), order.ReportedCost.Add(cost).String(), mode, s.accountID, report.OrderID); err != nil {
			return err
		}
		state := OrderPartial
		if filled == order.Intent.Steps {
			state = OrderFilled
		} else if order.State == OrderCanceled || order.State == OrderCancelPending || order.State == OrderUnknown {
			state = order.State
		}
		if err := s.setOrderState(tx, report.OrderID, state); err != nil {
			return err
		}
		actual, err := s.actualPosition(tx, order.Intent.Instrument)
		if err != nil {
			return err
		}
		actualRealized, err := updateLot(&actual, order.Intent.Side, EntryIntent, steps, cost, fee)
		if err != nil {
			return err
		}
		if err := s.writeActual(tx, actual); err != nil {
			return err
		}
		if err := s.reclassify(tx, report.EventID, realizedTotal.Sub(actualRealized), report.AtMS); err != nil {
			return err
		}
		if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "RealAccountFill", QuantityDelta: func() int64 {
			if order.Intent.Side == Sell {
				return -steps
			}
			return steps
		}(), CashDelta: actualRealized.Sub(fee), RealizedPnL: actualRealized, Fee: fee, AtMS: report.AtMS}); err != nil {
			return err
		}
		if err := s.commitAccountEvent(tx, report.EventID, actualRealized.Sub(fee), nil); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (s *Store) feeCorrection(tx *storeTxn, order StoredOrder, report FillReport, fee decimal.Decimal) error {
	allocated := decimal.Zero
	for _, a := range order.Intent.Allocations {
		filled := order.AllocationFilled[a.ID]
		if filled == 0 {
			continue
		}
		part, _ := fee.Mul(decimal.NewFromInt(filled)).QuoRem(decimal.NewFromInt(order.FilledSteps), order.Intent.Instrument.MoneyScale)
		lot, err := s.readLot(tx, a.Strategy, a.Lot, order.Intent.Instrument)
		if err != nil {
			return err
		}
		lot.Fees = lot.Fees.Add(part)
		if err := s.writeLot(tx, lot); err != nil {
			return err
		}
		if err := s.addCash(tx, a.Strategy, part.Neg()); err != nil {
			return err
		}
		if err := s.addStrategyTotals(tx, a.Strategy, part, decimal.Zero); err != nil {
			return err
		}
		if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "FeeCorrection", Strategy: a.Strategy, Lot: a.Lot, CashDelta: part.Neg(), Fee: part, AtMS: report.AtMS}); err != nil {
			return err
		}
		allocated = allocated.Add(part)
	}
	residual := fee.Sub(allocated)
	if !residual.IsZero() {
		if err := s.addCash(tx, "", residual.Neg()); err != nil {
			return err
		}
		if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "FeeResidual", CashDelta: residual.Neg(), Fee: residual, AtMS: report.AtMS}); err != nil {
			return err
		}
	}
	actual, err := s.actualPosition(tx, order.Intent.Instrument)
	if err != nil {
		return err
	}
	actual.Fees = actual.Fees.Add(fee)
	if err := s.writeActual(tx, actual); err != nil {
		return err
	}
	if _, err := tx.Exec(opUpdateOrderFee, order.ReportedFee.Add(fee).String(), s.accountID, order.Intent.ID); err != nil {
		return err
	}
	if err := s.ledger(tx, LedgerEntry{EventID: report.EventID, Kind: "RealAccountFeeCorrection", CashDelta: fee.Neg(), Fee: fee, AtMS: report.AtMS}); err != nil {
		return err
	}
	return s.commitAccountEvent(tx, report.EventID, fee.Neg(), nil)
}

func (s *Store) ApplyCashEvent(ctx context.Context, event CashEvent) (bool, error) {
	if !canonicalID(event.ID) || event.AtMS < 0 || len(event.Postings) == 0 {
		return false, errors.New("execution: invalid cash event")
	}
	sum := decimal.Zero
	seen := make(map[StrategyID]bool)
	for _, p := range event.Postings {
		if seen[p.Strategy] || p.Strategy != "" && !canonicalID(string(p.Strategy)) {
			return false, errors.New("execution: invalid cash posting identity")
		}
		seen[p.Strategy] = true
		sum = sum.Add(p.Amount)
	}
	if !sum.Equal(event.AccountDelta) {
		return false, errors.New("execution: cash postings do not conserve account delta")
	}
	var freeze *bool
	switch event.Kind {
	case CapitalTransfer:
		if !event.AccountDelta.IsZero() || len(event.Postings) < 2 {
			return false, errors.New("execution: capital transfer requires a conserving counterparty")
		}
	case ExternalCashChange, Liquidation:
		if len(event.Postings) != 1 || event.Postings[0].Strategy != "" {
			return false, errors.New("execution: external cash must remain unassigned")
		}
		v := true
		freeze = &v
	case Fee, Funding:
	case Reconciliation: // cash reconciliation alone does not unfreeze unknown positions/orders
	default:
		return false, errors.New("execution: unsupported cash event kind")
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
		for _, p := range event.Postings {
			if err := s.addCash(tx, p.Strategy, p.Amount); err != nil {
				return err
			}
			fee := decimal.Zero
			if event.Kind == Fee {
				fee = p.Amount.Neg()
			}
			funding := decimal.Zero
			if event.Kind == Funding {
				funding = p.Amount
			}
			if err := s.addStrategyTotals(tx, p.Strategy, fee, funding); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: event.ID, Kind: string(event.Kind), Strategy: p.Strategy, CashDelta: p.Amount, Fee: fee, AtMS: event.AtMS}); err != nil {
				return err
			}
		}
		if err := s.commitAccountEvent(tx, event.ID, event.AccountDelta, freeze); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (s *Store) Snapshot(ctx context.Context) (AccountSnapshot, error) {
	snapshot := AccountSnapshot{SyntheticStrategyCash: make(map[StrategyID]decimal.Decimal)}
	err := s.commit(ctx, func(tx *storeTxn) error {
		var cash, unassigned, reclassification string
		if err := tx.QueryRow(opReadAccountSnapshot, s.accountID).Scan(&cash, &unassigned, &reclassification, &snapshot.RiskFrozen, &snapshot.Checkpoint); err != nil {
			return err
		}
		var err error
		snapshot.AccountSettledCash, err = decimal.NewFromString(cash)
		if err != nil {
			return err
		}
		snapshot.UnassignedCash, err = decimal.NewFromString(unassigned)
		if err != nil {
			return err
		}
		snapshot.PnLReclassification, err = decimal.NewFromString(reclassification)
		if err != nil {
			return err
		}
		rows, err := tx.Query(opListStrategyCash, s.accountID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id StrategyID
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				rows.Close()
				return err
			}
			d, err := decimal.NewFromString(value)
			if err != nil {
				rows.Close()
				return err
			}
			snapshot.SyntheticStrategyCash[id] = d
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(opListActiveLots, s.accountID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				rows.Close()
				return err
			}
			var lot VirtualLot
			if err := json.Unmarshal([]byte(body), &lot); err != nil {
				rows.Close()
				return err
			}
			snapshot.Lots = append(snapshot.Lots, lot)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(opListActiveOrders, s.accountID)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range ids {
			order, err := s.readOrder(ctx, tx, id)
			if err != nil {
				return err
			}
			snapshot.Orders = append(snapshot.Orders, order)
		}
		rows, err = tx.Query(opListActualPositions, s.accountID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				rows.Close()
				return err
			}
			var position VirtualLot
			if err := json.Unmarshal([]byte(body), &position); err != nil {
				rows.Close()
				return err
			}
			snapshot.ActualPositions = append(snapshot.ActualPositions, position)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(opListExternalPositions, s.accountID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				rows.Close()
				return err
			}
			var lot VirtualLot
			if err := json.Unmarshal([]byte(body), &lot); err != nil {
				rows.Close()
				return err
			}
			snapshot.ExternalPositions = append(snapshot.ExternalPositions, lot)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		return nil
	})
	return snapshot, err
}
