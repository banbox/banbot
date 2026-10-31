package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
)

const autoTakeoverStrategy StrategyID = "execution-takeover"

func takeoverSnapshotMatches(local AccountSnapshot, cash decimal.Decimal, positions map[string]int64) bool {
	if !local.AccountSettledCash.Equal(cash) {
		return false
	}
	seen := make(map[string]bool)
	for _, actual := range local.ActualPositions {
		if positions[actual.Instrument.ID] != actual.SignedSteps {
			return false
		}
		seen[actual.Instrument.ID] = true
	}
	for instrument, quantity := range positions {
		if quantity != 0 && !seen[instrument] {
			return false
		}
	}
	return true
}

// AutoTakeoverRecord preserves adoption provenance for strategy projections.
// Position contains the actual transferred economics, not an inferred snapshot.
type AutoTakeoverRecord struct {
	Attribution   ExternalAttribution
	Position      VirtualLot
	SourceEventID string
}

func (s *Store) AutoTakeoverRecords(ctx context.Context) ([]AutoTakeoverRecord, error) {
	body, err := s.StrategyCheckpoint(ctx, autoTakeoverStrategy, "records-v1")
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var records []AutoTakeoverRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, err
	}
	return records, nil
}

// takeoverSource proves a complete opening/addition book from immutable trades.
// Opposing manual fills and liquidations require explicit human attribution;
// neither a venue position snapshot nor matching net quantity supplies basis.
func (s *Store) takeoverSource(ctx context.Context, external VirtualLot) (string, bool, error) {
	var cursor int64
	var source string
	var steps int64
	cost, fees := decimal.Zero, decimal.Zero
	blocked := false
	owned := make(map[[2]string]int64)
	for {
		page, err := s.EventsAfter(ctx, cursor, 512)
		if err != nil {
			return "", false, err
		}
		if len(page) == 0 {
			break
		}
		for _, event := range page {
			if event.Kind == "ExternalAttribution" {
				var attribution ExternalAttribution
				if err := json.Unmarshal(event.Payload, &attribution); err != nil {
					return "", false, err
				}
				if attribution.Instrument.ID == external.Instrument.ID {
					source, steps, cost, fees, blocked = "", 0, decimal.Zero, decimal.Zero, false
				}
			}
			if event.Kind == string(ExternalCashChange) || event.Kind == string(Liquidation) {
				var trade ExternalPositionEvent
				if err := json.Unmarshal(event.Payload, &trade); err != nil {
					return "", false, err
				}
				if trade.Instrument.ID == external.Instrument.ID && trade.Steps > 0 {
					if !sameAccountInstrument(trade.Instrument, external.Instrument) || trade.Kind != ExternalCashChange || trade.Side == Buy != (external.SignedSteps > 0) {
						blocked = true
					}
					for _, quantity := range owned {
						if quantity != 0 && quantity > 0 != (external.SignedSteps > 0) {
							blocked = true
						}
					}
					if trade.Side == Buy {
						steps += trade.Steps
					} else {
						steps -= trade.Steps
					}
					cost = cost.Add(trade.Instrument.Notional(trade.Steps, trade.Price))
					fees = fees.Add(trade.Fee)
					source = trade.ID
				}
			}
			for _, entry := range event.Ledger {
				if entry.Strategy == "" || entry.QuantityDelta == 0 {
					continue
				}
				instrument, err := fundingActivityInstrument(ctx, s, event)
				if err != nil {
					// An older untyped quantity source cannot prove ownership history.
					return "", false, nil
				}
				if instrument.ID == external.Instrument.ID {
					owned[[2]string{string(entry.Strategy), string(entry.Lot)}] += entry.QuantityDelta
				}
			}
		}
		cursor = page[len(page)-1].Checkpoint
	}
	return source, !blocked && source != "" && steps == external.SignedSteps && cost.Equal(external.CostBasis) && fees.Equal(external.Fees) && external.RealizedPnL.IsZero(), nil
}

// AutoTakeoverExternal only changes ownership of fully evidenced quarantined
// books. The caller still needs a separate authoritative reconciliation before
// sending; no snapshot-only inventory or unknown live order is adopted here.
func (s *Store) AutoTakeoverExternal(ctx context.Context, strategy StrategyID, now int64) ([]AutoTakeoverRecord, error) {
	if strategy == "" {
		return nil, nil
	}
	if !canonicalID(string(strategy)) || now < 0 {
		return nil, errors.New("execution: invalid automatic takeover owner or time")
	}
	var adopted []AutoTakeoverRecord
	err := s.atomically(ctx, func(ctx context.Context) error {
		snapshot, err := s.Snapshot(ctx)
		if err != nil {
			return err
		}
		if len(snapshot.Orders) > 0 {
			return nil // Active or uncertain orders need a separate ownership decision.
		}
		records, err := s.AutoTakeoverRecords(ctx)
		if err != nil {
			return err
		}
		book, err := s.LoadPendingBook(ctx)
		if err != nil {
			return err
		}
		sort.Slice(snapshot.ExternalPositions, func(i, j int) bool {
			return snapshot.ExternalPositions[i].Instrument.ID < snapshot.ExternalPositions[j].Instrument.ID
		})
		for _, external := range snapshot.ExternalPositions {
			if external.SignedSteps == 0 || external.Instrument.IsOption() {
				continue
			}
			source, proven, err := s.takeoverSource(ctx, external)
			if err != nil {
				return err
			}
			if !proven {
				continue
			}
			identity, _ := json.Marshal([]string{string(strategy), external.Instrument.ID, source})
			hash := fmt.Sprintf("%x", sha256.Sum256(identity))
			request := ExternalAttribution{ID: "auto-takeover/" + hash, Strategy: strategy, Lot: VirtualLotID("external/" + hash), Instrument: external.Instrument, ExpectedSignedSteps: external.SignedSteps, AtMS: now}
			applied, err := s.AttributeExternal(ctx, request)
			if err != nil {
				return err
			}
			if !applied {
				return errors.New("execution: automatic takeover source was already consumed")
			}
			side := Buy
			if external.SignedSteps < 0 {
				side = Sell
			}
			quantity := absSteps(external.SignedSteps)
			entry := EligibleIntent{ID: VirtualIntentID("external-entry/" + hash), Account: s.Account(), Strategy: strategy, Lot: request.Lot, Instrument: external.Instrument.ID, Kind: EntryIntent, Side: side, QuantitySteps: quantity, FilledSteps: quantity, State: Filled}
			book = append(book, PendingDefinition{Order: PendingOrder{Strategy: strategy, Lot: request.Lot, Entry: entry, Desired: external.SignedSteps}, Instrument: external.Instrument})
			record := AutoTakeoverRecord{Attribution: request, Position: external, SourceEventID: source}
			records, adopted = append(records, record), append(adopted, record)
		}
		if len(adopted) == 0 {
			return nil
		}
		checkpoint, err := PendingBookCheckpoint(book)
		if err != nil {
			return err
		}
		if err := s.saveAcceptedCheckpoint(ctx, checkpoint); err != nil {
			return err
		}
		body, err := json.Marshal(records)
		if err != nil {
			return err
		}
		return s.SaveStrategyCheckpoint(ctx, autoTakeoverStrategy, "records-v1", body)
	})
	if err != nil {
		return nil, err
	}
	return adopted, nil
}
