package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/shopspring/decimal"
)

const LegacyMigrationSchemaVersion = 1

type LegacySourceMapping struct {
	Strategy   StrategyID
	Lot        VirtualLotID
	TaskID     int64
	IOrderID   int64
	ExOrderIDs []string
	TriggerIDs []string
	RawJSON    json.RawMessage
}

type LegacyCheckpoint struct {
	Strategy StrategyID
	Name     string
	Payload  json.RawMessage
}

// LegacyVenueOrder is normalized by the authoritative adapter against the same
// immutable instrument catalog as the import. Instrument is its stable ID;
// Steps/FilledSteps use that descriptor's QuantityStep and Cost/Fee use its
// settlement currency. Raw venue symbols or contract units are never accepted.
// Import verifies exact order identity, normalized quantities and economics
// against the mapped durable OrderIntent; catalog changes require a new import.
type LegacyVenueOrder struct {
	ExchangeID  string
	ClientID    string
	Instrument  string
	Side        OrderSide
	Steps       int64
	FilledSteps int64
	Cost        decimal.Decimal // authoritative cumulative fill settlement notional
	Fee         decimal.Decimal // authoritative cumulative settlement-currency fee
}

type LegacyVenueSnapshot struct {
	Complete    bool // authoritative cash, positions and the full open-order inventory
	AccountCash decimal.Decimal
	Positions   []VirtualLot // exact actual basis; strategy/id are ignored
	OpenOrders  []LegacyVenueOrder
	AtMS        int64
}

type LegacyMigration struct {
	Preflight         func() error `json:"-"` // caller verifies stop/join, account claim and snapshot capability
	ID                string
	SourceVersion     string
	SourceSnapshotID  string
	StoppedOwnerProof string // evidence from the stopped legacy owner, verified by caller
	BackupPath        string // caller-created immutable backup; import never modifies it
	BackupSHA256      string // binds the exact preserved source backup bytes
	AccountCash       decimal.Decimal
	StrategyCash      map[StrategyID]decimal.Decimal
	UnassignedCash    decimal.Decimal
	Lots              []VirtualLot
	ActualPositions   []VirtualLot
	Plans             []Plan
	Orders            []StoredOrder
	Checkpoints       []LegacyCheckpoint
	RawLegacyMap      []LegacySourceMapping
	VenueSnapshot     LegacyVenueSnapshot
}

type MigrationCheckpoint struct {
	ID            string
	SourceHash    string
	SourceVersion string
	SchemaVersion int
	State         string // pending survives every import failure; ready is committed with all books
}

func (s *Store) Migration(ctx context.Context, id string) (MigrationCheckpoint, error) {
	var result MigrationCheckpoint
	err := s.commit(ctx, func(tx *storeTxn) error {
		present, err := tableExists(tx, "exec_migration")
		if err != nil {
			return err
		}
		if !present {
			return sql.ErrNoRows
		}
		return tx.QueryRow(opReadMigration, s.accountID, id).Scan(&result.ID, &result.SourceHash, &result.SourceVersion, &result.SchemaVersion, &result.State)
	})
	return result, err
}

func (s *Store) LegacySource(ctx context.Context, strategy StrategyID, lot VirtualLotID) (LegacySourceMapping, error) {
	var mapping LegacySourceMapping
	err := s.commit(ctx, func(tx *storeTxn) error {
		present, err := tableExists(tx, "exec_legacy_source")
		if err != nil {
			return err
		}
		if !present {
			return sql.ErrNoRows
		}
		var body string
		if err := tx.QueryRow(opReadLegacySource, s.accountID, string(strategy), string(lot)).Scan(&body); err != nil {
			return err
		}
		return json.Unmarshal([]byte(body), &mapping)
	})
	return mapping, err
}

// ImportMigration accepts a complete, already-attributed source snapshot. The
// separate pending commit fences admission before verification. All destination
// books, source identities, projection metadata and ready status commit together;
// legacy tables and the caller's backup are never changed.
func (s *Store) ImportMigration(ctx context.Context, request LegacyMigration) (bool, error) {
	if !canonicalID(request.ID) || !canonicalID(request.SourceVersion) || !canonicalID(request.SourceSnapshotID) {
		return false, errors.New("execution: migration identity/version/snapshot required")
	}
	source := request
	source.VenueSnapshot = LegacyVenueSnapshot{}
	body, err := payload(source)
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256([]byte(body))
	sourceHash := hex.EncodeToString(hash[:])
	alreadyReady := false
	err = s.commit(ctx, func(tx *storeTxn) error {
		if err := ensureMigrationSchema(tx); err != nil {
			return err
		}
		var existing, state string
		err := tx.QueryRow(opReadMigrationHash, s.accountID, request.ID).Scan(&existing, &state)
		if err == nil {
			if existing != sourceHash {
				return errors.New("execution: migration identity reused with different legacy source")
			}
			alreadyReady = state == "ready"
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.QueryRow(opCountMigrations, s.accountID).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("execution: account already has a migration checkpoint")
		}
		if _, err := tx.Exec(opInsertMigration, s.accountID, request.ID, sourceHash, request.SourceVersion, LegacyMigrationSchemaVersion, body); err != nil {
			return err
		}
		_, err = tx.Exec(opFreezeAccount, s.accountID)
		return err
	})
	if err != nil || alreadyReady {
		return false, err
	}
	if err := s.validateLegacyMigration(request); err != nil {
		return false, err
	}
	if request.Preflight == nil {
		return false, errors.New("execution: migration requires verified cutover preflight")
	}
	if err := request.Preflight(); err != nil {
		return false, err
	}
	applied := false
	err = s.atomically(ctx, func(ctx context.Context) error {
		return s.commit(ctx, func(tx *storeTxn) error {
			var state, currentHash string
			if err := tx.QueryRow(opReadMigrationState, s.accountID, request.ID).Scan(&state, &currentHash); err != nil {
				return err
			}
			if currentHash != sourceHash {
				return errors.New("execution: migration source changed")
			}
			if state == "ready" {
				return nil
			}
			var count int
			if err := tx.QueryRow(opCountMigrationDestination, s.accountID, s.accountID, s.accountID, s.accountID, s.accountID, s.accountID, s.accountID).Scan(&count); err != nil {
				return err
			}
			var cash, unassigned, reclassification string
			var checkpoint int64
			if err := tx.QueryRow(opReadAccountGenesis, s.accountID).Scan(&cash, &unassigned, &reclassification, &checkpoint); err != nil {
				return err
			}
			if count != 0 || cash != "0" || unassigned != "0" || reclassification != "0" || checkpoint != 0 {
				return errors.New("execution: migration requires an empty destination; no partial takeover")
			}
			for _, plan := range request.Plans {
				if err := s.SavePlan(ctx, plan); err != nil {
					return err
				}
			}
			for strategy, amount := range request.StrategyCash {
				if _, err := tx.Exec(opInsertStrategy, s.accountID, string(strategy), amount.String()); err != nil {
					return err
				}
			}
			for _, lot := range request.Lots {
				b, err := payload(lot)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(opInsertLot, s.accountID, string(lot.Strategy), string(lot.ID), lot.SignedSteps, b); err != nil {
					return err
				}
				if err := s.addStrategyTotals(tx, lot.Strategy, lot.Fees, lot.Funding); err != nil {
					return err
				}
			}
			for _, position := range request.ActualPositions {
				b, err := payload(position)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(opInsertActualPosition, s.accountID, position.Instrument.ID, b); err != nil {
					return err
				}
			}
			for _, order := range request.Orders {
				if err := s.importLegacyOrder(ctx, tx, order); err != nil {
					return err
				}
			}
			for _, checkpoint := range request.Checkpoints {
				if err := s.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
					return err
				}
			}
			for _, mapping := range request.RawLegacyMap {
				b, err := payload(mapping)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(opInsertLegacySource, s.accountID, request.ID, string(mapping.Strategy), string(mapping.Lot), b); err != nil {
					return err
				}
			}
			synthetic := request.UnassignedCash
			for _, cash := range request.StrategyCash {
				synthetic = synthetic.Add(cash)
			}
			if _, err := tx.Exec(opUpdateAccountGenesis, request.AccountCash.String(), request.UnassignedCash.String(), synthetic.Sub(request.AccountCash).String(), s.accountID); err != nil {
				return err
			}
			eventID := "migration-" + request.ID
			if _, err := s.recordEvent(tx, eventID, "LegacyMigration", body); err != nil {
				return err
			}
			freeze := false
			if err := request.Preflight(); err != nil {
				return err
			}
			if err := validateMigrationBackup(request); err != nil {
				return err
			}
			if err := s.commitAccountEvent(tx, eventID, decimal.Zero, &freeze); err != nil {
				return err
			}
			if _, err := tx.Exec(opFinishMigration, s.accountID, request.ID); err != nil {
				return err
			}
			applied = true
			return nil
		})
	})
	return applied, err
}

func (s *Store) importLegacyOrder(ctx context.Context, tx *storeTxn, order StoredOrder) error {
	intent := order.Intent
	if err := intent.Instrument.Validate(); err != nil {
		return err
	}
	if !canonicalID(intent.ID) || !canonicalID(intent.PlanID) || intent.Steps <= 0 || intent.Side != Buy && intent.Side != Sell || intent.Limit.IsNegative() {
		return errors.New("execution: invalid legacy frozen real order")
	}
	var total int64
	seen := make(map[string]bool)
	for _, allocation := range intent.Allocations {
		if !canonicalID(allocation.ID) || seen[allocation.ID] || allocation.Side != intent.Side || allocation.Steps <= 0 || allocation.Steps > intent.Steps-total {
			return errors.New("execution: invalid legacy frozen allocation")
		}
		seen[allocation.ID] = true
		total += allocation.Steps
		var b string
		if err := tx.QueryRow(opReadPlanIntentDefinition, s.accountID, string(allocation.IntentID), intent.PlanID).Scan(&b); err != nil {
			return err
		}
		var virtual EligibleIntent
		if err := json.Unmarshal([]byte(b), &virtual); err != nil {
			return err
		}
		if virtual.Strategy != allocation.Strategy || virtual.Lot != allocation.Lot || virtual.Side != allocation.Side || virtual.Kind != allocation.Kind || virtual.Instrument != intent.Instrument.ID {
			return errors.New("execution: legacy order allocation mismatches frozen intent")
		}
		var allocated int64
		if err := tx.QueryRow(opReadAllocatedSteps, s.accountID, string(allocation.IntentID)).Scan(&allocated); err != nil {
			return err
		}
		if allocation.Steps > virtual.QuantitySteps-allocated {
			return errors.New("execution: legacy intent overallocated")
		}
	}
	if total != intent.Steps {
		return errors.New("execution: legacy allocations do not total real order")
	}
	body, err := payload(intent)
	if err != nil {
		return err
	}
	_, err = tx.Exec(opInsertImportedOrder, s.accountID, intent.ID, intent.PlanID, body, order.ClientID, order.ExchangeID, string(order.State), order.FilledSteps, order.ReportedFee.String(), order.ReportedCost.String(), order.Attempt, order.Generation)
	if err != nil {
		return err
	}
	for _, allocation := range order.Intent.Allocations {
		if _, err := tx.Exec(opInsertImportedAllocation, s.accountID, intent.ID, allocation.ID, string(allocation.IntentID), string(allocation.Strategy), string(allocation.Lot), allocation.Steps, order.AllocationFilled[allocation.ID]); err != nil {
			return err
		}
	}
	return s.recordOrderState(tx, order.Intent.ID)
}

func (s *Store) validateLegacyMigration(request LegacyMigration) error {
	if !canonicalID(request.StoppedOwnerProof) || !filepath.IsAbs(request.BackupPath) {
		return errors.New("execution: stopped owner evidence and absolute backup required")
	}
	if err := validateMigrationBackup(request); err != nil {
		return err
	}
	var err error
	venue := request.VenueSnapshot
	if !venue.Complete || venue.AtMS < 0 || !venue.AccountCash.Equal(request.AccountCash) {
		return errors.New("execution: complete matching venue cash snapshot required")
	}
	type lotKey struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	lots := make(map[lotKey]VirtualLot)
	net := make(map[string]int64)
	unitsByID := make(map[string]string)
	virtualBasis := decimal.Zero
	for _, lot := range request.Lots {
		key := lotKey{lot.Strategy, lot.ID}
		if err := lot.Instrument.Validate(); err != nil {
			return err
		}
		units, _ := payload(lot.Instrument)
		if previous, ok := unitsByID[lot.Instrument.ID]; ok && previous != units {
			return errors.New("execution: legacy instrument units/version mismatch")
		}
		unitsByID[lot.Instrument.ID] = units
		if !canonicalID(string(lot.Strategy)) || !canonicalID(string(lot.ID)) || lot.SignedSteps == -1<<63 || lot.CostBasis.IsNegative() || lot.SignedSteps == 0 && !lot.CostBasis.IsZero() {
			return errors.New("execution: invalid legacy virtual lot")
		}
		if _, ok := lots[key]; ok {
			return errors.New("execution: duplicate legacy lot")
		}
		if _, ok := request.StrategyCash[lot.Strategy]; !ok {
			return errors.New("execution: legacy lot lacks strategy cash")
		}
		lots[key] = lot
		net[lot.Instrument.ID], err = checkedSteps(net[lot.Instrument.ID], lot.SignedSteps)
		if err != nil {
			return err
		}
		basis := lot.CostBasis
		if lot.SignedSteps < 0 {
			basis = basis.Neg()
		}
		virtualBasis = virtualBasis.Add(basis)
	}
	mapped := make(map[lotKey]bool)
	for _, mapping := range request.RawLegacyMap {
		key := lotKey{mapping.Strategy, mapping.Lot}
		if _, ok := lots[key]; !ok || mapped[key] || mapping.TaskID <= 0 || mapping.IOrderID <= 0 || !json.Valid(mapping.RawJSON) {
			return errors.New("execution: complete unique legacy source attribution required")
		}
		mapped[key] = true
	}
	if len(mapped) != len(lots) {
		return errors.New("execution: legacy lot missing source attribution")
	}
	actual, err := migrationPositions(request.ActualPositions)
	if err != nil {
		return err
	}
	proven, err := migrationPositions(venue.Positions)
	if err != nil {
		return err
	}
	actualBasis := decimal.Zero
	for id, position := range actual {
		other, ok := proven[id]
		units, _ := payload(position.Instrument)
		if previous, ok := unitsByID[id]; ok && previous != units {
			return errors.New("execution: virtual/actual instrument units/version mismatch")
		}
		otherUnits, _ := payload(other.Instrument)
		if !ok || other.SignedSteps != position.SignedSteps || !other.CostBasis.Equal(position.CostBasis) || otherUnits != units {
			return errors.New("execution: actual position does not match venue snapshot")
		}
		if net[id] != position.SignedSteps {
			return errors.New("execution: unmatched legacy virtual/actual net")
		}
		delete(net, id)
		basis := position.CostBasis
		if position.SignedSteps < 0 {
			basis = basis.Neg()
		}
		actualBasis = actualBasis.Add(basis)
	}
	if len(actual) != len(proven) {
		return errors.New("execution: unexplained venue position")
	}
	for _, n := range net {
		if n != 0 {
			return errors.New("execution: legacy exposure absent from actual snapshot")
		}
	}
	synthetic := request.UnassignedCash
	for strategy, amount := range request.StrategyCash {
		if !canonicalID(string(strategy)) {
			return errors.New("execution: invalid strategy cash attribution")
		}
		synthetic = synthetic.Add(amount)
	}
	if !synthetic.Sub(request.AccountCash).Equal(virtualBasis.Sub(actualBasis)) {
		return errors.New("execution: legacy actual/virtual equity does not reconcile")
	}
	orders := make(map[string]StoredOrder)
	for _, order := range request.Orders {
		if order.State != OrderAcknowledged && order.State != OrderPartial || !canonicalID(order.ExchangeID) || !canonicalID(order.ClientID) || order.FilledSteps < 0 || order.FilledSteps >= order.Intent.Steps || order.ReportedCost.IsNegative() {
			return errors.New("execution: only completely attributed confirmed active legacy orders may migrate")
		}
		if _, ok := orders[order.ExchangeID]; ok {
			return errors.New("execution: duplicate legacy exchange order")
		}
		var filled int64
		for _, allocation := range order.Intent.Allocations {
			key := lotKey{allocation.Strategy, allocation.Lot}
			if _, ok := lots[key]; !ok {
				return errors.New("execution: legacy active allocation lacks lot attribution")
			}
			attributed := false
			for _, mapping := range request.RawLegacyMap {
				if mapping.Strategy != key.strategy || mapping.Lot != key.lot {
					continue
				}
				for _, id := range append(append([]string(nil), mapping.ExOrderIDs...), mapping.TriggerIDs...) {
					if id == order.ExchangeID {
						attributed = true
					}
				}
			}
			if !attributed {
				return errors.New("execution: active order missing legacy exchange source ID")
			}
			n := order.AllocationFilled[allocation.ID]
			if n < 0 || n > allocation.Steps {
				return errors.New("execution: invalid legacy allocation highwater")
			}
			filled, err = checkedSteps(filled, n)
			if err != nil {
				return err
			}
		}
		if filled != order.FilledSteps {
			return errors.New("execution: legacy allocation fills do not match order")
		}
		orders[order.ExchangeID] = order
	}
	seen := make(map[string]bool)
	for _, open := range venue.OpenOrders {
		order, ok := orders[open.ExchangeID]
		if !ok || seen[open.ExchangeID] || open.ClientID != order.ClientID || open.Instrument != order.Intent.Instrument.ID || open.Side != order.Intent.Side || open.Steps != order.Intent.Steps || open.FilledSteps != order.FilledSteps || !open.Cost.Equal(order.ReportedCost) || !open.Fee.Equal(order.ReportedFee) {
			return errors.New("execution: unmatched authoritative venue open order")
		}
		seen[open.ExchangeID] = true
	}
	if len(seen) != len(orders) {
		return errors.New("execution: legacy active order absent from authoritative venue inventory")
	}
	return nil
}

func validateMigrationBackup(request LegacyMigration) error {
	if !filepath.IsAbs(request.BackupPath) || len(request.BackupSHA256) != 64 {
		return errors.New("execution: absolute legacy backup and SHA256 required")
	}
	file, err := os.Open(request.BackupPath)
	if err != nil {
		return errors.New("execution: preserved legacy backup unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("execution: preserved legacy backup must be regular file")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != request.BackupSHA256 {
		return errors.New("execution: preserved legacy backup digest mismatch")
	}
	return nil
}

func migrationPositions(positions []VirtualLot) (map[string]VirtualLot, error) {
	result := make(map[string]VirtualLot)
	for _, position := range positions {
		if err := position.Instrument.Validate(); err != nil {
			return nil, err
		}
		if position.SignedSteps == -1<<63 || position.CostBasis.IsNegative() || position.SignedSteps == 0 && !position.CostBasis.IsZero() {
			return nil, errors.New("execution: invalid migration actual position")
		}
		if _, ok := result[position.Instrument.ID]; ok {
			return nil, errors.New("execution: duplicate migration actual position")
		}
		result[position.Instrument.ID] = position
	}
	return result, nil
}
