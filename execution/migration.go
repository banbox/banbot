package execution

import (
	"context"
	"database/sql"
	"encoding/json"

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
