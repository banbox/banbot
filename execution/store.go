package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"
)

// Store is the account-scoped execution record facade for durable SQLite or
// process-local memory. AccountHandle serializes mutations and network attempts.
type Store struct {
	db           *sql.DB
	memory       *memoryState
	key          AccountKey
	accountID    string
	mu           sync.Mutex
	closed       bool
	closing      bool
	work         sync.WaitGroup
	closeOnce    sync.Once
	closeErr     error
	releaseLease func() error
}

type storeTxContextKey struct{}
type scopedStoreTx struct {
	store *Store
	tx    *storeTxn
}

func (s *Store) atomically(ctx context.Context, fn func(context.Context) error) error {
	return s.commit(ctx, func(tx *storeTxn) error { return fn(context.WithValue(ctx, storeTxContextKey{}, scopedStoreTx{s, tx})) })
}

const executionSchema = `
CREATE TABLE IF NOT EXISTS exec_schema(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO exec_schema VALUES(4);
CREATE TABLE IF NOT EXISTS exec_account(account TEXT PRIMARY KEY,key_json TEXT NOT NULL,cash TEXT NOT NULL DEFAULT '0',unassigned TEXT NOT NULL DEFAULT '0',pnl_reclassification TEXT NOT NULL DEFAULT '0',frozen INTEGER NOT NULL DEFAULT 0,checkpoint INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS exec_strategy(account TEXT NOT NULL,strategy TEXT NOT NULL,cash TEXT NOT NULL,fees TEXT NOT NULL DEFAULT '0',funding TEXT NOT NULL DEFAULT '0',PRIMARY KEY(account,strategy));
CREATE TABLE IF NOT EXISTS exec_plan(account TEXT NOT NULL,id TEXT NOT NULL,sequence INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,id),UNIQUE(account,sequence));
CREATE TABLE IF NOT EXISTS exec_virtual_intent(account TEXT NOT NULL,id TEXT NOT NULL,plan_id TEXT NOT NULL,payload TEXT NOT NULL,runtime_payload TEXT,PRIMARY KEY(account,id),FOREIGN KEY(account,plan_id) REFERENCES exec_plan(account,id));
CREATE TABLE IF NOT EXISTS exec_plan_intent(account TEXT NOT NULL,plan_id TEXT NOT NULL,intent_id TEXT NOT NULL,PRIMARY KEY(account,plan_id,intent_id),FOREIGN KEY(account,plan_id) REFERENCES exec_plan(account,id),FOREIGN KEY(account,intent_id) REFERENCES exec_virtual_intent(account,id));
CREATE TABLE IF NOT EXISTS exec_order(account TEXT NOT NULL,id TEXT NOT NULL,plan_id TEXT NOT NULL,payload TEXT NOT NULL,client_id TEXT NOT NULL,exchange_id TEXT NOT NULL DEFAULT '',state TEXT NOT NULL,filled INTEGER NOT NULL DEFAULT 0,fee TEXT NOT NULL DEFAULT '0',cost TEXT NOT NULL DEFAULT '0',report_mode TEXT NOT NULL DEFAULT '',attempt INTEGER NOT NULL DEFAULT 0,generation TEXT NOT NULL DEFAULT '0',PRIMARY KEY(account,id),UNIQUE(account,client_id),FOREIGN KEY(account,plan_id) REFERENCES exec_plan(account,id));
CREATE INDEX IF NOT EXISTS exec_order_state ON exec_order(account,state);
CREATE INDEX IF NOT EXISTS exec_order_plan ON exec_order(account,plan_id,id);
CREATE INDEX IF NOT EXISTS exec_order_active ON exec_order(account,id) WHERE state IN ('Prepared','Sending','Unknown','Acknowledged','Partial','CancelPending');
CREATE TABLE IF NOT EXISTS exec_allocation(account TEXT NOT NULL,order_id TEXT NOT NULL,id TEXT NOT NULL,intent_id TEXT NOT NULL,strategy TEXT NOT NULL,lot TEXT NOT NULL,steps INTEGER NOT NULL CHECK(steps>0),filled INTEGER NOT NULL DEFAULT 0 CHECK(filled>=0 AND filled<=steps),PRIMARY KEY(account,order_id,id),FOREIGN KEY(account,order_id) REFERENCES exec_order(account,id),FOREIGN KEY(account,intent_id) REFERENCES exec_virtual_intent(account,id));
CREATE TABLE IF NOT EXISTS exec_internal_allocation(account TEXT NOT NULL,event_id TEXT NOT NULL,intent_id TEXT NOT NULL,steps INTEGER NOT NULL CHECK(steps>0),PRIMARY KEY(account,event_id,intent_id),FOREIGN KEY(account,event_id) REFERENCES exec_event(account,id),FOREIGN KEY(account,intent_id) REFERENCES exec_virtual_intent(account,id));
CREATE INDEX IF NOT EXISTS exec_allocation_intent ON exec_allocation(account,intent_id,order_id);
CREATE INDEX IF NOT EXISTS exec_internal_allocation_intent ON exec_internal_allocation(account,intent_id,event_id);
CREATE TABLE IF NOT EXISTS exec_lot(account TEXT NOT NULL,strategy TEXT NOT NULL,id TEXT NOT NULL,quantity INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,strategy,id));
CREATE INDEX IF NOT EXISTS exec_lot_active ON exec_lot(account,quantity) WHERE quantity<>0;
CREATE INDEX IF NOT EXISTS exec_lot_active_order ON exec_lot(account,strategy,id) WHERE quantity<>0;
CREATE TABLE IF NOT EXISTS exec_position(account TEXT NOT NULL,instrument TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,instrument));
CREATE TABLE IF NOT EXISTS exec_external_position(account TEXT NOT NULL,instrument TEXT NOT NULL,quantity INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,instrument));
CREATE TABLE IF NOT EXISTS exec_event(account TEXT NOT NULL,id TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,checkpoint INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(account,id));
CREATE UNIQUE INDEX IF NOT EXISTS exec_event_checkpoint ON exec_event(account,checkpoint) WHERE checkpoint>0;
CREATE INDEX IF NOT EXISTS exec_event_tail ON exec_event(account,checkpoint);
` + checkpointSchema + `
CREATE TABLE IF NOT EXISTS exec_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,event_id TEXT NOT NULL,kind TEXT NOT NULL,strategy TEXT NOT NULL,lot TEXT NOT NULL,quantity INTEGER NOT NULL,cash TEXT NOT NULL,fee TEXT NOT NULL,realized TEXT NOT NULL,at_ms INTEGER NOT NULL,FOREIGN KEY(account,event_id) REFERENCES exec_event(account,id));
CREATE INDEX IF NOT EXISTS exec_ledger_account_event ON exec_ledger(account,event_id);
CREATE INDEX IF NOT EXISTS exec_ledger_lot_entry ON exec_ledger(account,strategy,lot,at_ms) WHERE kind IN ('ExchangeFill','InternalFill');

`

func OpenStore(path string, key AccountKey) (*Store, error) {
	// This default only fences processes sharing this host's temp directory.
	// Deployments must configure a stable shared lease directory explicitly;
	// neither variant provides a distributed or cross-host network fence.
	return OpenStoreWithLeaseDir(path, key, filepath.Join(os.TempDir(), "banbot", "execution-owner"))
}

func OpenStoreWithLeaseDir(path string, key AccountKey, leaseDir string) (*Store, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("execution: absolute SQLite path required")
	}
	body, _ := json.Marshal(key)
	hash := sha256.Sum256(body)
	accountID := hex.EncodeToString(hash[:])
	release, leaseErr := acquireSenderLeases(key, []string{key.SettlementDomain}, leaseDir)
	if leaseErr != nil {
		return nil, fmt.Errorf("execution: account sender lease unavailable: %w", leaseErr)
	}
	// Execution owns its pool and durability policy. Legacy trade pools use
	// NORMAL and create unrelated tables; neither belongs in the sender ledger.
	options := url.Values{"mode": {"rwc"}, "_pragma": {"busy_timeout(10000)", "journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)"}}
	dbPath := filepath.ToSlash(filepath.Clean(path))
	if len(dbPath) > 0 && dbPath[0] != '/' {
		dbPath = "/" + dbPath
	}
	dsn := (&url.URL{Scheme: "file", Path: dbPath, RawQuery: options.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		release()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, key: key, accountID: accountID, releaseLease: release}
	if err := s.transaction(context.Background(), func(tx *sql.Tx) error {
		if err := migrateExecutionSchema(tx); err != nil {
			return err
		}
		var version int
		if err := tx.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil {
			return err
		}
		if version != 4 {
			return errors.New("execution: unsupported ledger schema version")
		}
		_, err := tx.Exec("INSERT OR IGNORE INTO exec_account(account,key_json) VALUES(?,?)", s.accountID, string(body))
		return err
	}); err != nil {
		db.Close()
		release()
		return nil, err
	}
	return s, nil
}

func (s *Store) Account() AccountKey { return s.key }

// LatestPlanSequence is account wide; a strategy's own sequence must not be
// reused as the sequence of a combined account plan. Empty stores return -1.
func (s *Store) LatestPlanSequence(ctx context.Context) (int64, error) {
	var sequence int64
	err := s.commit(ctx, func(tx *storeTxn) error {
		return tx.QueryRow(opReadPlanSequence, s.accountID).Scan(&sequence)
	})
	return sequence, err
}
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()
		s.work.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		if s.db != nil {
			s.closeErr = s.db.Close()
		}
		if s.memory != nil && s.memory.history != nil {
			s.closeErr = errors.Join(s.closeErr, s.memory.history.db.Close())
		}
		if s.releaseLease != nil {
			s.closeErr = errors.Join(s.closeErr, s.releaseLease())
		}
	})
	return s.closeErr
}
func (s *Store) beginOwnerOperation() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.closing {
		return errors.New("execution: store closing")
	}
	s.work.Add(1)
	return nil
}

func (s *Store) transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("execution: store closed")
	}
	if s.db == nil {
		return errors.New("execution: SQL maintenance requires a durable store")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// SQLite foreign-key enforcement is per connection, and cannot be changed
	// after BEGIN. Keep explicit checks when acquiring the execution connection.
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return err
	}
	// Every execution commit must sync its WAL before a send can occur. The ORM
	// pool defaults to NORMAL and may open a fresh connection for each call.
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func payload(v any) (string, error) { b, err := json.Marshal(v); return string(b), err }

func (s *Store) SavePlan(ctx context.Context, plan Plan) error {
	if !canonicalID(plan.ID) || plan.Sequence < 0 || plan.DecisionMS < 0 || plan.ExpiresMS <= plan.DecisionMS {
		return errors.New("execution: invalid plan identity/validity")
	}
	seen := make(map[VirtualIntentID]bool)
	for _, intent := range plan.Intents {
		if err := intent.Validate(); err != nil {
			return err
		}
		if intent.Account != s.key || seen[intent.ID] {
			return errors.New("execution: duplicate or foreign plan intent")
		}
		seen[intent.ID] = true
	}
	body, err := payload(plan)
	if err != nil {
		return err
	}
	return s.commit(ctx, func(tx *storeTxn) error {
		var previous string
		err := tx.QueryRow(opReadPlan, s.accountID, plan.ID).Scan(&previous)
		if err == nil {
			if previous != body {
				return errors.New("execution: immutable plan identity reused")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var latest int64
		if err := tx.QueryRow(opReadPlanSequence, s.accountID).Scan(&latest); err != nil {
			return err
		}
		if plan.Sequence <= latest {
			return errors.New("execution: new account plan sequence must advance")
		}
		rows, err := tx.Query(opListOrdersByState, s.accountID, string(OrderPrepared))
		if err != nil {
			return err
		}
		var retired []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			retired = append(retired, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range retired {
			if err := s.setOrderState(tx, id, OrderCanceled); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(opInsertPlan, s.accountID, plan.ID, plan.Sequence, body); err != nil {
			return err
		}
		for _, intent := range plan.Intents {
			definition := intent
			definition.State = ""
			definition.FilledSteps = 0
			definition.ReservedSteps = 0
			definition.Triggered = false
			definition.TrailingActive = false
			definition.TrailingAnchor = decimal.Zero
			b, err := payload(definition)
			if err != nil {
				return err
			}
			var previousIntent string
			err = tx.QueryRow(opReadIntentDefinition, s.accountID, string(intent.ID)).Scan(&previousIntent)
			if err == nil && previousIntent != b {
				return errors.New("execution: stable virtual intent definition changed across plans")
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			runtime := intent
			runtime.FilledSteps = 0
			runtime.ReservedSteps = 0
			runtimeBody, err := payload(runtime)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(opEnsureIntent, s.accountID, string(intent.ID), plan.ID, b, runtimeBody); err != nil {
				return err
			}
			if _, err := tx.Exec(opInsertPlanMembership, s.accountID, plan.ID, string(intent.ID)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) PrepareOrder(ctx context.Context, order OrderIntent, nowMS int64) error {
	if err := order.Instrument.Validate(); err != nil {
		return err
	}
	if !canonicalID(order.ID) || !canonicalID(order.PlanID) || order.Steps <= 0 || (order.Side != Buy && order.Side != Sell) || order.Limit.IsNegative() || order.PostOnly && !order.Limit.IsPositive() || nowMS < 0 {
		return errors.New("execution: invalid real order")
	}
	if order.Steps < order.Instrument.MinSteps || order.Instrument.Notional(order.Steps, order.Observation.Price).LessThan(order.Instrument.MinNotional) {
		return errors.New("execution: order below instrument minimum units/notional")
	}
	var sum int64
	seen := make(map[string]bool)
	for _, a := range order.Allocations {
		if !canonicalID(a.ID) || a.Steps <= 0 || a.Steps > order.Steps-sum || a.Side != order.Side || seen[a.ID] {
			return errors.New("execution: invalid frozen allocation")
		}
		sum += a.Steps
		seen[a.ID] = true
	}
	if sum != order.Steps {
		return errors.New("execution: allocation quantity must equal real order")
	}
	body, err := payload(order)
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(s.accountID + "/" + order.ID))
	clientID := "exec-" + hex.EncodeToString(hash[:12])
	return s.commit(ctx, func(tx *storeTxn) error {
		var existing string
		err := tx.QueryRow(opReadOrderDefinition, s.accountID, order.ID).Scan(&existing)
		if err == nil {
			if existing != body {
				return errors.New("execution: frozen order identity reused")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var planBody string
		if err := tx.QueryRow(opReadPlan, s.accountID, order.PlanID).Scan(&planBody); err != nil {
			return err
		}
		var plan Plan
		if err := json.Unmarshal([]byte(planBody), &plan); err != nil {
			return err
		}
		if nowMS < plan.DecisionMS || nowMS >= plan.ExpiresMS {
			return errors.New("execution: plan outside validity")
		}
		var latest int64
		if err := tx.QueryRow(opReadLatestSequence, s.accountID).Scan(&latest); err != nil {
			return err
		}
		if plan.Sequence != latest {
			return errors.New("execution: superseded plan cannot prepare new orders")
		}
		observation := order.Observation
		if !observation.Price.IsPositive() || observation.AtMS < 0 || observation.AtMS > nowMS || observation.ValidUntilMS <= nowMS || observation.Bar < 0 {
			return errors.New("execution: order requires visible unexpired execution observation")
		}
		var frozen bool
		if err := tx.QueryRow(opReadAccountFrozen, s.accountID).Scan(&frozen); err != nil {
			return err
		}
		if frozen {
			return errors.New("execution: external/reconciliation risk freeze")
		}
		newAllocation := make(map[VirtualIntentID]int64)
		for _, a := range order.Allocations {
			var intentBody string
			if err := tx.QueryRow(opReadPlanIntentRuntime, s.accountID, string(a.IntentID), s.accountID, order.PlanID, string(a.IntentID)).Scan(&intentBody); err != nil {
				return err
			}
			var intent EligibleIntent
			if err := json.Unmarshal([]byte(intentBody), &intent); err != nil {
				return err
			}
			if intent.Strategy != a.Strategy || intent.Lot != a.Lot || intent.Instrument != order.Instrument.ID || intent.Side != a.Side || intent.Kind != a.Kind {
				return errors.New("execution: allocation does not match eligible virtual intent")
			}
			if intent.Conditions.PostOnly && !order.PostOnly {
				return errors.New("execution: frozen order drops contributor post-only condition")
			}
			available, err := intent.Evaluate(observation.Price, nowMS, observation.Bar)
			if err != nil {
				return err
			}
			var allocated int64
			if err := tx.QueryRow(opReadIntentConsumed, s.accountID, string(a.IntentID), s.accountID, string(a.IntentID)).Scan(&allocated); err != nil {
				return err
			}
			newAllocation[a.IntentID] += a.Steps
			if newAllocation[a.IntentID] > available-allocated {
				return errors.New("execution: virtual intent already allocated")
			}
		}
		if _, err := tx.Exec(opInsertOrder, s.accountID, order.ID, order.PlanID, body, clientID, string(OrderPrepared)); err != nil {
			return err
		}
		for _, a := range order.Allocations {
			if _, err := tx.Exec(opInsertAllocation, s.accountID, order.ID, a.ID, string(a.IntentID), string(a.Strategy), string(a.Lot), a.Steps); err != nil {
				return err
			}
		}
		return s.recordOrderState(tx, order.ID)
	})
}

func (s *Store) Plan(ctx context.Context, id string) (Plan, error) {
	var plan Plan
	err := s.commit(ctx, func(tx *storeTxn) error {
		var body string
		if err := tx.QueryRow(opReadPlan, s.accountID, id).Scan(&body); err != nil {
			return err
		}
		return json.Unmarshal([]byte(body), &plan)
	})
	return plan, err
}

func (s *Store) readOrder(ctx context.Context, db *storeTxn, id string) (StoredOrder, error) {
	var o StoredOrder
	var body, fee, cost, generation string
	err := db.QueryRowContext(ctx, opReadOrder, s.accountID, id).Scan(&body, &o.ClientID, &o.ExchangeID, &o.State, &o.FilledSteps, &fee, &cost, &o.Attempt, &generation)
	if err != nil {
		return o, err
	}
	o.ReportedCost, err = decimal.NewFromString(cost)
	if err != nil {
		return o, err
	}
	if err := json.Unmarshal([]byte(body), &o.Intent); err != nil {
		return o, err
	}
	o.ReportedFee, err = decimal.NewFromString(fee)
	if err != nil {
		return o, err
	}
	_, err = fmt.Sscan(generation, &o.Generation)
	if err != nil {
		return o, err
	}
	o.AllocationFilled = make(map[string]int64)
	for _, a := range o.Intent.Allocations {
		var filled int64
		if err := db.QueryRowContext(ctx, opReadAllocationFilled, s.accountID, id, a.ID).Scan(&filled); err != nil {
			return o, err
		}
		o.AllocationFilled[a.ID] = filled
	}
	return o, err
}
func (s *Store) Order(ctx context.Context, id string) (StoredOrder, error) {
	var order StoredOrder
	err := s.readRecord(ctx, func(tx *storeTxn) error { var err error; order, err = s.readOrder(ctx, tx, id); return err })
	return order, err
}

func (s *Store) setOrderState(tx *storeTxn, id string, state RealOrderState) error {
	if _, err := tx.Exec(opUpdateOrderState, string(state), s.accountID, id); err != nil {
		return err
	}
	return s.recordOrderState(tx, id)
}

func (s *Store) recordOrderState(tx *storeTxn, id string) error {
	order, err := s.readOrder(context.Background(), tx, id)
	if err != nil {
		return err
	}
	event := OrderStateEvent{OrderID: id, ExchangeID: order.ExchangeID, ClientID: order.ClientID, State: order.State, Allocations: order.Intent.Allocations, AllocationFilled: order.AllocationFilled, FilledSteps: order.FilledSteps, Attempt: order.Attempt, Generation: order.Generation}
	body, err := payload(event)
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(body))
	eventID := "state-" + hex.EncodeToString(hash[:])
	fresh, err := s.recordEvent(tx, eventID, "OrderState", body)
	if err != nil || !fresh {
		return err
	}
	return s.commitAccountEvent(tx, eventID, decimal.Zero, nil)
}
