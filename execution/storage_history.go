package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// memoryHistory is optional cold storage for a simulated account. It stores
// domain records, not SQL operations, and never claims a sender lease or real
// trading durability. Domain validation and undo still run in MemoryStore;
// changed records reach this archive in one transaction before hot eviction.
// Keeping the full indexed history makes delayed funding, repeated reports and
// lagging consumers safe without inventing producer finality or pruning facts.
type memoryHistory struct{ db *sql.DB }

// HasMemoryHistory reports this store's immutable simulated output choice.
func (s *Store) HasMemoryHistory() bool {
	return s != nil && s.memory != nil && s.memory.history != nil
}

const memoryHistorySchema = `
CREATE TABLE history_schema(version INTEGER NOT NULL,account TEXT NOT NULL);
CREATE TABLE history_record(kind INTEGER NOT NULL,k1 TEXT NOT NULL,k2 TEXT NOT NULL,
 body TEXT NOT NULL,id TEXT NOT NULL,sequence INTEGER NOT NULL,client TEXT NOT NULL,
 checkpoint INTEGER NOT NULL,event TEXT NOT NULL,strategy TEXT NOT NULL,lot TEXT NOT NULL,
 intent TEXT NOT NULL,plan TEXT NOT NULL,posting INTEGER NOT NULL,
 event_type TEXT NOT NULL,exchange TEXT NOT NULL,order_id TEXT NOT NULL,
 PRIMARY KEY(kind,k1,k2));
CREATE INDEX history_sequence ON history_record(kind,sequence);
CREATE INDEX history_client ON history_record(kind,client);
CREATE INDEX history_checkpoint ON history_record(kind,checkpoint);
CREATE INDEX history_event ON history_record(kind,event,posting);
CREATE INDEX history_lot ON history_record(kind,strategy,lot,posting);
CREATE INDEX history_intent ON history_record(kind,intent);
CREATE INDEX history_plan ON history_record(kind,plan);
CREATE INDEX history_type ON history_record(kind,event_type,order_id,checkpoint);
CREATE INDEX history_exchange ON history_record(kind,exchange);
`

// NewMemoryStoreWithHistory keeps execution's domain memory commit path and
// spills committed history into a fresh indexed file. Empty paths select the
// ordinary file-free MemoryStore. A history file is optional backtest output,
// not a restart snapshot; existing files are never overwritten or reopened.
func NewMemoryStoreWithHistory(key AccountKey, path string) (*Store, error) {
	s, err := NewMemoryStore(key)
	if err != nil || path == "" {
		return s, err
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("execution: memory history needs an absolute new file path")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	options := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(10000)", "journal_mode(WAL)", "synchronous(NORMAL)"}}
	dbPath := filepath.ToSlash(filepath.Clean(path))
	if dbPath[0] != '/' {
		dbPath = "/" + dbPath
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: dbPath, RawQuery: options.Encode()}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err = db.Exec(memoryHistorySchema); err == nil {
		_, err = db.Exec("INSERT INTO history_schema VALUES(1,?)", s.accountID)
	}
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	s.memory.history = &memoryHistory{db: db}
	return s, nil
}

type historyRecordKey struct {
	kind recordKind
	key  memoryKey
}

func (tx *storeTxn) memoryRecord(kind recordKind, key memoryKey) (memoryRecord, bool) {
	if r, ok := tx.memory.records[kind][key]; ok {
		return r, true
	}
	if tx.memory.history == nil || tx.recordErr != nil {
		return memoryRecord{}, false
	}
	var body string
	err := tx.memory.history.db.QueryRowContext(context.WithoutCancel(tx.ctx), "SELECT body FROM history_record WHERE kind=? AND k1=? AND k2=?", kind, key.first, key.second).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return memoryRecord{}, false
	}
	var r memoryRecord
	if err == nil {
		err = json.Unmarshal([]byte(body), &r)
	}
	if err == nil && recordKey(kind, r) != key {
		err = errors.New("execution: cold history record identity mismatch")
	}
	if err != nil {
		tx.recordErr = err
		return memoryRecord{}, false
	}
	return r, true
}

// visitHistory streams one decoded record at a time. Hot staged changes shadow
// archived values, so every domain read sees the current account transaction.
func (tx *storeTxn) visitHistory(kind recordKind, where string, args []any, visit func(memoryRecord)) {
	if tx.memory.history == nil || tx.recordErr != nil {
		return
	}
	args = append([]any{kind}, args...)
	rows, err := tx.memory.history.db.QueryContext(context.WithoutCancel(tx.ctx), "SELECT body FROM history_record WHERE kind=?"+where, args...)
	if err != nil {
		tx.recordErr = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		if err := tx.ctx.Err(); err != nil {
			tx.recordErr = err
			return
		}
		var body string
		var r memoryRecord
		if err := rows.Scan(&body); err != nil {
			tx.recordErr = err
			return
		}
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			tx.recordErr = err
			return
		}
		if _, hot := tx.memory.records[kind][recordKey(kind, r)]; !hot {
			visit(r)
			if tx.recordErr != nil {
				return
			}
		}
	}
	tx.recordErr = rows.Err()
}

func (tx *storeTxn) historyIndex(kind recordKind, column string, value any) string {
	if tx.memory.history == nil || tx.recordErr != nil {
		return ""
	}
	// column is a private, fixed implementation choice, never user input.
	var id string
	err := tx.memory.history.db.QueryRowContext(context.WithoutCancel(tx.ctx), "SELECT id FROM history_record WHERE kind=? AND "+column+"=? LIMIT 1", kind, value).Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		tx.recordErr = err
	}
	return id
}

func (tx *storeTxn) saveHistory() error {
	if tx.recordErr != nil {
		return tx.recordErr
	}
	if tx.memory.history == nil || len(tx.changed) == 0 {
		return nil
	}
	cold, err := tx.memory.history.db.BeginTx(tx.ctx, nil)
	if err != nil {
		return err
	}
	defer cold.Rollback()
	statement, err := cold.PrepareContext(tx.ctx, `INSERT INTO history_record VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(kind,k1,k2) DO UPDATE SET body=excluded.body,id=excluded.id,
sequence=excluded.sequence,client=excluded.client,checkpoint=excluded.checkpoint,
event=excluded.event,strategy=excluded.strategy,lot=excluded.lot,intent=excluded.intent,
plan=excluded.plan,posting=excluded.posting,event_type=excluded.event_type,exchange=excluded.exchange,order_id=excluded.order_id`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for changed := range tx.changed {
		r := tx.memory.records[changed.kind][changed.key]
		body, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := statement.ExecContext(tx.ctx, changed.kind, changed.key.first, changed.key.second, string(body), r.ID, r.Sequence, r.ClientId, r.Checkpoint, r.EventId, r.Strategy, r.Lot, r.IntentId, r.PlanId, r.PostingID, r.Kind, r.ExchangeId, r.OrderId); err != nil {
			return err
		}
	}
	return cold.Commit()
}

func (m *memoryState) evictHistory() {
	if m.history == nil {
		return
	}
	for kind, rows := range m.records {
		for key, r := range rows {
			keep := false
			switch kind {
			case recordsAccount, recordsStrategy, recordsPosition, recordsExternalPosition:
				keep = true
			case recordsCheckpoint:
				// Per-plan identities are immutable history. Fixed-name policy and
				// consumer cursors remain hot; old revisions use indexed point reads.
				keep = !strings.HasPrefix(r.Name, "target-revision:") && !strings.HasPrefix(r.Name, "legacy-ts/order:") && !strings.HasPrefix(r.Name, "legacy-ts/command:")
			case recordsOrder:
				keep = !terminalOrder(r.State)
			case recordsLot:
				keep = r.Quantity != 0
			case recordsPlan:
				keep = r.ID == m.latestPlanID
			}
			if !keep {
				delete(rows, key)
			}
		}
	}
	// Historical secondary indexes live in the cold file as well. Active indexes
	// remain in memory and contain only nonterminal orders and nonzero lots.
	clear(m.planSequences)
	clear(m.clientOrders)
	clear(m.eventCheckpoints)
	clear(m.postingsByEvent)
	clear(m.postingsByLot)
	clear(m.allocationsByIntent)
	clear(m.internalByIntent)
}

type MemoryHistoryStats struct {
	HotRecords  int
	ColdRecords int64
}

// MemoryHistoryStats reports retained domain records, not total process RSS or
// a bound on strategy-owned state, active requests, or one unusually wide row.
func (s *Store) MemoryHistoryStats(ctx context.Context) (MemoryHistoryStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stats MemoryHistoryStats
	if s.closed || s.memory == nil {
		return stats, errors.New("execution: history statistics need an open memory store")
	}
	for _, rows := range s.memory.records {
		stats.HotRecords += len(rows)
	}
	if s.memory.history != nil {
		err := s.memory.history.db.QueryRowContext(context.WithoutCancel(ctx), "SELECT COUNT(*) FROM history_record").Scan(&stats.ColdRecords)
		if err != nil {
			return stats, fmt.Errorf("execution: cold history statistics: %w", err)
		}
	}
	return stats, ctx.Err()
}
