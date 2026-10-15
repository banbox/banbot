package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type Durability string

const (
	MemoryOnly Durability = "memory"
	Durable    Durability = "durable"
)

// NewMemoryStore creates process-local execution state, with no SQL driver,
// files, or sender lease. The account owner still serializes commands. Its
// journal stages all changed records under that lock and undoes them on error,
// panic, or canceled commit; existing domain validation and attribution run
// unchanged. MemoryOnly must never be selected for a real trading session.
func NewMemoryStore(key AccountKey) (*Store, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(body)
	s := &Store{key: key, accountID: hex.EncodeToString(hash[:]), memory: &memoryState{
		records: make(map[recordKind]map[memoryKey]memoryRecord), latestSequence: -1,
		planSequences: make(map[int64]string), clientOrders: make(map[string]string), eventCheckpoints: make(map[int64]string),
		activeLots: make(map[memoryKey]bool), activeOrders: make(map[memoryKey]bool),
		postingsByEvent: make(map[memoryKey][]memoryKey), allocationsByIntent: make(map[memoryKey][]memoryKey), internalByIntent: make(map[memoryKey][]memoryKey),
		postingsByLot: make(map[memoryKey][]memoryKey),
	}}
	s.memory.records[recordsAccount] = map[memoryKey]memoryRecord{{s.accountID, "", ""}: {Account: s.accountID, KeyJson: string(body), Cash: "0", Unassigned: "0", PnlReclassification: "0"}}
	return s, nil
}
func (s *Store) Durability() Durability {
	if s.memory != nil {
		return MemoryOnly
	}
	return Durable
}

type memoryKey struct{ account, first, second string }
type memoryState struct {
	history                                                *memoryHistory
	records                                                map[recordKind]map[memoryKey]memoryRecord
	version                                                uint64
	nextPosting                                            int64
	migrationEnabled                                       bool
	latestSequence                                         int64
	latestPlanID                                           string
	planSequences                                          map[int64]string
	clientOrders                                           map[string]string
	eventCheckpoints                                       map[int64]string
	activeLots, activeOrders                               map[memoryKey]bool
	postingsByEvent, allocationsByIntent, internalByIntent map[memoryKey][]memoryKey
	postingsByLot                                          map[memoryKey][]memoryKey
}
type recordConflict uint8

const (
	insertRecord recordConflict = iota
	ignoreRecord
	replaceRecord
)

func argString(value any) string { return fmt.Sprint(value) }
func argInt(value any) int64     { v := reflectInteger(value); return v }
func reflectInteger(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case uint64:
		return int64(v)
	case int32:
		return int64(v)
	}
	panic(fmt.Sprintf("execution: invalid integer argument %T", value))
}
func argBool(value any) bool {
	if v, ok := value.(bool); ok {
		return v
	}
	return argInt(value) != 0
}
func terminalOrder(state string) bool {
	return state == "Canceled" || state == "Rejected" || state == "Filled"
}

func recordKey(kind recordKind, r memoryRecord) memoryKey {
	key := memoryKey{account: r.Account}
	switch kind {
	case recordsAccount:
	case recordsStrategy:
		key.first = r.Strategy
	case recordsPlan, recordsVirtualIntent, recordsOrder, recordsEvent, recordsMigration, recordsPaperOrder:
		key.first = r.ID
	case recordsPlanIntent:
		key.first, key.second = r.PlanId, r.IntentId
	case recordsAllocation:
		key.first, key.second = r.OrderId, r.ID
	case recordsInternalAllocation:
		key.first, key.second = r.EventId, r.IntentId
	case recordsLot:
		key.first, key.second = r.Strategy, r.ID
	case recordsPosition, recordsExternalPosition:
		key.first = r.Instrument
	case recordsCheckpoint:
		key.first, key.second = r.Kind+":"+r.Strategy, r.Name
	case recordsLedger:
		key.first = strconv.FormatInt(r.PostingID, 10)
	case recordsLegacySource:
		key.first, key.second = r.Strategy, r.Lot
	default:
		panic("execution: invalid record kind")
	}
	return key
}

func (tx *storeTxn) selectRecords(kind recordKind, predicate func(memoryRecord) bool) []memoryRecord {
	return tx.selectRecordsWhere(kind, "", nil, predicate)
}

func (tx *storeTxn) selectRecordsWhere(kind recordKind, where string, args []any, predicate func(memoryRecord) bool) []memoryRecord {
	var records []memoryRecord
	for _, r := range tx.memory.records[kind] {
		if predicate(r) {
			records = append(records, r)
		}
	}
	tx.visitHistory(kind, where, args, func(r memoryRecord) {
		if predicate(r) {
			records = append(records, r)
		}
	})
	return records
}

func (tx *storeTxn) validateRecord(kind recordKind, r memoryRecord, key memoryKey) error {
	has := func(kind recordKind, a, b string) bool {
		_, ok := tx.memoryRecord(kind, memoryKey{r.Account, a, b})
		return ok
	}
	valid := true
	switch kind {
	case recordsVirtualIntent, recordsOrder:
		valid = has(recordsPlan, r.PlanId, "")
	case recordsPlanIntent:
		valid = has(recordsPlan, r.PlanId, "") && has(recordsVirtualIntent, r.IntentId, "")
	case recordsAllocation:
		valid = r.Steps > 0 && r.Filled >= 0 && r.Filled <= r.Steps && has(recordsOrder, r.OrderId, "") && has(recordsVirtualIntent, r.IntentId, "")
	case recordsInternalAllocation:
		valid = r.Steps > 0 && has(recordsEvent, r.EventId, "") && has(recordsVirtualIntent, r.IntentId, "")
	case recordsLedger:
		valid = has(recordsEvent, r.EventId, "")
	case recordsLegacySource:
		valid = has(recordsMigration, r.MigrationId, "")
	}
	if !valid {
		return errors.New("execution: invalid execution record reference/quantity")
	}
	var existing string
	switch kind {
	case recordsPlan:
		existing = tx.memory.planSequences[r.Sequence]
		if existing == "" {
			existing = tx.historyIndex(kind, "sequence", r.Sequence)
		}
	case recordsOrder:
		existing = tx.memory.clientOrders[r.ClientId]
		if existing == "" {
			existing = tx.historyIndex(kind, "client", r.ClientId)
		}
	case recordsEvent:
		if r.Checkpoint > 0 {
			existing = tx.memory.eventCheckpoints[r.Checkpoint]
			if existing == "" {
				existing = tx.historyIndex(kind, "checkpoint", r.Checkpoint)
			}
		}
	}
	if existing != "" && existing != r.ID {
		return errors.New("execution: duplicate execution record sequence/identity")
	}
	return nil
}

func (tx *storeTxn) stageRecord(kind recordKind, key memoryKey, r memoryRecord) {
	rows := tx.memory.records[kind]
	if rows == nil {
		rows = make(map[memoryKey]memoryRecord)
		tx.memory.records[kind] = rows
	}
	old, present := rows[key]
	tx.undo = append(tx.undo, func() {
		if present {
			rows[key] = old
		} else {
			delete(rows, key)
		}
	})
	rows[key] = r
	if tx.memory.history != nil {
		if tx.changed == nil {
			tx.changed = make(map[historyRecordKey]bool)
		}
		tx.changed[historyRecordKey{kind, key}] = true
	}
	switch kind {
	case recordsPlan:
		stageMemoryIndex(tx, tx.memory.planSequences, r.Sequence, r.ID)
		if r.Sequence > tx.memory.latestSequence {
			previousSequence, previousID := tx.memory.latestSequence, tx.memory.latestPlanID
			tx.undo = append(tx.undo, func() { tx.memory.latestSequence, tx.memory.latestPlanID = previousSequence, previousID })
			tx.memory.latestSequence, tx.memory.latestPlanID = r.Sequence, r.ID
		}
	case recordsOrder:
		stageMemoryIndex(tx, tx.memory.clientOrders, r.ClientId, r.ID)
		stageActiveRecord(tx, tx.memory.activeOrders, key, !terminalOrder(r.State))
	case recordsEvent:
		if r.Checkpoint > 0 {
			stageMemoryIndex(tx, tx.memory.eventCheckpoints, r.Checkpoint, r.ID)
		}
	case recordsLot:
		stageActiveRecord(tx, tx.memory.activeLots, key, r.Quantity != 0)
	case recordsLedger:
		if !present {
			tx.indexRecord(tx.memory.postingsByEvent, memoryKey{r.Account, r.EventId, ""}, key)
			if r.Kind == "ExchangeFill" || r.Kind == "InternalFill" {
				tx.indexRecord(tx.memory.postingsByLot, memoryKey{r.Account, r.Strategy, r.Lot}, key)
			}
		}
	case recordsAllocation:
		if !present {
			tx.indexRecord(tx.memory.allocationsByIntent, memoryKey{r.Account, r.IntentId, ""}, key)
		}
	case recordsInternalAllocation:
		if !present {
			tx.indexRecord(tx.memory.internalByIntent, memoryKey{r.Account, r.IntentId, ""}, key)
		}
	}
}

func stageMemoryIndex[K comparable, V comparable](tx *storeTxn, index map[K]V, key K, value V) {
	previous, present := index[key]
	if present && previous == value {
		return
	}
	tx.undo = append(tx.undo, func() {
		if present {
			index[key] = previous
		} else {
			delete(index, key)
		}
	})
	index[key] = value
}
func stageActiveRecord(tx *storeTxn, index map[memoryKey]bool, key memoryKey, active bool) {
	if active {
		stageMemoryIndex(tx, index, key, true)
		return
	}
	if previous, present := index[key]; present {
		tx.undo = append(tx.undo, func() { index[key] = previous })
		delete(index, key)
	}
}
func (tx *storeTxn) indexRecord(index map[memoryKey][]memoryKey, owner, key memoryKey) {
	previous, present := index[owner]
	tx.undo = append(tx.undo, func() {
		if present {
			index[owner] = previous
		} else {
			delete(index, owner)
		}
	})
	index[owner] = append(previous, key)
}

func (tx *storeTxn) lookupRecords(kind recordKind, key memoryKey) []memoryRecord {
	r, present := tx.memoryRecord(kind, key)
	if !present {
		return nil
	}
	return []memoryRecord{r}
}
func (tx *storeTxn) activeRecords(kind recordKind, index map[memoryKey]bool, predicate func(memoryRecord) bool) []memoryRecord {
	var records []memoryRecord
	for key, active := range index {
		if active {
			r := tx.memory.records[kind][key]
			if predicate(r) {
				records = append(records, r)
			}
		}
	}
	return records
}
func (tx *storeTxn) indexedRecords(kind recordKind, index map[memoryKey][]memoryKey, owner memoryKey) []memoryRecord {
	var records []memoryRecord
	for _, key := range index[owner] {
		records = append(records, tx.memory.records[kind][key])
	}
	where := " AND intent=?"
	args := []any{owner.first}
	if kind == recordsLedger {
		where = " AND event=?"
		if owner.second != "" {
			where = " AND strategy=? AND lot=?"
			args = []any{owner.first, owner.second}
		}
	}
	tx.visitHistory(kind, where, args, func(r memoryRecord) {
		if kind != recordsLedger || owner.second == "" || r.Kind == "ExchangeFill" || r.Kind == "InternalFill" {
			records = append(records, r)
		}
	})
	return records
}

func (tx *storeTxn) putRecord(kind recordKind, r memoryRecord, conflict recordConflict, update func(*memoryRecord)) error {
	if r.Cash == "" {
		r.Cash = "0"
	}
	if r.Unassigned == "" {
		r.Unassigned = "0"
	}
	if r.PnlReclassification == "" {
		r.PnlReclassification = "0"
	}
	if r.Fee == "" {
		r.Fee = "0"
	}
	if r.Fees == "" {
		r.Fees = "0"
	}
	if r.Funding == "" {
		r.Funding = "0"
	}
	if r.Cost == "" {
		r.Cost = "0"
	}
	if r.Generation == "" {
		r.Generation = "0"
	}
	if kind == recordsLedger {
		previous := tx.memory.nextPosting
		tx.undo = append(tx.undo, func() { tx.memory.nextPosting = previous })
		tx.memory.nextPosting++
		r.PostingID = tx.memory.nextPosting
	}
	key := recordKey(kind, r)
	if old, present := tx.memoryRecord(kind, key); present {
		if conflict == ignoreRecord {
			return nil
		}
		if conflict == insertRecord {
			return errors.New("execution: duplicate execution record")
		}
		update(&old)
		r = old
	}
	if err := tx.validateRecord(kind, r, key); err != nil {
		return err
	}
	tx.stageRecord(kind, key, r)
	return nil
}

func (tx *storeTxn) updateRecord(kind recordKind, key memoryKey, update func(*memoryRecord)) error {
	r, present := tx.memoryRecord(kind, key)
	if !present {
		return nil
	}
	update(&r)
	if err := tx.validateRecord(kind, r, key); err != nil {
		return err
	}
	tx.stageRecord(kind, key, r)
	return nil
}
