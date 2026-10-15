package orm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type sidRegistryFakeDB struct {
	mu    sync.Mutex
	next  int32
	byKey map[string]SIDRegistryReservation
	bySID map[int32]string
}

func newSIDRegistryFakeDB() *sidRegistryFakeDB {
	return &sidRegistryFakeDB{next: 1, byKey: make(map[string]SIDRegistryReservation), bySID: make(map[int32]string)}
}

func (db *sidRegistryFakeDB) Exec(_ context.Context, _ string, args ...interface{}) (pgconn.CommandTag, error) {
	if len(args) == 1 {
		if floor, ok := args[0].(int32); ok {
			db.mu.Lock()
			if floor >= db.next {
				db.next = floor + 1
			}
			db.mu.Unlock()
		}
	}
	return pgconn.NewCommandTag("CREATE 0"), nil
}

func (db *sidRegistryFakeDB) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !strings.Contains(sql, "exsymbol_sid_registry") {
		return sidRegistryFakeRow{err: fmt.Errorf("unexpected SQL: %s", sql)}
	}
	exchange, _ := args[0].(string)
	market, _ := args[1].(string)
	symbol, _ := args[2].(string)
	key := exSymbolKey(exchange, market, symbol)
	if existing, ok := db.byKey[key]; ok {
		if len(args) == 9 {
			requestedSID, _ := args[3].(int32)
			if requestedSID != existing.ID {
				return sidRegistryFakeRow{err: fmt.Errorf("SID %d already belongs to %s", requestedSID, db.bySID[requestedSID])}
			}
		}
		return sidRegistryFakeRow{reservation: existing}
	}
	reservation := SIDRegistryReservation{
		ExSymbol: ExSymbol{
			Exchange: exchange,
			Market:   market,
			Symbol:   symbol,
		},
		WriteTS: time.Unix(1_700_000_000+int64(db.next), 0).UTC(),
	}
	if len(args) == 9 {
		reservation.ID, _ = args[3].(int32)
		reservation.ExgReal, _ = args[4].(string)
		reservation.Combined, _ = args[5].(bool)
		reservation.ListMs, _ = args[6].(int64)
		reservation.DelistMs, _ = args[7].(int64)
		reservation.AggRules, _ = args[8].(string)
	} else {
		reservation.ExgReal, _ = args[3].(string)
		reservation.Combined, _ = args[4].(bool)
		reservation.ListMs, _ = args[5].(int64)
		reservation.DelistMs, _ = args[6].(int64)
		reservation.AggRules, _ = args[7].(string)
		reservation.ID = db.next
		db.next++
	}
	if reservation.ID <= 0 {
		return sidRegistryFakeRow{err: fmt.Errorf("invalid SID %d", reservation.ID)}
	}
	if owner := db.bySID[reservation.ID]; owner != "" && owner != key {
		return sidRegistryFakeRow{err: fmt.Errorf("SID %d already belongs to %s", reservation.ID, owner)}
	}
	db.byKey[key] = reservation
	db.bySID[reservation.ID] = key
	return sidRegistryFakeRow{reservation: reservation}
}

type sidRegistryFakeRow struct {
	reservation SIDRegistryReservation
	err         error
}

func (r sidRegistryFakeRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	values := []interface{}{
		r.reservation.Exchange, r.reservation.Market, r.reservation.Symbol, r.reservation.ID,
		r.reservation.WriteTS, r.reservation.ExgReal, r.reservation.Combined,
		r.reservation.ListMs, r.reservation.DelistMs, r.reservation.AggRules,
	}
	if len(dest) != len(values) {
		return fmt.Errorf("scan destinations = %d, want %d", len(dest), len(values))
	}
	for i, value := range values {
		switch target := dest[i].(type) {
		case *string:
			*target = value.(string)
		case *int32:
			*target = value.(int32)
		case *time.Time:
			*target = value.(time.Time)
		case *bool:
			*target = value.(bool)
		case *int64:
			*target = value.(int64)
		default:
			return fmt.Errorf("unsupported scan destination %T", dest[i])
		}
	}
	return nil
}

func TestSIDRegistryConcurrentLogicalReservationIsStable(t *testing.T) {
	db := newSIDRegistryFakeDB()
	first := newSymbolSIDRegistryForDB(db)
	second := newSymbolSIDRegistryForDB(db)
	arg := AddSymbolsParams{Exchange: "test", Market: "spot", Symbol: "BTC/USDT", ExgReal: "test"}

	results := make(chan SIDRegistryReservation, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, registry := range []*SymbolSIDRegistry{first, second} {
		registry := registry
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := registry.Reserve(context.Background(), []AddSymbolsParams{arg})
			if err != nil {
				errs <- err
				return
			}
			results <- rows[0]
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var reservations []SIDRegistryReservation
	for reservation := range results {
		reservations = append(reservations, reservation)
	}
	if len(reservations) != 2 || reservations[0].ID != reservations[1].ID ||
		!reservations[0].WriteTS.Equal(reservations[1].WriteTS) {
		t.Fatalf("concurrent reservations = %+v, want one stable logical identity", reservations)
	}
	if got := len(db.byKey); got != 1 {
		t.Fatalf("registry logical rows = %d, want 1", got)
	}

	other, err := first.Reserve(context.Background(), []AddSymbolsParams{{
		Exchange: "test", Market: "spot", Symbol: "ETH/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 1 || other[0].ID == reservations[0].ID {
		t.Fatalf("different logical symbol reservation = %+v, want a distinct SID", other)
	}
}

func TestSIDRegistryAdoptPreservesPhysicalSID(t *testing.T) {
	db := newSIDRegistryFakeDB()
	registry := newSymbolSIDRegistryForDB(db)
	item := &ExSymbol{ID: 41, Exchange: "test", Market: "spot", Symbol: "BTC/USDT", ExgReal: "test"}
	reservation, err := registry.Adopt(context.Background(), item)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ID != item.ID {
		t.Fatalf("adopted SID = %d, want %d", reservation.ID, item.ID)
	}
	rows, err := registry.Reserve(context.Background(), []AddSymbolsParams{{
		Exchange: item.Exchange, Market: item.Market, Symbol: item.Symbol,
	}})
	if err != nil || len(rows) != 1 || rows[0].ID != item.ID {
		t.Fatalf("reserved adopted symbol = (%+v, %v), want SID %d", rows, err, item.ID)
	}
}

func TestSIDRegistryRetriesAfterInitializationFailure(t *testing.T) {
	db := newSIDRegistryFakeDB()
	attempts := 0
	registry := &SymbolSIDRegistry{openFn: func(context.Context) (sidRegistryDB, func(), error) {
		attempts++
		if attempts == 1 {
			return nil, nil, fmt.Errorf("temporary registry outage")
		}
		return db, func() {}, nil
	}}

	if _, err := registry.database(context.Background()); err == nil {
		t.Fatal("first registry initialization unexpectedly succeeded")
	}
	got, err := registry.database(context.Background())
	if err != nil {
		t.Fatalf("second registry initialization failed: %v", err)
	}
	if got != db || attempts != 2 {
		t.Fatalf("registry retry = db %p, attempts %d; want %p, 2", got, attempts, db)
	}
}

func TestExplicitSIDAllocatorDoesNotReadLegacyRegistryConfig(t *testing.T) {
	previous := config.Database
	config.Database = &config.DatabaseConfig{SIDRegistryURL: "://invalid"}
	defer func() { config.Database = previous }()

	allocator := NewSIDAllocatorForStorage("explicit:"+t.Name(), t.TempDir())
	registry, err := allocator.configuredSIDRegistry()
	if err != nil {
		t.Fatalf("explicit allocator consulted legacy registry config: %v", err)
	}
	if registry != nil {
		t.Fatal("explicit allocator unexpectedly inherited a legacy SID registry")
	}
}

func TestSIDAllocatorRejectsRegistrySwitchAfterLocalReservation(t *testing.T) {
	allocator := NewSIDAllocatorForNamespace("storage-switch")
	if got := allocator.reserveSID(exSymbolKey("test", "spot", "BTC/USDT"), 1); got != 1 {
		t.Fatalf("local SID reservation = %d, want 1", got)
	}
	registry := newSymbolSIDRegistryForDB(newSIDRegistryFakeDB())
	if err := allocator.BindSIDRegistry(registry); err == nil {
		t.Fatal("allocator switched SID authority after local reservation")
	}
}
