package orm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// sidRegistryDB is intentionally smaller than DBTX. SID registration is a
// low-frequency metadata operation and never needs CopyFrom.
type sidRegistryDB interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// SIDRegistryReservation is the canonical identity returned by the shared
// registry. WriteTS is stable for a logical symbol and makes a QuestDB retry
// idempotent on exsymbol_q's (sid, ts) dedup key.
type SIDRegistryReservation struct {
	ExSymbol
	WriteTS time.Time
}

// SymbolSIDRegistry is an optional PostgreSQL authority for QuestDB symbol
// identities. It is deliberately separate from the QuestDB connection:
// QuestDB has neither a sequence nor a unique constraint suitable for this
// allocation problem.
type SymbolSIDRegistry struct {
	url        string
	autoCreate bool

	initMu  sync.Mutex
	db      sidRegistryDB
	closeFn func()
	closed  bool
	openFn  func(context.Context) (sidRegistryDB, func(), error)
}

// NewSymbolSIDRegistry validates the registry URL without opening a network
// connection. The pool is created on the first Reserve or Adopt call.
func NewSymbolSIDRegistry(url string, autoCreate bool) (*SymbolSIDRegistry, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, nil
	}
	if _, err := pgxpool.ParseConfig(normalizeDatabaseURL(url)); err != nil {
		return nil, fmt.Errorf("parse SID registry URL: %w", err)
	}
	return &SymbolSIDRegistry{url: url, autoCreate: autoCreate}, nil
}

// newSymbolSIDRegistryForDB is used by package tests to exercise the atomic
// logical-key contract without requiring a live PostgreSQL server.
func newSymbolSIDRegistryForDB(db sidRegistryDB) *SymbolSIDRegistry {
	return &SymbolSIDRegistry{db: db, openFn: func(context.Context) (sidRegistryDB, func(), error) {
		return db, func() {}, nil
	}}
}

func (r *SymbolSIDRegistry) URL() string {
	if r == nil {
		return ""
	}
	return r.url
}

// AutoCreate reports whether this registry is allowed to create or repair its
// schema. A Process must not silently reuse the same URL with a different
// schema policy: doing so makes construction order change runtime behavior.
func (r *SymbolSIDRegistry) AutoCreate() bool {
	if r == nil {
		return false
	}
	r.initMu.Lock()
	autoCreate := r.autoCreate
	r.initMu.Unlock()
	return autoCreate
}

func (r *SymbolSIDRegistry) database(ctx context.Context) (sidRegistryDB, error) {
	if r == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("SID registry is closed")
	}
	if r.db != nil {
		return r.db, nil
	}
	openFn := r.openFn
	if openFn == nil {
		openFn = r.open
	}
	db, closeFn, err := openFn(ctx)
	if err != nil {
		// Do not permanently cache a failed connection attempt. A transient
		// database outage must be recoverable on the next metadata operation.
		return nil, err
	}
	r.db = db
	r.closeFn = closeFn
	return db, nil
}

func (r *SymbolSIDRegistry) open(ctx context.Context) (sidRegistryDB, func(), error) {
	if strings.TrimSpace(r.url) == "" {
		return nil, nil, fmt.Errorf("SID registry URL is empty")
	}
	cfg, err := pgxpool.ParseConfig(normalizeDatabaseURL(r.url))
	if err != nil {
		return nil, nil, fmt.Errorf("parse SID registry URL: %w", err)
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 4
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open SID registry: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping SID registry: %w", err)
	}
	if r.autoCreate {
		if err := ensureSIDRegistrySchema(ctx, pool); err != nil {
			pool.Close()
			return nil, nil, err
		}
	}
	return pool, pool.Close, nil
}

func ensureSIDRegistrySchema(ctx context.Context, db sidRegistryDB) error {
	for _, stmt := range []string{
		`CREATE SEQUENCE IF NOT EXISTS public.exsymbol_sid_registry_seq AS integer`,
		`CREATE TABLE IF NOT EXISTS public.exsymbol_sid_registry (
  exchange  text NOT NULL,
  market    text NOT NULL,
  symbol    text NOT NULL,
  sid       integer NOT NULL DEFAULT nextval('public.exsymbol_sid_registry_seq'::regclass),
  write_ts  timestamptz NOT NULL DEFAULT clock_timestamp(),
  exg_real  text NOT NULL DEFAULT '',
  combined  boolean NOT NULL DEFAULT false,
  list_ms   bigint NOT NULL DEFAULT 0,
  delist_ms bigint NOT NULL DEFAULT 0,
  agg_rules text NOT NULL DEFAULT '',
		  PRIMARY KEY (exchange, market, symbol),
	  UNIQUE (sid)
)`,
		`ALTER SEQUENCE public.exsymbol_sid_registry_seq
OWNED BY public.exsymbol_sid_registry.sid`,
		`DO $$
DECLARE
  max_sid bigint;
  sequence_sid bigint;
  sequence_called boolean;
BEGIN
  SELECT COALESCE(MAX(sid), 0) INTO max_sid
  FROM public.exsymbol_sid_registry;
  SELECT last_value, is_called INTO sequence_sid, sequence_called
  FROM public.exsymbol_sid_registry_seq;
  IF max_sid > sequence_sid OR (max_sid = sequence_sid AND max_sid > 0 AND NOT sequence_called) THEN
    PERFORM setval(
      'public.exsymbol_sid_registry_seq'::regclass,
      max_sid,
      max_sid > 0
    );
  END IF;
END
$$`,
	} {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ensure SID registry schema: %w", err)
		}
	}
	return nil
}

// Reserve atomically creates or reads canonical IDs for logical symbols.
// Sequence gaps are expected when concurrent inserts lose a logical-key
// conflict; uniqueness and the returned row are the correctness boundary.
func (r *SymbolSIDRegistry) Reserve(ctx context.Context, arg []AddSymbolsParams) ([]SIDRegistryReservation, error) {
	if len(arg) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := r.database(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]SIDRegistryReservation, 0, len(arg))
	for _, item := range arg {
		row := db.QueryRow(ctx, `INSERT INTO public.exsymbol_sid_registry AS r
  (exchange, market, symbol, exg_real, combined, list_ms, delist_ms, agg_rules)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (exchange, market, symbol)
DO UPDATE SET write_ts = r.write_ts
RETURNING exchange, market, symbol, sid, write_ts, exg_real, combined, list_ms, delist_ms, agg_rules`,
			item.Exchange, item.Market, item.Symbol, item.ExgReal, item.Combined,
			item.ListMs, item.DelistMs, item.AggRules)
		reservation, scanErr := scanSIDRegistryReservation(row)
		if scanErr != nil {
			return nil, fmt.Errorf("reserve SID for %s: %w", exSymbolKey(item.Exchange, item.Market, item.Symbol), scanErr)
		}
		result = append(result, reservation)
	}
	return result, nil
}

// Adopt imports an already existing QuestDB symbol into the registry while
// preserving its physical SID. This is required for first deployment against
// an existing exsymbol_q catalog.
func (r *SymbolSIDRegistry) Adopt(ctx context.Context, item *ExSymbol) (SIDRegistryReservation, error) {
	if item == nil || item.ID <= 0 || item.Exchange == "" || item.Market == "" || item.Symbol == "" {
		return SIDRegistryReservation{}, fmt.Errorf("cannot adopt invalid exchange symbol")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := r.database(ctx)
	if err != nil {
		return SIDRegistryReservation{}, err
	}
	row := db.QueryRow(ctx, `INSERT INTO public.exsymbol_sid_registry AS r
  (exchange, market, symbol, sid, exg_real, combined, list_ms, delist_ms, agg_rules)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (exchange, market, symbol)
DO UPDATE SET write_ts = r.write_ts
RETURNING exchange, market, symbol, sid, write_ts, exg_real, combined, list_ms, delist_ms, agg_rules`,
		item.Exchange, item.Market, item.Symbol, item.ID, item.ExgReal, item.Combined,
		item.ListMs, item.DelistMs, item.AggRules)
	reservation, err := scanSIDRegistryReservation(row)
	if err != nil {
		return SIDRegistryReservation{}, fmt.Errorf("adopt SID %d for %s: %w", item.ID,
			exSymbolKey(item.Exchange, item.Market, item.Symbol), err)
	}
	if reservation.ID != item.ID {
		return SIDRegistryReservation{}, fmt.Errorf("registry SID conflict for %s: existing=%d physical=%d",
			exSymbolKey(item.Exchange, item.Market, item.Symbol), reservation.ID, item.ID)
	}
	return reservation, nil
}

// EnsureSIDFloor advances the registry sequence past physical SIDs already
// present in a QuestDB catalog. The physical catalog has no unique constraint
// on sid, so a fresh registry must not allocate an ID that predates it.
func (r *SymbolSIDRegistry) EnsureSIDFloor(ctx context.Context, floor int32) error {
	if r == nil || floor <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := r.database(ctx)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `SELECT setval(
  'public.exsymbol_sid_registry_seq'::regclass,
  GREATEST(
    COALESCE((SELECT MAX(sid) FROM public.exsymbol_sid_registry), 0),
    $1::bigint,
    (SELECT last_value FROM public.exsymbol_sid_registry_seq)
  ),
  true
)`, floor)
	if err != nil {
		return fmt.Errorf("advance SID registry sequence past physical SID %d: %w", floor, err)
	}
	return nil
}

func scanSIDRegistryReservation(row pgx.Row) (SIDRegistryReservation, error) {
	var item SIDRegistryReservation
	err := row.Scan(&item.Exchange, &item.Market, &item.Symbol, &item.ID, &item.WriteTS,
		&item.ExgReal, &item.Combined, &item.ListMs, &item.DelistMs, &item.AggRules)
	return item, err
}

func (r *SymbolSIDRegistry) Close() {
	if r == nil {
		return
	}
	r.initMu.Lock()
	if r.closed {
		r.initMu.Unlock()
		return
	}
	r.closed = true
	closeFn := r.closeFn
	r.closeFn = nil
	r.initMu.Unlock()
	if closeFn != nil {
		closeFn()
	}
}
