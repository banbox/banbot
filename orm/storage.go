package orm

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage owns one database pool and the immutable backend/coordination
// identity associated with that pool. Runtime-owned ORM objects must receive
// the same Storage instance instead of consulting package configuration.
type Storage struct {
	pool      *pgxpool.Pool
	questDB   bool
	identity  string
	legacy    bool
	closeFn   func()
	closeOnce sync.Once
}

// NewStorage binds an existing pool without taking ownership of its lifetime.
// This is useful when a process owns a shared pool and several runtimes use
// it. Call OpenStorage when Storage should own and close a newly opened pool.
func NewStorage(pool *pgxpool.Pool, questDB bool, identity string) *Storage {
	return &Storage{pool: pool, questDB: questDB, identity: strings.TrimSpace(identity)}
}

// OpenStorage opens a runtime-owned pool from an immutable database config.
// Schema migrations and application setup remain explicit lifecycle steps;
// opening a pool has no package-global side effects.
func OpenStorage(ctx context.Context, dbCfg *config.DatabaseConfig, dataDir string) (*Storage, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dbCfg == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "database config is missing")
	}
	dbPool, questDB, err := pgConnPoolForConfig(ctx, dbCfg)
	if err != nil {
		return nil, err
	}
	identity := CanonicalStorageIdentityForType("", dbCfg.Url, dataDir, dbCfg.DbType)
	storage := &Storage{pool: dbPool, questDB: questDB, identity: identity}
	storage.closeFn = dbPool.Close
	return storage, nil
}

// Pool returns the owned pool for low-level lifecycle operations. Callers must
// not close it unless they own the Storage.
func (s *Storage) Pool() *pgxpool.Pool {
	if s == nil {
		return nil
	}
	return s.pool
}

func (s *Storage) IsQuestDB() bool {
	return s != nil && s.questDB
}

func (s *Storage) Identity() string {
	if s == nil {
		return ""
	}
	return s.identity
}

// ValidateConfig verifies that this storage owner is the concrete backend
// selected by a runtime's immutable database configuration. Runtime-owned
// persistence must not silently use a pool for another database or backend.
func (s *Storage) ValidateConfig(dbCfg *config.DatabaseConfig, dataDir string) error {
	if s == nil {
		return fmt.Errorf("storage is nil")
	}
	if dbCfg == nil || strings.TrimSpace(dbCfg.Url) == "" {
		return nil
	}
	expectedIdentity := CanonicalStorageIdentityForType("", dbCfg.Url, dataDir, dbCfg.DbType)
	if expectedIdentity == "" || s.Identity() != expectedIdentity {
		return fmt.Errorf("storage identity %q does not match database identity %q", s.Identity(), expectedIdentity)
	}
	expectedQuestDB, known, err := databaseBackendForConfig(dbCfg)
	if err != nil {
		return err
	}
	if known && s.IsQuestDB() != expectedQuestDB {
		return fmt.Errorf("storage backend questdb=%t does not match database configuration questdb=%t", s.IsQuestDB(), expectedQuestDB)
	}
	return nil
}

// ProcessLockRoot is the stable process coordination root for this storage.
// It is shared by runtimes that point at the same storage identity and is
// distinct for independent databases.
func (s *Storage) ProcessLockRoot() string {
	if s == nil {
		return ""
	}
	return CompactProcessLockRootForIdentity(s.identity)
}

// Conn acquires a connection owned by this Storage and binds the resulting
// Queries to the same backend and coordination identity.
func (s *Storage) Conn(ctx context.Context) (*Queries, *pgxpool.Conn, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.pool == nil {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail, "storage pool is not configured")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	return NewWithStorage(&SubQueries{db: conn, storage: s}, s), conn, nil
}

// CurrentStorage returns the shared storage installed by the legacy ORM
// setup. It is an entrypoint-only bridge for callers that are migrating to
// Runtime-owned dependencies; domain code should keep using an explicit
// Storage or SymbolState instead.
func CurrentStorage() *Storage {
	storageMu.RLock()
	current := defaultStorage
	storageMu.RUnlock()
	if current != nil {
		return current
	}
	if pool == nil {
		return nil
	}
	current = NewStorage(pool, IsQuestDB,
		CanonicalStorageIdentityForType("", databaseURL(), config.GetDataDirSafe(), databaseType()))
	current.legacy = true
	storageMu.Lock()
	if defaultStorage == nil {
		defaultStorage = current
	} else {
		current = defaultStorage
	}
	storageMu.Unlock()
	return current
}

// NewQueries binds an existing DBTX to this Storage. It is primarily useful
// for test doubles and transaction wrappers; normal callers should use Conn.
func (s *Storage) NewQueries(db DBTX) *Queries {
	return NewWithStorage(db, s)
}

func (q *Queries) tableReadLock(ctx context.Context, table string) (func(), error) {
	return acquireQuestTableReadLockAtRoot(ctx, table, q.isQuestDB(), q.processLockRoot())
}

func (q *Queries) tableWriteLock(ctx context.Context, table string) (func(), bool, error) {
	return acquireQuestTableWriteLockAtRoot(ctx, table, q.isQuestDB(), q.processLockRoot())
}

func (s *SymbolState) Conn(ctx context.Context) (*Queries, *pgxpool.Conn, *errs.Error) {
	if s == nil {
		return Conn(ctx)
	}
	if storage := s.Storage(); storage != nil {
		q, conn, err := storage.Conn(ctx)
		if q != nil {
			q = q.WithSeriesSymbolState(s)
		}
		return q, conn, err
	}
	if s.identitySet {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail,
			"symbol state %s/%s is not bound to storage", s.identityExchange, s.identityMarket)
	}
	if !s.allowLegacyConn {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail,
			"explicit symbol state is not bound to storage")
	}
	q, conn, err := Conn(ctx)
	if q != nil {
		q = q.WithSeriesSymbolState(s)
	}
	return q, conn, err
}

// Close releases a pool opened by OpenStorage. Shared pools created through
// NewStorage are intentionally not closed here.
func (s *Storage) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.closeFn != nil {
			s.closeFn()
		}
	})
}

func (s *Storage) String() string {
	if s == nil {
		return "<nil>"
	}
	if s.identity == "" {
		return fmt.Sprintf("storage(questdb=%t)", s.questDB)
	}
	return fmt.Sprintf("storage(%s, questdb=%t)", s.identity, s.questDB)
}
