package orm

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/exg"
	utils2 "github.com/banbox/banbot/utils"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type questExecer interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

var (
	pool           *pgxpool.Pool
	defaultStorage *Storage
	storageMu      sync.RWMutex
	dbPathMap      = make(map[string]string)
	dbPathInit     = make(map[string]bool)
	dbPathLock     = deadlock.Mutex{}
	trackedDBs     = make(map[*TrackedDB]bool)
	trackedDBsLock = deadlock.Mutex{}
	// SQLite 连接池缓存：每个数据库路径对应一个 sql.DB 实例
	sqlitePools     = make(map[string]*sql.DB)
	sqlitePoolsLock = deadlock.Mutex{}

	// IsQuestDB QuestDB (PGWire, port 8812). TimescaleDB (standard PostgreSQL, port 5432).
	IsQuestDB = true
)

var (
	qdbMissingPartitionErrRe = regexp.MustCompile("Partition [`'\"]([^`'\"]+)[`'\"] does not exist in table [`'\"]([^`'\"]+)[`'\"] directory")
	qdbRepairStmtErrRe       = regexp.MustCompile(`(?i)ALTER TABLE ([A-Za-z0-9_]+) FORCE DROP PARTITION LIST ['"]([^'"]+)['"]`)
	qdbRepairTableNameRe     = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	qdbRepairPartitionRe     = regexp.MustCompile(`^[A-Za-z0-9:_.-]+$`)
)

//go:embed sql/trade_schema.sql
var ddlTrade string

//go:embed sql/ui_schema.sql
var ddlBanpub string

//go:embed sql/qdb_migrations.sql
var ddlQdbMigrations string

//go:embed sql/pg_schema.sql
var ddlPgSchema string

//go:embed sql/pg_schema2.sql
var ddlPgSchema2 string

//go:embed sql/pg_migrations.sql
var ddlPgMigrations string

const legacyPgCalendarRenameSQL = `DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'calendars' AND column_name = 'name'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'calendars' AND column_name = 'market'
    ) THEN
        ALTER TABLE public.calendars RENAME COLUMN name TO market;
    END IF;
END
$$;`

var (
	DbTrades = "trades"
	// DbPub stores mutable relational/meta data (calendars/adj_factors/sranges/ins_kline/kline_un + ui task).
	DbPub = "banpub"
)

func Setup() *errs.Error {
	return setup(false)
}

func SetupWithAutoCompact(autoCompact bool) *errs.Error {
	return setup(autoCompact)
}

func setup(autoCompact bool) *errs.Error {
	stopCompactWorker()
	storageMu.Lock()
	oldStorage := defaultStorage
	defaultStorage = nil
	storageMu.Unlock()
	if oldStorage != nil {
		oldStorage.closeFn = nil
	}
	if pool != nil {
		pool.Close()
		pool = nil
	}
	var err2 *errs.Error
	pool, err2 = pgConnPool()
	if err2 != nil {
		return err2
	}
	storageMu.Lock()
	defaultStorage = NewStorage(pool, IsQuestDB,
		CanonicalStorageIdentityForType("", databaseURL(), config.GetDataDirSafe(), databaseType()))
	defaultStorage.legacy = true
	storageMu.Unlock()
	initSQLitePaths()
	{
		// Ensure banpub.db exists and schema is initialized (task table only).
		db, err := BanPubConn(true)
		if err != nil {
			return err
		}
		_ = db.Close()
	}
	dbCfg := config.Database
	ctx := context.Background()
	if dbCfg != nil && dbCfg.AutoCreate {
		if IsQuestDB {
			if err := runQdbMigrations(ctx, pool); err != nil {
				return err
			}
		} else {
			if err := runPgMigrations(ctx, pool); err != nil {
				return err
			}
		}
	}
	log.Info("connect db ok", zap.String("url", utils2.MaskDBUrl(dbCfg.Url)), zap.Int("pool", dbCfg.MaxPoolSize),
		zap.Bool("questdb", IsQuestDB))
	err2 = LoadAllExSymbols()
	if err2 != nil {
		return err2
	}
	sess, conn, err2 := Conn(ctx)
	if err2 != nil {
		return err2
	}
	defer conn.Release()
	if exg.Default != nil {
		_, err2 = LoadMarkets(exg.Default, false)
		if err2 != nil {
			return err2
		}
	}
	if err2 = sess.UpdatePendingIns(); err2 != nil {
		return err2
	}
	if autoCompact {
		startCompactWorker()
	}
	return nil
}

func initSQLitePaths() {
	dataDir := config.GetDataDir()
	dbPath := filepath.Join(dataDir, "banpub.db")
	SetDbPath(DbPub, dbPath)
}

func execMultiSQL(ctx context.Context, pool *pgxpool.Pool, sqlText string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// Use pgconn.Exec (simple query protocol) to avoid prepared-statement
	// restrictions on DDL statements (e.g. CREATE TABLE IF NOT EXISTS).
	mrr := conn.Conn().PgConn().Exec(ctx, sqlText)
	for mrr.NextResult() {
		_, err2 := mrr.ResultReader().Close()
		if err2 != nil {
			_ = mrr.Close()
			return err2
		}
	}
	return mrr.Close()
}

func isQuestDuplicateColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column") || strings.Contains(msg, "column 'agg_rules' already exists")
}

// execMultiSQLTx executes multiple semicolon-separated SQL statements inside a pgx.Tx.
func execMultiSQLTx(ctx context.Context, tx pgx.Tx, sqlText string) error {
	// Use pgconn.Exec (simple query protocol) to avoid prepared-statement
	// restrictions on DDL statements.
	mrr := tx.Conn().PgConn().Exec(ctx, sqlText)
	for mrr.NextResult() {
		_, err := mrr.ResultReader().Close()
		if err != nil {
			_ = mrr.Close()
			return err
		}
	}
	return mrr.Close()
}

func pgConnPool() (*pgxpool.Pool, *errs.Error) {
	dbPool, questDB, err := pgConnPoolForConfig(context.Background(), config.Database)
	if err == nil {
		IsQuestDB = questDB
	}
	return dbPool, err
}

func pgConnPoolForConfig(ctx context.Context, dbCfg *config.DatabaseConfig) (*pgxpool.Pool, bool, *errs.Error) {
	if dbCfg == nil {
		return nil, false, errs.NewMsg(core.ErrBadConfig, "database config is missing!")
	}
	poolCfg, err_ := pgxpool.ParseConfig(normalizeDatabaseURL(dbCfg.Url))
	if err_ != nil {
		return nil, false, errs.New(core.ErrBadConfig, err_)
	}

	// Detect DB type from explicit config or port heuristic.
	questDB, backendKnown, backendErr := databaseBackendForPoolConfig(dbCfg, poolCfg)
	if backendErr != nil {
		return nil, false, errs.New(core.ErrBadConfig, backendErr)
	}
	port := uint16(0)
	if poolCfg.ConnConfig != nil {
		port = poolCfg.ConnConfig.Port
	}

	if questDB {
		// QuestDB uses the PostgreSQL wire protocol, but (unlike Postgres/TimescaleDB) it doesn't benefit from
		// pgx's statement cache when our SQL strings are dynamic. Disable the statement/describe cache and run
		// in non-caching exec mode to reduce per-query overhead (especially under concurrency).
		poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		poolCfg.ConnConfig.StatementCacheCapacity = 0
		poolCfg.ConnConfig.DescriptionCacheCapacity = 0
	}

	if dbCfg.MaxPoolSize == 0 {
		dbCfg.MaxPoolSize = max(40, runtime.NumCPU()*4)
	} else if dbCfg.MaxPoolSize < 30 {
		log.Warn("max_pool_size < 30 may cause connection exhaustion and hang during batch downloads",
			zap.Int("cur", dbCfg.MaxPoolSize))
	}
	poolCfg.MaxConns = int32(dbCfg.MaxPoolSize)
	connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer connCancel()
	dbPool, err_ := pgxpool.NewWithConfig(connCtx, poolCfg)
	if err_ != nil {
		return nil, false, errs.New(core.ErrDbConnFail, err_)
	}
	// Ping to verify connectivity; if QuestDB is not reachable, try to auto-start it.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err_ = dbPool.Ping(pingCtx); err_ != nil {
		dbPool.Close()
		if questDB {
			if ensureErr := ensureQuestDB(port); ensureErr != nil {
				return nil, false, ensureErr
			}
		} else {
			return nil, false, errs.New(core.ErrDbConnFail, err_)
		}
		// Retry after QuestDB is started.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer retryCancel()
		dbPool, err_ = pgxpool.NewWithConfig(retryCtx, poolCfg)
		if err_ != nil {
			return nil, false, errs.New(core.ErrDbConnFail, err_)
		}
	}

	// When auto-detect is still ambiguous (non-5432, non-8812), probe QuestDB-specific syntax.
	if !backendKnown {
		var n int64
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer probeCancel()
		if probeErr := dbPool.QueryRow(probeCtx, `select count() from tables()`).Scan(&n); probeErr != nil {
			// QuestDB probe failed – treat as TimescaleDB/PostgreSQL.
			questDB = false
		}
	}

	return dbPool, questDB, nil
}

// databaseBackendForConfig returns the backend selected without connecting.
// The result is marked unknown for non-standard ports because those require
// the same QuestDB probe used by the actual pool setup.
func databaseBackendForConfig(dbCfg *config.DatabaseConfig) (bool, bool, error) {
	if dbCfg == nil {
		return false, false, fmt.Errorf("database config is missing")
	}
	poolCfg, err := pgxpool.ParseConfig(normalizeDatabaseURL(dbCfg.Url))
	if err != nil {
		return false, false, err
	}
	return databaseBackendForPoolConfig(dbCfg, poolCfg)
}

func databaseBackendForPoolConfig(dbCfg *config.DatabaseConfig, poolCfg *pgxpool.Config) (bool, bool, error) {
	if dbCfg == nil || poolCfg == nil || poolCfg.ConnConfig == nil {
		return false, false, fmt.Errorf("database connection config is missing")
	}
	dbType := strings.ToLower(strings.TrimSpace(dbCfg.DbType))
	port := poolCfg.ConnConfig.Port
	switch dbType {
	case "questdb", "quest":
		return true, true, nil
	case "timescale", "timescaledb", "postgres", "postgresql", "postgresql+timescale":
		return false, true, nil
	case "":
		// Auto-detect by port: 8812 = QuestDB default, 5432 = PostgreSQL default.
		if port == 8812 {
			return true, true, nil
		}
		if port == 5432 {
			return false, true, nil
		}
		// Keep the historical optimistic QuestDB choice until the probe runs.
		return true, false, nil
	default:
		return false, false, fmt.Errorf("unsupported database type %q", dbCfg.DbType)
	}
}

// normalizeDatabaseURL accepts legacy configs that bracket an IPv4 host. URL
// brackets are valid for IPv6 only, but older worker configs used them around
// 127.0.0.1; normalize that form before pgx parses the connection string.
func normalizeDatabaseURL(raw string) string {
	return strings.ReplaceAll(raw, "@[127.0.0.1]", "@127.0.0.1")
}

func Conn(ctx context.Context) (*Queries, *pgxpool.Conn, *errs.Error) {
	storageMu.RLock()
	current := defaultStorage
	storageMu.RUnlock()
	if current != nil {
		return current.Conn(ctx)
	}
	if pool == nil {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail, "database pool is not configured")
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
	return current.Conn(ctx)
}

func SetDbPath(key, path string) {
	dbPathLock.Lock()
	dbPathMap[key] = path
	dbPathLock.Unlock()
}

// TrackedDB wraps sql.DB to track connection hold time and detect timeouts
type TrackedDB struct {
	*sql.DB
	acquireTime time.Time
	timeoutMs   int64
	path        string
	stack       string
	closed      bool
	mu          deadlock.Mutex
}

// Close marks the TrackedDB as closed and removes it from tracking
// 注意：不关闭底层 sql.DB，因为它是共享的连接池
func (t *TrackedDB) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()

	trackedDBsLock.Lock()
	delete(trackedDBs, t)
	trackedDBsLock.Unlock()

	return nil
}

// IsClosed returns whether the TrackedDB has been closed
func (t *TrackedDB) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func DbLite(src string, path string, write bool, timeoutMs int64) (*TrackedDB, *errs.Error) {
	dbPathLock.Lock()
	if target, ok := dbPathMap[path]; ok {
		path = target
	}
	dbPathLock.Unlock()

	// Include the schema source in the key. Trade and publication databases
	// may intentionally point at the same file during tests or migrations, but
	// they must not share a pool or a one-time schema initialization marker.
	cacheKey := src + "\x00" + path
	if write {
		cacheKey += ":write"
	} else {
		cacheKey += ":read"
	}

	// 从缓存获取或创建连接池
	sqlitePoolsLock.Lock()
	db, exists := sqlitePools[cacheKey]
	if !exists {
		var err *errs.Error
		db, err = newDbLite(src, path, write, timeoutMs)
		if err != nil {
			sqlitePoolsLock.Unlock()
			return nil, err
		}
		// 缓存连接池
		sqlitePools[cacheKey] = db
	}
	sqlitePoolsLock.Unlock()

	// Create tracked DB wrapper
	tracked := &TrackedDB{
		DB:          db,
		acquireTime: time.Now(),
		timeoutMs:   timeoutMs,
		path:        path,
		stack:       errs.CallStack(3, 20),
		closed:      false,
	}

	// Register for timeout monitoring
	if timeoutMs > 0 {
		trackedDBsLock.Lock()
		trackedDBs[tracked] = true
		trackedDBsLock.Unlock()

		// Start timeout monitor goroutine
		go monitorTimeout(tracked)
	}

	return tracked, nil
}

func newDbLite(src, path string, write bool, timeoutMs int64) (*sql.DB, *errs.Error) {
	if timeoutMs <= 0 {
		timeoutMs = 5000 // 默认5秒超时
	}
	// Keep the lock timeout explicit before other per-connection pragmas.
	openFlag := fmt.Sprintf("_pragma=busy_timeout(%d)", timeoutMs)
	openFlag += "&_pragma=synchronous(NORMAL)&_pragma=cache_size(-64000)"
	openFlag += "&_pragma=mmap_size(300000000)"
	if write {
		openFlag += "&cache=shared&mode=rwc"
	} else {
		openFlag += "&mode=ro"
	}

	var connStr = fmt.Sprintf("file:%s?%s", path, openFlag)
	db, err_ := sql.Open("sqlite", connStr)
	if err_ != nil {
		return nil, errs.New(core.ErrDbConnFail, err_)
	}

	// 配置连接池参数以支持多进程并发访问
	if write {
		// 写连接：限制为1个
		// 原因：SQLite WAL模式下同一时刻只允许一个写事务，多个连接会在SQLite层竞争锁
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(0) // 写连接不保持空闲，用完立即释放，避免阻塞其他进程
	} else {
		// 读连接：允许多个并发读取
		// WAL模式支持多个读操作同时进行，不会阻塞
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(1)
	}
	// 多进程场景：缩短连接生命周期，让其他进程有机会获取锁
	db.SetConnMaxLifetime(30 * time.Second)
	db.SetConnMaxIdleTime(1 * time.Second)
	if write {
		if err_ = enableSQLiteWAL(db, time.Duration(timeoutMs)*time.Millisecond); err_ != nil {
			_ = db.Close()
			return nil, errs.New(core.ErrDbExecFail, err_)
		}
	}

	// 初始化数据库结构（如果需要）
	dbPathLock.Lock()
	defer dbPathLock.Unlock()
	initKey := src + "\x00" + path
	if _, ok := dbPathInit[initKey]; !ok {
		ddl, tbl := ddlTrade, "bottask"
		if src == DbPub {
			ddl, tbl = ddlBanpub, "task"
		}
		checkSql := "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name=?;"
		var count int
		err_ = db.QueryRow(checkSql, tbl).Scan(&count)
		if err_ != nil || count == 0 {
			if write {
				// 数据库不存在，创建表
				log.Info("init sqlite structure", zap.String("path", path))
				if _, err_ = db.Exec(ddl); err_ != nil {
					return nil, errs.New(core.ErrDbExecFail, err_)
				}
			} else if err_ != nil {
				return nil, errs.New(core.ErrDbExecFail, err_)
			} else {
				return nil, errs.NewMsg(core.ErrDbExecFail, "db is empty: %v", path)
			}
		} else if write && src == DbPub {
			// Best-effort: ensure new tables/indexes are created when upgrading.
			if _, err_ = db.Exec(ddlBanpub); err_ != nil {
				return nil, errs.New(core.ErrDbExecFail, err_)
			}
		}
		dbPathInit[initKey] = true
	}
	return db, nil
}

type sqliteErrorCoder interface {
	Code() int
}

func isSQLiteBusy(err error) bool {
	var sqliteErr sqliteErrorCoder
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_BUSY
}

func enableSQLiteWAL(db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return retrySQLiteBusy(ctx, 50*time.Millisecond, func() error {
		_, err := db.Exec("PRAGMA journal_mode = WAL")
		return err
	})
}

func retrySQLiteBusy(ctx context.Context, delay time.Duration, fn func() error) error {
	for {
		err := fn()
		if err == nil || !isSQLiteBusy(err) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
	}
}

// monitorTimeout monitors a TrackedDB and logs an error if it exceeds the timeout
func monitorTimeout(tracked *TrackedDB) {
	if tracked.timeoutMs <= 0 {
		return
	}

	timeout := time.Duration(tracked.timeoutMs) * time.Millisecond
	time.Sleep(timeout)

	// Check if connection is still held
	if !tracked.IsClosed() {
		holdTime := time.Since(tracked.acquireTime)
		log.Error("SQLite connection held timeout",
			zap.String("path", tracked.path),
			zap.Duration("timeout", timeout),
			zap.Duration("held_for", holdTime),
			zap.String("stack", tracked.stack))
	}

	// Remove from tracking
	trackedDBsLock.Lock()
	delete(trackedDBs, tracked)
	trackedDBsLock.Unlock()
}

type Tx struct {
	tx     pgx.Tx
	closed bool
}

func (t *Tx) Close(ctx context.Context, commit bool) *errs.Error {
	if t.closed {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	if commit {
		err = t.tx.Commit(ctx)
	} else {
		err = t.tx.Rollback(ctx)
	}
	t.closed = true
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (q *Queries) SetShow(allow bool) {
	if it, ok := q.db.(*SubQueries); ok {
		it.ShowLog = allow
	}
}

func (q *Queries) NewTx(ctx context.Context) (*Tx, *Queries, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var tx pgx.Tx
	var err error
	if q != nil && q.storage != nil {
		if q.storage.pool == nil {
			err = fmt.Errorf("storage pool is not configured")
		} else {
			tx, err = q.storage.pool.Begin(ctx)
		}
	} else if pool != nil {
		tx, err = pool.Begin(ctx)
	} else {
		err = fmt.Errorf("database pool is not configured")
	}
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	allowShow := false
	if q != nil {
		if it, ok := q.db.(*SubQueries); ok {
			allowShow = it.ShowLog
		}
	}
	nq := q.WithTx(&SubQueries{db: tx, ShowLog: allowShow, storage: q.storage})
	return &Tx{tx: tx}, nq, nil
}

func (q *Queries) Exec(sql string, args ...interface{}) *errs.Error {
	_, err_ := q.db.Exec(context.Background(), sql, args...)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

type SubQueries struct {
	db      DBTX
	ShowLog bool
	storage *Storage
}

func (q *SubQueries) Begin(ctx context.Context) (pgx.Tx, error) {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Begin(ctx)
	}
	if beginner, ok := q.db.(dbBeginner); ok {
		return beginner.Begin(ctx)
	}
	return nil, fmt.Errorf("db is not pgx.Tx")
}

func (q *SubQueries) Commit(ctx context.Context) error {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Commit(ctx)
	}
	return fmt.Errorf("db is not pgx.Tx")
}

func (q *SubQueries) Rollback(ctx context.Context) error {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Rollback(ctx)
	}
	return fmt.Errorf("db is not pgx.Tx")
}

func (q *SubQueries) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.SendBatch(ctx, b)
	}
	panic("db is not pgx.Tx")
}

func (q *SubQueries) LargeObjects() pgx.LargeObjects {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.LargeObjects()
	}
	panic("db is not pgx.Tx")
}

func (q *SubQueries) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Prepare(ctx, name, sql)
	}
	return nil, fmt.Errorf("db is not pgx.Tx")
}

func (q *SubQueries) Conn() *pgx.Conn {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Conn()
	}
	panic("db is not pgx.Tx")
}

func (q *SubQueries) Exec(ctx context.Context, sql string, params ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	res, err := q.db.Exec(ctx, sql, params...)
	if q.ShowLog {
		log.Info("db exec", zap.String("sql", sql), zap.Duration("cost", time.Since(start)))
	}
	return res, err
}

func (q *SubQueries) Query(ctx context.Context, sql string, params ...interface{}) (pgx.Rows, error) {
	start := time.Now()
	res, err := q.db.Query(ctx, sql, params...)
	if q.ShowLog {
		log.Info("db query", zap.String("sql", sql), zap.Duration("cost", time.Since(start)))
	}
	return res, err
}

func (q *SubQueries) QueryRow(ctx context.Context, sql string, params ...interface{}) pgx.Row {
	start := time.Now()
	res := q.db.QueryRow(ctx, sql, params...)
	if q.ShowLog {
		log.Info("db QueryRow", zap.String("sql", sql), zap.Duration("cost", time.Since(start)))
	}
	return res
}

func (q *SubQueries) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	start := time.Now()
	res, err := q.db.CopyFrom(ctx, tableName, columnNames, rowSrc)
	if q.ShowLog {
		log.Info("db CopyFrom", zap.String("tbl", tableName.Sanitize()), zap.Duration("cost", time.Since(start)))
	}
	return res, err
}

func LoadMarkets(exchange banexg.BanExchange, reload bool) (banexg.MarketMap, *errs.Error) {
	return LoadMarketsWithSymbolState(nil, exchange, reload)
}

// LoadMarketsWithSymbolState loads exchange markets without consulting the
// process-wide symbol catalog. Runtime-owned callers pass their SymbolState so
// contract-market capability reloads stay on the same task identity.
func LoadMarketsWithSymbolState(state *SymbolState, exchange banexg.BanExchange, reload bool) (banexg.MarketMap, *errs.Error) {
	return loadMarketsWithRuntimeConfig(state, exchange, reload, &config.Data, config.GetDataDir(), nil, false)
}

// LoadMarketsWithRuntime loads markets using the runtime-owned configuration
// and core mode. The legacy LoadMarkets/LoadMarketsWithSymbolState facades
// pass nil and retain their process-wide behavior.
func LoadMarketsWithRuntime(state *SymbolState, exchange banexg.BanExchange, reload bool,
	snapshot *config.Snapshot, runtimeCore *core.State,
) (banexg.MarketMap, *errs.Error) {
	if snapshot == nil {
		return loadMarketsWithRuntimeConfig(state, exchange, reload, &config.Data, config.GetDataDir(), nil, false)
	}
	return loadMarketsWithRuntimeConfig(state, exchange, reload, snapshot.View(), snapshot.DataDir, runtimeCore, true)
}

// LoadMarketsWithRuntimeConfig is the config-level counterpart used by
// domain helpers that already carry a cloned Config but not its Snapshot.
// dataDir is the only filesystem root consulted for a configured snapshot.
func LoadMarketsWithRuntimeConfig(state *SymbolState, exchange banexg.BanExchange, reload bool,
	cfg *config.Config, dataDir string, runtimeCore *core.State,
) (banexg.MarketMap, *errs.Error) {
	return loadMarketsWithRuntimeConfig(state, exchange, reload, cfg, dataDir, runtimeCore, true)
}

func loadMarketsWithRuntimeConfig(state *SymbolState, exchange banexg.BanExchange, reload bool,
	cfg *config.Config, dataDir string, runtimeCore *core.State, explicit bool,
) (banexg.MarketMap, *errs.Error) {
	if exchange == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	if !explicit {
		legacyConfig := config.Data
		legacyConfig.Exchange = config.Exchange
		cfg = &legacyConfig
	}
	if hasConfiguredMarketSnapshotForConfig(cfg, runtimeCore, explicit) {
		markets := make(banexg.MarketMap)
		if err := applyConfiguredMarketSnapshotForConfig(cfg, dataDir, runtimeCore, explicit, exchange, markets); err != nil {
			return nil, err
		}
		return markets, nil
	}
	exInfo := exchange.Info()
	if exInfo == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "exchange info is required")
	}
	markets, err := exchange.LoadMarkets(reload, nil)
	if err != nil || len(markets) > 0 || !exchange.IsContract(exInfo.MarketType) {
		return markets, err
	}
	var items map[int32]*ExSymbol
	if state != nil {
		items = state.GetExSymbols(exInfo.ID, exInfo.MarketType)
	} else {
		items = GetExSymbols(exInfo.ID, exInfo.MarketType)
	}
	if len(items) == 0 {
		return markets, nil
	}
	loader := symbolScopedMarketLoader(exchange)
	if loader == nil {
		// A symbol-scoped reload is an adapter capability, not a generic
		// contract-market assumption. Keep the adapter's result unchanged when
		// it does not opt into that operation.
		return markets, nil
	}
	symbols := make([]string, 0, len(items))
	for _, it := range items {
		if it == nil || it.Symbol == "" {
			return nil, errs.NewMsg(errs.CodeRunTime, "symbol empty for cached market")
		}
		symbols = append(symbols, it.Symbol)
	}
	return loader.LoadMarketsForSymbols(true, symbols)
}

func symbolScopedMarketLoader(exchange banexg.BanExchange) banexg.SymbolScopedMarketLoader {
	if exchange == nil {
		return nil
	}
	if loader, ok := exchange.(banexg.SymbolScopedMarketLoader); ok {
		return loader
	}
	if wrapper, ok := exchange.(*exg.BotExchange); ok && wrapper != nil {
		loader, _ := wrapper.BanExchange.(banexg.SymbolScopedMarketLoader)
		return loader
	}
	return nil
}

func InitExg(exchange banexg.BanExchange) *errs.Error {
	// LoadMarkets will be called internally
	// 内部会调用LoadMarkets
	err := EnsureExgSymbols(exchange)
	if err != nil {
		return err
	}
	lastAcc, validAcc := "", ""
	for name, acc := range config.Accounts {
		if !acc.NoTrade {
			validAcc = name
		}
		lastAcc = name
	}
	if validAcc == "" {
		validAcc = lastAcc
	}
	marketType := exchange.Info().MarketType
	if marketType == banexg.MarketLinear || marketType == banexg.MarketInverse {
		initializeLeverageBrackets(exchange, validAcc)
	}
	return nil
}

func initializeLeverageBrackets(exchange banexg.BanExchange, account string) {
	if hasConfiguredMarketSnapshot() {
		if err := exchange.InitLeverageBrackets(); err != nil {
			log.Warn("InitLeverageBrackets fail", zap.String("err", err.Short()))
		}
		return
	}
	err := exchange.LoadLeverageBrackets(false, map[string]interface{}{
		banexg.ParamAccount: account,
	})
	if err == nil {
		return
	}
	log.Error("LoadLeverageBrackets fail, skip, maint margin calculation may have large deviation",
		zap.String("err", err.Short()))
	if err = exchange.InitLeverageBrackets(); err != nil {
		log.Warn("InitLeverageBrackets fail", zap.String("err", err.Short()))
	}
}

func (a *AdjInfo) Apply(bars []*banexg.Kline, adj int) []*banexg.Kline {
	if a == nil || a.CumFactor == 1 || a.CumFactor == 0 {
		return bars
	}
	result := make([]*banexg.Kline, 0, len(bars))
	factor := float64(1)
	if adj == core.AdjFront {
		factor = a.CumFactor
	} else if adj == core.AdjBehind {
		factor = 1 / a.CumFactor
	} else {
		return bars
	}
	for _, b := range bars {
		k := b.Clone()
		k.Open *= factor
		k.High *= factor
		k.Low *= factor
		k.Close *= factor
		k.Volume *= factor
		k.BuyVolume *= factor
		result = append(result, k)
	}
	return result
}

func isTransientDBConnError(err_ error) bool {
	if err_ == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err_, &opErr) {
		return true
	}
	if errors.Is(err_, io.EOF) {
		return true
	}
	msg := strings.ToLower(err_.Error())
	for _, marker := range []string{
		"broken pipe",
		"unexpected eof",
		"connection reset",
		"connection refused",
		"connection closed",
		"use of closed network connection",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func NewDbErr(code int, err_ error) *errs.Error {
	if isTransientDBConnError(err_) {
		return errs.New(core.ErrDbConnFail, err_)
	}
	var opErr *net.OpError
	var pgErr *pgconn.ConnectError
	if errors.As(err_, &opErr) {
		if strings.Contains(opErr.Err.Error(), "connection reset") {
			return errs.New(core.ErrDbConnFail, err_)
		}
	} else if errors.As(err_, &pgErr) {
		var errMsg = pgErr.Error()
		if strings.Contains(errMsg, "SQLSTATE 3D000") {
			return errs.NewMsg(core.ErrDbConnFail, "db not exist")
		}
	}
	return errs.New(code, err_)
}

type qdbPartitionRepair struct {
	Table     string
	Partition string
}

func (r qdbPartitionRepair) SQL() string {
	return fmt.Sprintf("ALTER TABLE %s FORCE DROP PARTITION LIST '%s'", r.Table, r.Partition)
}

func parseQuestDBMissingPartitionRepair(err error) *qdbPartitionRepair {
	if err == nil {
		return nil
	}
	msg := err.Error()
	match := qdbMissingPartitionErrRe.FindStringSubmatch(msg)
	if len(match) != 3 {
		return nil
	}
	repair := &qdbPartitionRepair{
		Partition: match[1],
		Table:     match[2],
	}
	if !qdbRepairTableNameRe.MatchString(repair.Table) || !qdbRepairPartitionRe.MatchString(repair.Partition) {
		return nil
	}
	stmtMatch := qdbRepairStmtErrRe.FindStringSubmatch(msg)
	if len(stmtMatch) == 3 {
		if stmtMatch[1] != repair.Table || stmtMatch[2] != repair.Partition {
			return nil
		}
	}
	return repair
}

func tryRepairQuestDBMissingPartition(ctx context.Context, db DBTX, err error, reason string) (bool, error) {
	if !IsQuestDB || db == nil {
		return false, nil
	}
	repair := parseQuestDBMissingPartitionRepair(err)
	if repair == nil {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	log.Warn("repairing questdb missing partition",
		zap.String("table", repair.Table),
		zap.String("partition", repair.Partition),
		zap.String("reason", reason),
		zap.Error(err))
	_, execErr := db.Exec(ctx, repair.SQL())
	if execErr != nil {
		return false, execErr
	}
	return true, nil
}

// runQdbMigrations executes QuestDB schema migrations as ordered, non-transactional
// steps. Every database, visibility, and migration error is returned to the caller.
func runQdbMigrations(ctx context.Context, pool *pgxpool.Pool) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Detect QuestDB (PGWire-compatible) vs Postgres/Timescale to avoid executing QuestDB-only DDL on Postgres.
	// QuestDB supports `count()` and `tables()`, while Postgres does not.
	{
		var n int64
		if err := pool.QueryRow(ctx, `select count() from tables()`).Scan(&n); err != nil {
			log.Warn("skip questdb migrations (non-questdb backend detected)", zap.Error(err))
			return nil
		}
	}
	unlockCompactTables, lockErr := lockAllCompactTablesRead(ctx)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlockCompactTables()

	log.Warn("running database migrations (questdb) ...")
	_, err := pool.Exec(ctx, `
create table if not exists schema_migrations (
  version long,
  applied_ts timestamp
) timestamp(applied_ts) partition by day;
`)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	var cur *int64
	if err := pool.QueryRow(ctx, `select max(version) from schema_migrations`).Scan(&cur); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	var currentVersion int64
	if cur != nil {
		currentVersion = *cur
	}
	initVersion := currentVersion

	migrations := strings.Split(ddlQdbMigrations, "-- version")
	for _, migration := range migrations {
		migration = strings.TrimSpace(migration)
		if migration == "" {
			continue
		}
		lines := strings.SplitN(migration, "\n", 2)
		if len(lines) < 2 {
			continue
		}
		versionStr := strings.TrimSpace(lines[0])
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			if strings.HasPrefix(versionStr, "--") {
				// Header comments before the first "-- version N" section.
				continue
			}
			log.Warn("invalid migration version", zap.String("version", versionStr))
			continue
		}
		if version <= currentVersion {
			continue
		}
		if err := execMultiSQL(ctx, pool, lines[1]); err != nil {
			if isQuestDuplicateColumnErr(err) {
				log.Warn("questdb migration column already exists, marking migration applied", zap.Int64("version", version), zap.Error(err))
			} else {
				return NewDbErr(core.ErrDbExecFail, err)
			}
		}
		if _, err := pool.Exec(ctx, `insert into schema_migrations (version, applied_ts) values ($1,$2)`, version, time.Now().UTC()); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
		currentVersion = version
	}
	if initVersion < currentVersion {
		log.Info("database migration completed", zap.Int64("from", initVersion), zap.Int64("to", currentVersion))
	}
	if err := ensureQuestDBCreateTables(ctx, pool, ddlQdbMigrations); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func ensureQuestDBCreateTables(ctx context.Context, db questExecer, ddl string) error {
	for _, stmt := range splitQuestDBStatements(ddl) {
		trimmed := strings.TrimSpace(stmt)
		lower := strings.ToLower(trimmed)
		if !strings.HasPrefix(lower, "create table if not exists ") {
			continue
		}
		if _, err := db.Exec(ctx, trimmed); err != nil {
			return err
		}
	}
	return nil
}

func splitQuestDBStatements(ddl string) []string {
	parts := strings.Split(ddl, ";")
	stmts := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		lines := strings.Split(trimmed, "\n")
		kept := make([]string, 0, len(lines))
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "--") {
				continue
			}
			kept = append(kept, line)
		}
		if len(kept) == 0 {
			continue
		}
		stmts = append(stmts, strings.Join(kept, "\n"))
	}
	return stmts
}

// runPgMigrations executes TimescaleDB/PostgreSQL schema migrations using schema_migrations table.
func runPgMigrations(ctx context.Context, pool *pgxpool.Pool) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	log.Warn("running database migrations (timescaledb) ...")

	// Check if sranges table already exists (base schema was applied).
	var tblCount int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_class WHERE relname = 'sranges'`).Scan(&tblCount)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if tblCount == 0 {
		// Some pre-migration installations have the legacy calendars.name
		// column but no sranges table. The current base schema creates indexes
		// on calendars.market, so reconcile that one prerequisite before the
		// base DDL rather than attempting the later migration after it fails.
		if _, err2 := pool.Exec(ctx, legacyPgCalendarRenameSQL); err2 != nil {
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		log.Info("initializing timescaledb base schema...")
		if err2 := execMultiSQL(ctx, pool, ddlPgSchema); err2 != nil {
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		if err2 := execMultiSQL(ctx, pool, ddlPgSchema2); err2 != nil {
			return NewDbErr(core.ErrDbExecFail, err2)
		}
	}

	// Ensure schema_migrations table exists.
	if _, err2 := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
  version     bigint PRIMARY KEY,
  applied_at  timestamptz NOT NULL DEFAULT now()
)`); err2 != nil {
		return NewDbErr(core.ErrDbExecFail, err2)
	}

	var cur *int64
	if err2 := pool.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&cur); err2 != nil {
		return NewDbErr(core.ErrDbReadFail, err2)
	}
	var currentVersion int64
	if cur != nil {
		currentVersion = *cur
	}
	initVersion := currentVersion

	migrations := strings.Split(ddlPgMigrations, "-- version")
	for _, migration := range migrations {
		migration = strings.TrimSpace(migration)
		if migration == "" {
			continue
		}
		lines := strings.SplitN(migration, "\n", 2)
		if len(lines) < 2 {
			continue
		}
		versionStr := strings.TrimSpace(lines[0])
		version, err2 := strconv.ParseInt(versionStr, 10, 64)
		if err2 != nil {
			if strings.HasPrefix(versionStr, "--") || strings.Contains(versionStr, "<") {
				continue
			}
			log.Warn("invalid pg migration version", zap.String("version", versionStr))
			continue
		}
		if version <= currentVersion {
			continue
		}
		tx, err2 := pool.Begin(ctx)
		if err2 != nil {
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		// Execute DDL statements inside the transaction (pgx.Tx also implements Exec).
		if err2 = execMultiSQLTx(ctx, tx, lines[1]); err2 != nil {
			_ = tx.Rollback(ctx)
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		if _, err2 = tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, version); err2 != nil {
			_ = tx.Rollback(ctx)
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		if err2 = tx.Commit(ctx); err2 != nil {
			return NewDbErr(core.ErrDbExecFail, err2)
		}
		currentVersion = version
	}
	if initVersion < currentVersion {
		log.Info("pg migration completed", zap.Int64("from", initVersion), zap.Int64("to", currentVersion))
	}
	return nil
}

// buildBatchValues builds a multi-row VALUES clause for a batch INSERT.
//
// n is the number of rows, cols is the number of bound parameters per row.
// suffix is an optional string of literal SQL tokens appended after the bound
// parameters of every row (e.g. ",true" to add a literal boolean column).
// It returns the VALUES string ready to be concatenated after the INSERT header.
//
// Example: buildBatchValues(2, 3, ",false")
//
//	→ "($1,$2,$3,false),($4,$5,$6,false)"
func buildBatchValues(n, cols int, suffix string) string {
	var b strings.Builder
	b.Grow(n * (cols*4 + len(suffix) + 3))
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		base := i*cols + 1
		for c := range cols {
			if c > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(base + c))
		}
		b.WriteString(suffix)
		b.WriteByte(')')
	}
	return b.String()
}

// boolLit returns the SQL literal for a Go bool.
func boolLit(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
