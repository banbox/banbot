package orm

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
)

var (
	pool           *pgxpool.Pool
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

var (
	DbTrades = "trades"
	// DbPub stores mutable relational/meta data (calendars/adj_factors/sranges/ins_kline/kline_un + ui task).
	DbPub = "banpub"
)

func Setup() *errs.Error {
	if pool != nil {
		pool.Close()
		pool = nil
	}
	var err2 *errs.Error
	pool, err2 = pgConnPool()
	if err2 != nil {
		return err2
	}
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
	return sess.UpdatePendingIns()
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
	dbCfg := config.Database
	if dbCfg == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "database config is missing!")
	}
	poolCfg, err_ := pgxpool.ParseConfig(dbCfg.Url)
	if err_ != nil {
		return nil, errs.New(core.ErrBadConfig, err_)
	}

	// Detect DB type from explicit config or port heuristic.
	dbType := strings.ToLower(strings.TrimSpace(dbCfg.DbType))
	port := uint16(0)
	if poolCfg.ConnConfig != nil {
		port = poolCfg.ConnConfig.Port
	}
	switch dbType {
	case "questdb":
		IsQuestDB = true
	case "timescale", "timescaledb", "postgres", "postgresql":
		IsQuestDB = false
	default:
		// Auto-detect by port: 8812 = QuestDB default, 5432 = PostgreSQL default.
		IsQuestDB = port != 5432
	}

	if IsQuestDB {
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
		return nil, errs.New(core.ErrDbConnFail, err_)
	}
	// Ping to verify connectivity; if QuestDB is not reachable, try to auto-start it.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err_ = dbPool.Ping(pingCtx); err_ != nil {
		dbPool.Close()
		if IsQuestDB {
			if ensureErr := ensureQuestDB(port); ensureErr != nil {
				return nil, ensureErr
			}
		} else {
			return nil, errs.New(core.ErrDbConnFail, err_)
		}
		// Retry after QuestDB is started.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer retryCancel()
		dbPool, err_ = pgxpool.NewWithConfig(retryCtx, poolCfg)
		if err_ != nil {
			return nil, errs.New(core.ErrDbConnFail, err_)
		}
	}

	// When auto-detect is still ambiguous (non-5432, non-8812), probe QuestDB-specific syntax.
	if dbType == "" && port != 5432 && port != 8812 {
		var n int64
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer probeCancel()
		if probeErr := dbPool.QueryRow(probeCtx, `select count() from tables()`).Scan(&n); probeErr != nil {
			// QuestDB probe failed – treat as TimescaleDB/PostgreSQL.
			IsQuestDB = false
		}
	}

	return dbPool, nil
}

func Conn(ctx context.Context) (*Queries, *pgxpool.Conn, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	return New(&SubQueries{db: conn}), conn, nil
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

	// 构建缓存键：路径+读写模式
	cacheKey := path
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
	// 添加 WAL 模式和其他性能优化参数
	openFlag := "_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-64000)"
	// 设置 mmap_size 为 300MB，利用内存映射 I/O 减少磁盘读写
	openFlag += "&_pragma=mmap_size(300000000)"
	// 确保 busy_timeout 始终设置，避免多进程锁冲突
	if timeoutMs <= 0 {
		timeoutMs = 5000 // 默认5秒超时
	}
	openFlag += fmt.Sprintf("&_pragma=busy_timeout(%d)", timeoutMs)
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

	// 初始化数据库结构（如果需要）
	dbPathLock.Lock()
	defer dbPathLock.Unlock()
	if _, ok := dbPathInit[path]; !ok {
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
		dbPathInit[path] = true
	}
	return db, nil
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
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	allowShow := false
	if it, ok := q.db.(*SubQueries); ok {
		allowShow = it.ShowLog
	}
	nq := q.WithTx(&SubQueries{db: tx, ShowLog: allowShow})
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
}

func (q *SubQueries) Begin(ctx context.Context) (pgx.Tx, error) {
	if tx, ok := q.db.(pgx.Tx); ok {
		return tx.Begin(ctx)
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
	exInfo := exchange.Info()
	args := make(map[string]interface{})
	if exInfo.ID == "china" && exInfo.MarketType != banexg.MarketSpot {
		items := GetExSymbols(exInfo.ID, exInfo.MarketType)
		symbols := make([]string, 0, len(items))
		for _, it := range items {
			if it.Symbol == "" {
				return nil, errs.NewMsg(errs.CodeRunTime, "symbol empty for sid: %v", it.ID)
			}
			symbols = append(symbols, it.Symbol)
		}
		args[banexg.ParamSymbols] = symbols
	}
	return exchange.LoadMarkets(reload, args)
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
		err = exchange.LoadLeverageBrackets(false, map[string]interface{}{
			banexg.ParamAccount: validAcc,
		})
		if err != nil {
			log.Error("LoadLeverageBrackets fail, skip, maint margin calculation may have large deviation",
				zap.String("err", err.Short()))
			err = exchange.InitLeverageBrackets()
			if err != nil {
				log.Warn("InitLeverageBrackets fail", zap.String("err", err.Short()))
			}
		}
	}
	return nil
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

func NewDbErr(code int, err_ error) *errs.Error {
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

// runQdbMigrations executes QuestDB schema migrations (best-effort, non-transactional).
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
			return NewDbErr(core.ErrDbExecFail, err)
		}
		if _, err := pool.Exec(ctx, `insert into schema_migrations (version, applied_ts) values ($1,$2)`, version, time.Now().UTC()); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
		currentVersion = version
	}
	if initVersion < currentVersion {
		log.Info("database migration completed", zap.Int64("from", initVersion), zap.Int64("to", currentVersion))
	}
	return nil
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
