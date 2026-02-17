package orm

import (
	"bufio"
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"os"
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
)

//go:embed sql/trade_schema.sql
var ddlTrade string

//go:embed sql/banpub_schema.sql
var ddlBanpub string

//go:embed sql/qdb_migrations.sql
var ddlQdbMigrations string

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
	dataDir := config.GetDataDir()
	dbPath := filepath.Join(dataDir, "banpub.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err2 := checkQuestDBStale(pool); err2 != nil {
			return err2
		}
	}
	initSQLitePaths()
	{
		// Ensure banpub.db exists and schema is initialized before any read-only opens.
		db, err := BanPubConn(true)
		if err != nil {
			return err
		}
		_ = db.Close()
	}
	dbCfg := config.Database
	ctx := context.Background()
	if dbCfg != nil && dbCfg.AutoCreate {
		if err := runMigrations(ctx, pool); err != nil {
			return err
		}
	}
	log.Info("connect db ok", zap.String("url", utils2.MaskDBUrl(dbCfg.Url)), zap.Int("pool", dbCfg.MaxPoolSize))
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

// checkQuestDBStale is called when banpub.db was freshly created. If QuestDB
// already contains data the sid references are now invalid; ask the user
// whether to wipe QuestDB or abort.
func checkQuestDBStale(p *pgxpool.Pool) *errs.Error {
	ctx := context.Background()
	// List user tables in QuestDB via the tables() system function.
	rows, err := p.Query(ctx, `select table_name from tables()`, pgx.QueryExecModeDescribeExec)
	if err != nil {
		// Non-QuestDB backend or empty instance – nothing to do.
		return nil
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	if len(tables) == 0 {
		return nil
	}
	// Check whether any table actually has rows.
	hasData := false
	for _, tbl := range tables {
		var n int64
		if err := p.QueryRow(ctx, fmt.Sprintf(`select count() from '%s'`, tbl)).Scan(&n); err == nil && n > 0 {
			hasData = true
			break
		}
	}
	if !hasData {
		return nil
	}
	// Prompt user.
	msg := config.GetLangMsg("sqlite_empty_questdb_stale", "SQLite is empty but QuestDB has data (stale). Clear all QuestDB data?")
	fmt.Printf("\n%s [y/N]: ", msg)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
	if answer != "y" && answer != "yes" {
		panic(config.GetLangMsg("sqlite_questdb_abort", "User declined to clear stale QuestDB data, aborting."))
	}
	// Drop all tables.
	log.Warn("dropping all QuestDB tables ...", zap.Int("count", len(tables)))
	for _, tbl := range tables {
		if _, err := p.Exec(ctx, fmt.Sprintf(`DROP TABLE '%s'`, tbl)); err != nil {
			log.Error("drop QuestDB table failed", zap.String("table", tbl), zap.Error(err))
		}
	}
	log.Info("QuestDB tables cleared")
	return nil
}

func execMultiSQL(ctx context.Context, pool *pgxpool.Pool, sqlText string) error {
	stmts := strings.Split(sqlText, ";")
	for _, st := range stmts {
		s := strings.TrimSpace(st)
		if s == "" {
			continue
		}
		if _, err := pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
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
	if poolCfg.ConnConfig != nil {
		if poolCfg.ConnConfig.Port == 5432 {
			return nil, errs.NewMsg(core.ErrBadConfig, "port 5432 is old TimescaleDB, please update database.url to: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable")
		}
		host := strings.TrimSpace(poolCfg.ConnConfig.Host)
		if host != "" && !isLocalHost(host) {
			return nil, errs.NewMsg(core.ErrBadConfig, "questdb must be local-only (host=%s)", host)
		}
		for _, fb := range poolCfg.ConnConfig.Fallbacks {
			h := strings.TrimSpace(fb.Host)
			if h != "" && !isLocalHost(h) {
				return nil, errs.NewMsg(core.ErrBadConfig, "questdb must be local-only (fallback host=%s)", h)
			}
		}
	}
	// QuestDB uses the PostgreSQL wire protocol, but (unlike Postgres/TimescaleDB) it doesn't benefit from
	// pgx's statement cache when our SQL strings are dynamic. Disable the statement/describe cache and run
	// in non-caching exec mode to reduce per-query overhead (especially under concurrency).
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	poolCfg.ConnConfig.StatementCacheCapacity = 0
	poolCfg.ConnConfig.DescriptionCacheCapacity = 0
	if dbCfg.MaxPoolSize == 0 {
		dbCfg.MaxPoolSize = max(40, runtime.NumCPU()*4)
	} else if dbCfg.MaxPoolSize < 30 {
		log.Warn("max_pool_size < 30 may cause connection exhaustion and hang during batch downloads",
			zap.Int("cur", dbCfg.MaxPoolSize))
	}
	poolCfg.MaxConns = int32(dbCfg.MaxPoolSize)
	//poolCfg.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
	//	return true
	//}
	//poolCfg.AfterRelease = func(conn *pgx.Conn) bool {
	//  // 此函数不是在调用Release时必定被调用，连接可能直接被Destroy而不释放到池
	//	return true
	//}
	//poolCfg.BeforeClose = func(conn *pgx.Conn) {
	//	log.Info(fmt.Sprintf("close conn: %v", conn))
	//}
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
		if ensureErr := ensureQuestDB(poolCfg.ConnConfig.Port); ensureErr != nil {
			return nil, ensureErr
		}
		// Retry after QuestDB is started.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer retryCancel()
		dbPool, err_ = pgxpool.NewWithConfig(retryCtx, poolCfg)
		if err_ != nil {
			return nil, errs.New(core.ErrDbConnFail, err_)
		}
	}
	return dbPool, nil
}

func isLocalHost(host string) bool {
	host = strings.TrimSpace(host)
	host = strings.Trim(host, "[]") // tolerate IPv6 literals like [::1]
	if host == "" {
		return false
	}
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	return false
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
	if timeoutMs > 0 {
		openFlag += fmt.Sprintf("&_pragma=busy_timeout(%d)", timeoutMs)
	}
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
			ddl, tbl = ddlBanpub, "calendars"
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

// runMigrations executes QuestDB schema migrations (best-effort, non-transactional).
func runMigrations(ctx context.Context, pool *pgxpool.Pool) *errs.Error {
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
