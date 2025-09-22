package orm

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"net"
	"runtime"
	"strconv"
	"strings"

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
	pool       *pgxpool.Pool
	dbPathMap  = make(map[string]string)
	dbPathInit = make(map[string]bool)
	dbPathLock = deadlock.Mutex{}
)

//go:embed sql/trade_schema.sql
var ddlTrade string

//go:embed sql/ui_schema.sql
var ddlUi string

//go:embed sql/pg_schema.sql
var ddlPg1 string

//go:embed sql/pg_schema2.sql
var ddlPg2 string

//go:embed sql/pg_migrations.sql
var ddlMigrations string

var ddlDbConf = `DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dbconf') THEN
        CREATE TABLE dbconf (
            key varchar(50) PRIMARY KEY not null,
            value text not null
        );
        INSERT INTO dbconf (key,value) VALUES ('schema_version', '0');
    END IF;
END $$;`

var (
	DbTrades = "trades"
	DbUI     = "ui"
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
	dbCfg := config.Database
	ctx := context.Background()
	row := pool.QueryRow(ctx, "SELECT COUNT(*) FROM pg_class WHERE relname = 'kinfo'")
	var kInfoCnt int64
	err := row.Scan(&kInfoCnt)
	if err != nil {
		dbErr := NewDbErr(core.ErrDbReadFail, err)
		if dbCfg.AutoCreate && dbErr.Code == core.ErrDbConnFail && dbErr.Message() == "db not exist" {
			// 数据库不存在，需要创建
			log.Warn("database not exist, creating...")
			err2 = createPgDb(dbCfg.Url)
			if err2 != nil {
				return err2
			}
		} else {
			return dbErr
		}
	}
	if kInfoCnt == 0 {
		// 表不存在，创建
		log.Warn("initializing database schema for kline ...")
		_, err = pool.Exec(ctx, ddlPg1)
		if err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
		_, err = pool.Exec(ctx, ddlPg2)
		if err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
	} else {
		// 执行数据库迁移
		err2 = runMigrations(ctx, pool)
		if err2 != nil {
			return err2
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

func pgConnPool() (*pgxpool.Pool, *errs.Error) {
	dbCfg := config.Database
	if dbCfg == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "database config is missing!")
	}
	poolCfg, err_ := pgxpool.ParseConfig(dbCfg.Url)
	if err_ != nil {
		return nil, errs.New(core.ErrBadConfig, err_)
	}
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
	dbPool, err_ := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err_ != nil {
		return nil, errs.New(core.ErrDbConnFail, err_)
	}
	return dbPool, nil
}

func createPgDb(dbUrl string) *errs.Error {
	// 连接到默认的postgres数据库
	tmpConfig, err_ := pgx.ParseConfig(dbUrl)
	if err_ != nil {
		return errs.New(core.ErrBadConfig, err_)
	}
	dbName := tmpConfig.Database
	tmpConfig.Database = "postgres"
	conn, err_ := pgx.ConnectConfig(context.Background(), tmpConfig)
	if err_ != nil {
		return errs.New(core.ErrDbConnFail, err_)
	}
	defer conn.Close(context.Background())

	_, err_ = conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	return nil
}

func Conn(ctx context.Context) (*Queries, *pgxpool.Conn, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	return New(conn), conn, nil
}

func SetDbPath(key, path string) {
	dbPathLock.Lock()
	dbPathMap[key] = path
	dbPathLock.Unlock()
}

func DbLite(src string, path string, write bool, timeoutMs int64) (*sql.DB, *errs.Error) {
	dbPathLock.Lock()
	defer dbPathLock.Unlock()
	if target, ok := dbPathMap[path]; ok {
		path = target
	}
	openFlag := ""
	if timeoutMs > 0 {
		openFlag += fmt.Sprintf("&_busy_timeout=%d", timeoutMs)
	}
	if write {
		openFlag += "&cache=shared&mode=rw"
	} else {
		openFlag += "&mode=ro"
	}
	var connStr = fmt.Sprintf("file:%s?%s", path, openFlag)
	db, err_ := sql.Open("sqlite", connStr)
	if err_ != nil {
		return nil, errs.New(core.ErrDbConnFail, err_)
	}
	if _, ok := dbPathInit[path]; !ok {
		ddl, tbl := ddlTrade, "bottask"
		if src == DbUI {
			ddl, tbl = ddlUi, "task"
		}
		checkSql := "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name=?;"
		var count int
		err_ = db.QueryRow(checkSql, tbl).Scan(&count)
		if err_ != nil || count == 0 {
			if write {
				// 数据库不存在，创建表
				db, err_ = sql.Open("sqlite", connStr+"c")
				if err_ != nil {
					return nil, errs.New(core.ErrDbConnFail, err_)
				}
				log.Info("init sqlite structure", zap.String("path", path))
				if _, err_ = db.Exec(ddl); err_ != nil {
					return nil, errs.New(core.ErrDbExecFail, err_)
				}
			} else if err_ != nil {
				return nil, errs.New(core.ErrDbExecFail, err_)
			} else {
				return nil, errs.NewMsg(core.ErrDbExecFail, "db is empty: %v", path)
			}
		}
		dbPathInit[path] = true
	}
	return db, nil
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

func (q *Queries) NewTx(ctx context.Context) (*Tx, *Queries, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, errs.New(core.ErrDbConnFail, err)
	}
	nq := q.WithTx(tx)
	return &Tx{tx: tx}, nq, nil
}

func (q *Queries) Exec(sql string, args ...interface{}) *errs.Error {
	_, err_ := q.db.Exec(context.Background(), sql, args...)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
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
		k.Info *= factor
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

// 执行数据库迁移
func runMigrations(ctx context.Context, pool *pgxpool.Pool) *errs.Error {
	// 1. 检查dbconf表是否存在
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dbconf')`).Scan(&exists)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}

	// 2. 如果表不存在，执行第一个迁移脚本创建表
	if !exists {
		_, err = pool.Exec(ctx, ddlDbConf)
		if err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
	}

	// 3. 获取当前版本
	var currentVersion int
	err = pool.QueryRow(ctx, "SELECT value::int FROM dbconf WHERE key = 'schema_version'").Scan(&currentVersion)
	if err != nil && !strings.Contains(err.Error(), "no rows") {
		return NewDbErr(core.ErrDbReadFail, err)
	}

	// 4. 解析迁移脚本
	migrations := strings.Split(ddlMigrations, "-- version")
	initVersion := currentVersion

	for _, migration := range migrations {
		if strings.TrimSpace(migration) == "" {
			continue
		}

		// 提取版本号
		lines := strings.SplitN(migration, "\n", 2)
		if len(lines) < 2 {
			continue
		}
		versionStr := strings.TrimSpace(lines[0])
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			log.Warn("invalid migration version", zap.String("version", versionStr))
			continue
		}
		if version <= currentVersion {
			continue
		}

		// 在事务中执行迁移
		tx, err := pool.Begin(ctx)
		if err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}

		// 执行迁移脚本
		_, err = tx.Exec(ctx, lines[1])
		if err != nil {
			tx.Rollback(ctx)
			return NewDbErr(core.ErrDbExecFail, err)
		}

		// 更新版本号
		_, err = tx.Exec(ctx, "UPDATE dbconf SET value = $1 WHERE key = 'schema_version'", versionStr)
		if err != nil {
			tx.Rollback(ctx)
			return NewDbErr(core.ErrDbExecFail, err)
		}

		// 提交事务
		err = tx.Commit(ctx)
		if err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}

		currentVersion = version
	}

	if initVersion < currentVersion {
		log.Info("database migration completed", zap.Int("from", initVersion), zap.Int("to", currentVersion))
	}
	return nil
}
