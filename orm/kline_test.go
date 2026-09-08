package orm

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

func initApp() *errs.Error {
	var args config.CmdArgs
	err := config.LoadConfig(&args)
	if err != nil {
		return err
	}
	config.Args.SetLog(true)
	err = exg.Setup()
	if err != nil {
		return err
	}
	return Setup()
}

func TestGetKrange(t *testing.T) {
	t.Skip("integration test (requires database)")
	err := initApp()
	if err != nil {
		panic(err)
	}
	sess, conn, err := Conn(nil)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	start, stop := sess.GetKlineRange(12, "1m")
	log.Info("krange", zap.Int64("start", start), zap.Int64("stop", stop))
}

func TestBuildQuestKlineRewriteSQLKeepsDynamicColumnsAndWAL(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "open", Type: "DOUBLE"},
		{Name: "signal", Type: "STRING"},
		{Name: "quality", Type: "LONG"},
	}
	got, err := buildQuestKlineRewriteSQLChecked("kline_1m_compact", "kline_1m", `"sid" IN (7)`, "week", columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "sid", "ts", "open", "signal", "quality"`,
		`FROM "kline_1m"`,
		`WHERE "sid" IN (7)`,
		`TIMESTAMP("ts") PARTITION BY WEEK WAL`,
		`DEDUP UPSERT KEYS("sid", "ts")`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("rewrite SQL %q missing %q", got, want)
		}
	}
}

func TestBuildQuestKlineRewriteSQLRejectsSchemaWithoutDesignatedTimestamp(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
	}
	if _, err := buildQuestKlineRewriteSQLChecked("tmp", "kline_1m", `"sid" = 7`, "WEEK", columns); err == nil {
		t.Fatal("expected missing designated timestamp to reject CTAS")
	}
}

func TestBuildQuestKlineAggregateQueryUsesDesignatedSnapshotColumn(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "event_time", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "open", Type: "DOUBLE"},
		{Name: "signal", Type: "STRING"},
	}
	query, fields, err := buildQuestKlineAggregateQuery("kline_custom", columns)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, `cast("event_time" as long)/1000`) ||
		!strings.Contains(query, `WHERE "sid" = $1 AND "event_time" >= $2`) ||
		strings.Contains(query, `"event_time", "open"`) {
		t.Fatalf("aggregate query did not use the schema timestamp correctly: %q", query)
	}
	if !reflect.DeepEqual(fields, []string{"open", "signal"}) {
		t.Fatalf("unexpected aggregate fields: %v", fields)
	}
}

func TestBuildQuestRewriteSQLUsesTableColumnSchemaProperties(t *testing.T) {
	columns := []questTableColumn{
		{Name: "instrument", Type: "SYMBOL", UpsertKey: true},
		{Name: "event_time", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "metric", Type: "DOUBLE"},
		{Name: "optional_note", Type: "STRING"},
	}

	got, err := buildQuestRewriteSQLChecked("series_compact", "series", "instrument = 'BTC'", "month", "event_time", columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "instrument", "event_time", "metric", "optional_note"`,
		`FROM "series"`,
		`TIMESTAMP("event_time") PARTITION BY MONTH WAL`,
		`DEDUP UPSERT KEYS("instrument", "event_time")`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("rewrite SQL %q missing %q", got, want)
		}
	}
	if strings.Contains(got, `DEDUP UPSERT KEYS("sid", "ts")`) || strings.Contains(got, `TIMESTAMP("ts")`) {
		t.Fatalf("rewrite SQL hard-coded schema properties: %q", got)
	}
}

func TestInsertKLinesLockedDoesNotReacquireTableReadLock(t *testing.T) {
	withFreshCompactState(t)
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	const table = "kline_1m"
	tableLock := cptState.getTableLock(table)
	tableLock.RLock()
	released := false
	t.Cleanup(func() {
		if !released {
			tableLock.RUnlock()
		}
	})

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	go func() {
		close(writerStarted)
		tableLock.Lock()
		close(writerAcquired)
		tableLock.Unlock()
	}()
	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	execs := 0
	db := &visibilityDBStub{exec: func(_ string, _ ...interface{}) (pgconn.CommandTag, error) {
		execs++
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}}
	done := make(chan *errs.Error, 1)
	go func() {
		_, err := New(db).insertKLinesLocked("1m", 7, []*banexg.Kline{{Time: 1_700_000_000_000}})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("locked kline insert failed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("locked kline insert blocked behind its own queued writer")
	}
	if execs != 1 {
		t.Fatalf("expected one insert statement, got %d", execs)
	}
	tableLock.RUnlock()
	released = true
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued table writer did not acquire after read lock release")
	}
}

func TestInsertOHLCVRowsLockedDoesNotReacquireTableReadLock(t *testing.T) {
	withFreshCompactState(t)
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })

	const table = "kline_1m"
	tableLock := cptState.getTableLock(table)
	tableLock.RLock()
	released := false
	t.Cleanup(func() {
		if !released {
			tableLock.RUnlock()
		}
	})

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	go func() {
		close(writerStarted)
		tableLock.Lock()
		close(writerAcquired)
		tableLock.Unlock()
	}()
	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	execs := 0
	db := &visibilityDBStub{exec: func(_ string, _ ...interface{}) (pgconn.CommandTag, error) {
		execs++
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}}
	done := make(chan *errs.Error, 1)
	go func() {
		_, err := New(db).insertOHLCVRowsLocked("1m", []*DataSeries{{
			Sid: 7, TimeMS: 1_700_000_000_000,
			Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0},
		}})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("locked OHLCV insert failed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("locked OHLCV insert blocked behind its own queued writer")
	}
	if execs != 1 {
		t.Fatalf("expected one insert statement, got %d", execs)
	}
	tableLock.RUnlock()
	released = true
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued table writer did not acquire after read lock release")
	}
}

type adjustmentUnavailableExchange struct {
	banexg.BanExchange
	info *banexg.ExgInfo
}

func (e *adjustmentUnavailableExchange) Info() *banexg.ExgInfo { return e.info }

type adjustmentProviderExchange struct {
	banexg.BanExchange
	info   *banexg.ExgInfo
	called bool
}

func (e *adjustmentProviderExchange) Info() *banexg.ExgInfo { return e.info }

func (e *adjustmentProviderExchange) CalcAdjFactors(*config.CmdArgs) *errs.Error {
	e.called = true
	return nil
}

func TestCalcAdjFactorsReturnsExplicitMissingCapabilityError(t *testing.T) {
	previous := exg.Default
	t.Cleanup(func() { exg.Default = previous })
	exg.Default = &adjustmentUnavailableExchange{
		info: &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketSpot},
	}
	err := CalcAdjFactors(&config.CmdArgs{OutPath: t.TempDir()})
	if err == nil || err.Code != errs.CodeNotImplement {
		t.Fatalf("CalcAdjFactors error = %v, want CodeNotImplement", err)
	}
	if strings.Contains(strings.ToLower(err.Message()), "china") {
		t.Fatalf("missing capability error leaked an exchange-specific fallback: %v", err.Message())
	}
}

func TestCalcAdjFactorsUsesAdapterCapability(t *testing.T) {
	previous := exg.Default
	t.Cleanup(func() { exg.Default = previous })
	exchange := &adjustmentProviderExchange{
		info: &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketSpot},
	}
	exg.Default = exchange
	if err := CalcAdjFactors(&config.CmdArgs{OutPath: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if !exchange.called {
		t.Fatal("adapter adjustment-factor capability was not called")
	}
}

func TestCalcAdjFactorsAcceptsExplicitCalculator(t *testing.T) {
	args := &config.CmdArgs{OutPath: t.TempDir()}
	called := false
	err := CalcAdjFactors(args, AdjFactorCalculator(func(got *config.CmdArgs) *errs.Error {
		called = got == args
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("explicit adjustment-factor calculator was not called")
	}
}

func TestCalcAdjFactorsPreservesArgumentValidation(t *testing.T) {
	if err := CalcAdjFactors(nil); err == nil || err.Code != errs.CodeParamRequired {
		t.Fatalf("nil args error = %v, want CodeParamRequired", err)
	}
	if err := CalcAdjFactors(&config.CmdArgs{}); err == nil || err.Code != errs.CodeParamRequired {
		t.Fatalf("missing output error = %v, want CodeParamRequired", err)
	}
	if err := CalcAdjFactors(&config.CmdArgs{OutPath: t.TempDir()},
		AdjFactorCalculator(func(*config.CmdArgs) *errs.Error { return nil }),
		AdjFactorCalculator(func(*config.CmdArgs) *errs.Error { return nil })); err == nil || err.Code != errs.CodeParamInvalid {
		t.Fatalf("duplicate calculator error = %v, want CodeParamInvalid", err)
	}
}
