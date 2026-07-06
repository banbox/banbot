package data

import (
	"context"
	"fmt"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

func TestWatchOhlcv(t *testing.T) {
	t.Skip("integration test (requires watcher on 127.0.0.1:6789)")
	core.SetRunMode(core.RunModeLive)
	err := initApp()
	if err != nil {
		panic(err)
	}
	client, err := NewSeriesWatcher("127.0.0.1:6789")
	if err != nil {
		panic(err)
	}
	client.OnDataMsg = func(msg *SeriesMsg) {
		if len(msg.Arr) == 0 {
			return
		}
		code := fmt.Sprintf("%s.%s.%s", msg.ExgName, msg.Market, msg.Pair)
		k := msg.Arr[0]
		dateStr := btime.ToDateStr(k.Time, core.DefaultDateFmt)
		barStr := fmt.Sprintf("%f %f %f %f %f", k.Open, k.High, k.Low, k.Close, k.Volume)
		log.Info("receive", zap.String("code", code), zap.Int("num", len(msg.Arr)),
			zap.Int("tfSecs", msg.TFSecs), zap.Int("intv", msg.Interval),
			zap.String("date", dateStr), zap.String("bar", barStr))
	}
	market, quote := banexg.MarketLinear, "USDT"
	codes := []string{"BTC", "ETH", "SOL"}
	jobs := make([]WatchJob, 0, len(codes))
	for _, code := range codes {
		var symbol string
		if market == banexg.MarketSpot {
			symbol = fmt.Sprintf("%s/%s", code, quote)
		} else if market == banexg.MarketLinear {
			symbol = fmt.Sprintf("%s/%s:%s", code, quote, quote)
		} else if market == banexg.MarketInverse {
			symbol = fmt.Sprintf("%s/%s:%s", quote, code, quote)
		} else {
			panic("unsupported market")
		}
		jobs = append(jobs, WatchJob{
			Symbol:    symbol,
			TimeFrame: "1m",
		})
	}
	err = client.WatchJobs("binance", market, "ohlcv", jobs...)
	if err != nil {
		panic(err)
	}
	err = client.RunForever()
	if err != nil {
		panic(err)
	}
}

func initApp() *errs.Error {
	var args config.CmdArgs
	args.Init()
	errs.PrintErr = utils.PrintErr
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	core.StopAll = cancel
	err := config.LoadConfig(&args)
	if err != nil {
		return err
	}
	config.Args.SetLog(true)
	err = exg.Setup()
	if err != nil {
		return err
	}
	return orm.Setup()
}

func TestSaveKlines(t *testing.T) {
	t.Skip("integration test (requires writable kline tables and backend-specific SQL support)")
	err := initApp()
	if err != nil {
		panic(err)
	}
	var arr []*banexg.Kline
	err_ := utils2.ReadJsonFile("testdata/btc_1m.json", &arr, utils2.JsonNumDefault)
	if err_ != nil {
		panic(err_)
	}
	sid := int32(-1)
	timeFrame := "1m"
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		panic(err)
	}
	err = sess.Exec(fmt.Sprintf(`
delete from kline_1m where sid=%v;
delete from kline_5m where sid=%v;
delete from kline_15m where sid=%v;
delete from kline_1h where sid=%v;
delete from kline_1d where sid=%v;
`, sid, sid, sid, sid, sid))
	if err != nil {
		panic(err)
	}
	conn.Release()
	{
		sess, conn, err = orm.Conn(nil)
		if err != nil {
			panic(err)
		}
		_ = sess.DelKLineUn(sid, timeFrame)
		_ = sess.DelKInfo(sid, timeFrame)
		conn.Release()
	}
	core.SetRunMode(core.RunModeBackTest)
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	exs := orm.GetSymbolByID(sid)
	for i, bar := range arr {
		btime.CurTimeMS = bar.Time + tfMSecs
		sess, conn, err = orm.Conn(nil)
		if err != nil {
			panic(err)
		}
		_, err = sess.InsertKLinesAuto(timeFrame, exs, []*banexg.Kline{bar}, true)
		conn.Release()
		if i == 8 {
			break
		}
	}
}

type stubSpiderQueries struct {
	purgeCount int
	purgeErr   *errs.Error
}

func (s *stubSpiderQueries) PurgeKlineUn() *errs.Error {
	s.purgeCount++
	return s.purgeErr
}

type stubSpiderConn struct {
	releaseCount int
}

func (s *stubSpiderConn) Release() {
	s.releaseCount++
}

func newSpiderTestDeps(t *testing.T, recorder *runSpiderTestRecorder) spiderRuntimeDeps {
	t.Helper()
	return spiderRuntimeDeps{
		newServer: func(addr, aesKey string) *utils.ServerIO {
			recorder.addr = addr
			return &utils.ServerIO{Addr: addr, Data: map[string]string{}, DataExp: map[string]int64{}}
		},
		startWriteQ: func(workNum int) {
			recorder.writeQCalls++
			recorder.writeQWorkers = append(recorder.writeQWorkers, workNum)
		},
		ormConn: func(ctx context.Context) (spiderQueries, spiderConnRelease, *errs.Error) {
			recorder.ormConnCalls++
			recorder.ormCtx = ctx
			return recorder.queries, recorder.conn, recorder.ormErr
		},
		runForever: func(spider *LiveSpider) *errs.Error {
			recorder.runForeverCalls++
			recorder.runForeverSpider = spider
			return recorder.runErr
		},
		startMonitor: func(spider *LiveSpider) {
			recorder.monitorCalls++
		},
		startCron: func() {
			recorder.cronCalls++
		},
	}
}

type runSpiderTestRecorder struct {
	addr             string
	writeQCalls      int
	writeQWorkers    []int
	ormConnCalls     int
	ormCtx           context.Context
	runForeverCalls  int
	monitorCalls     int
	cronCalls        int
	runForeverSpider *LiveSpider
	queries          *stubSpiderQueries
	conn             *stubSpiderConn
	ormErr           *errs.Error
	runErr           *errs.Error
}

func TestRunSpiderSkipsStartupWhenCallbackNil(t *testing.T) {
	queries := &stubSpiderQueries{}
	conn := &stubSpiderConn{}
	recorder := &runSpiderTestRecorder{queries: queries, conn: conn}
	resetDataSourcesForTest(t)
	src := newStubRegistrySource("spider_no_callback_source")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}

	err := runSpiderWithDeps("127.0.0.1:0", newSpiderTestDeps(t, recorder))
	if err != nil {
		t.Fatalf("runSpiderWithDeps failed: %v", err)
	}
	if src.subscribeCount != 0 {
		t.Fatalf("expected no third-party activation without startup callback, got %d subscribe calls", src.subscribeCount)
	}
	if queries.purgeCount != 1 {
		t.Fatalf("expected PurgeKlineUn once, got %d", queries.purgeCount)
	}
	if conn.releaseCount != 1 {
		t.Fatalf("expected conn.Release once, got %d", conn.releaseCount)
	}
	if recorder.runForeverCalls != 1 {
		t.Fatalf("expected RunForever once, got %d", recorder.runForeverCalls)
	}
	if recorder.writeQCalls != 1 || len(recorder.writeQWorkers) != 1 || recorder.writeQWorkers[0] != 5 {
		t.Fatalf("expected write queue worker to start once with 5 workers, got calls=%d workers=%v", recorder.writeQCalls, recorder.writeQWorkers)
	}
	if recorder.monitorCalls != 1 || recorder.cronCalls != 1 {
		t.Fatalf("expected monitor and cron to start once, got monitor=%d cron=%d", recorder.monitorCalls, recorder.cronCalls)
	}
}

func TestRunSpiderStartupActivatesSelectedSourcesOnce(t *testing.T) {
	queries := &stubSpiderQueries{}
	conn := &stubSpiderConn{}
	recorder := &runSpiderTestRecorder{queries: queries, conn: conn}
	resetDataSourcesForTest(t)
	alpha := newStubRegistrySource("spider_activation_alpha")
	beta := newStubRegistrySource("spider_activation_beta")
	if err := RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if err := RegisterDataSource(beta); err != nil {
		t.Fatalf("RegisterDataSource beta failed: %v", err)
	}

	startupCalls := 0
	err := runSpiderWithDeps("127.0.0.1:0", newSpiderTestDeps(t, recorder), WithSpiderStartupHook(func(ctx context.Context, spider *LiveSpider) error {
		startupCalls++
		_, err := ActivateDataSources(ctx, []*strat.DataSub{
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101}, TimeFrame: alpha.info.TimeFrame},
			{Source: beta.info.Name, ExSymbol: &orm.ExSymbol{ID: 202}, TimeFrame: beta.info.TimeFrame},
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 303}, TimeFrame: alpha.info.TimeFrame},
		}, stubDataSink{})
		return err
	}))
	if err != nil {
		t.Fatalf("runSpiderWithDeps failed: %v", err)
	}
	if startupCalls != 1 {
		t.Fatalf("expected startup callback once, got %d", startupCalls)
	}
	if alpha.subscribeCount != 1 || beta.subscribeCount != 1 {
		t.Fatalf("expected grouped source activation once per source, got alpha=%d beta=%d", alpha.subscribeCount, beta.subscribeCount)
	}
	if len(alpha.subscribedSubs) != 1 || len(alpha.subscribedSubs[0]) != 2 {
		t.Fatalf("expected alpha activation group of 2 subs, got %+v", alpha.subscribedSubs)
	}
	if len(beta.subscribedSubs) != 1 || len(beta.subscribedSubs[0]) != 1 {
		t.Fatalf("expected beta activation group of 1 sub, got %+v", beta.subscribedSubs)
	}
	if recorder.runForeverCalls != 1 {
		t.Fatalf("expected RunForever once after startup activation, got %d", recorder.runForeverCalls)
	}
}

func TestRunSpiderStartupUsesCoreContext(t *testing.T) {
	queries := &stubSpiderQueries{}
	conn := &stubSpiderConn{}
	recorder := &runSpiderTestRecorder{queries: queries, conn: conn}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	oldCtx := core.Ctx
	core.Ctx = ctx
	t.Cleanup(func() {
		core.Ctx = oldCtx
	})
	var startupCtx context.Context

	err := runSpiderWithDeps("127.0.0.1:0", newSpiderTestDeps(t, recorder), WithSpiderStartupHook(func(ctx context.Context, spider *LiveSpider) error {
		startupCtx = ctx
		return nil
	}))
	if err != nil {
		t.Fatalf("runSpiderWithDeps failed: %v", err)
	}
	if recorder.ormCtx != ctx {
		t.Fatalf("expected orm connection to use core context")
	}
	if startupCtx != ctx {
		t.Fatalf("expected startup hook to use core context")
	}
}

func TestRunSpiderStartupReturnsUnknownSourceError(t *testing.T) {
	queries := &stubSpiderQueries{}
	conn := &stubSpiderConn{}
	recorder := &runSpiderTestRecorder{queries: queries, conn: conn}
	resetDataSourcesForTest(t)

	err := runSpiderWithDeps("127.0.0.1:0", newSpiderTestDeps(t, recorder), WithSpiderStartupHook(func(ctx context.Context, spider *LiveSpider) error {
		_, err := ActivateDataSources(ctx, []*strat.DataSub{{
			Source:    "spider_missing_source",
			ExSymbol:  &orm.ExSymbol{ID: 404},
			TimeFrame: "1d",
		}}, stubDataSink{})
		return err
	}))
	if err == nil {
		t.Fatalf("expected unknown source startup error")
	}
	if recorder.writeQCalls != 0 {
		t.Fatalf("expected startup error to stop before write queue worker, got %d calls", recorder.writeQCalls)
	}
	if recorder.runForeverCalls != 0 {
		t.Fatalf("expected startup error to stop before RunForever, got %d calls", recorder.runForeverCalls)
	}
	if recorder.monitorCalls != 0 || recorder.cronCalls != 0 {
		t.Fatalf("expected startup error to stop before monitor/cron, got monitor=%d cron=%d", recorder.monitorCalls, recorder.cronCalls)
	}
	if conn.releaseCount != 1 {
		t.Fatalf("expected conn.Release once even on startup error, got %d", conn.releaseCount)
	}
}
