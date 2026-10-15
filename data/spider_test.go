package data

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/internal/testutil"
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
	testutil.RequireIntegration(t)
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
		if len(msg.Rows) == 0 {
			return
		}
		code := fmt.Sprintf("%s.%s.%s", msg.ExgName, msg.Market, msg.Pair)
		k, err := msg.Rows[0].OHLCV()
		if err != nil {
			return
		}
		dateStr := btime.ToDateStr(k.Time, core.DefaultDateFmt)
		barStr := fmt.Sprintf("%f %f %f %f %f", k.Open, k.High, k.Low, k.Close, k.Volume)
		log.Info("receive", zap.String("code", code), zap.Int("num", len(msg.Rows)),
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

func TestSeriesWatcherOnSpiderSeriesKeepsDataSeriesRows(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Exchange: "custom", Market: "macro", Symbol: "CPI_US"}
	row := &orm.DataSeries{
		Source:    "macro",
		Sid:       exs.ID,
		TimeMS:    100,
		EndMS:     200,
		TimeFrame: "1d",
		Closed:    true,
		Values:    map[string]any{"value": 3.14},
		ExSymbol:  exs,
	}
	raw, err := utils2.Marshal(NotifySeries{TFSecs: 86400, Interval: 86400, Rows: []*orm.DataSeries{row}})
	if err != nil {
		t.Fatalf("marshal notify series: %v", err)
	}
	w := &SeriesWatcher{jobs: map[string]map[string]*PairTFCache{
		"ohlcv": {"CPI_US": {TimeFrame: "1d", TFSecs: 86400}},
	}}
	var got *SeriesMsg
	w.OnDataMsg = func(msg *SeriesMsg) { got = msg }

	w.onSpiderSeries(&utils.IOMsgRaw{Action: "ohlcv_custom_macro_CPI_US", Data: raw})

	if got == nil || len(got.Rows) != 1 {
		t.Fatalf("expected one data series row, got %+v", got)
	}
	if got.Rows[0].Source != "macro" || got.Rows[0].Values["value"] != 3.14 {
		t.Fatalf("unexpected data series row: %+v", got.Rows[0])
	}
}

type noFetchExchange struct {
	banexg.BanExchange
}

func (noFetchExchange) HasApi(string, string) bool { return false }

type fetchExchange struct {
	banexg.BanExchange
}

func (fetchExchange) HasApi(string, string) bool { return true }

type spiderTradeWatchExchange struct {
	banexg.BanExchange
	trades chan *banexg.Trade
}

type spiderIdentityExchange struct {
	banexg.BanExchange
	name   string
	market string
	jobs   [][2]string
}

func (s *spiderIdentityExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: s.name, MarketType: s.market}
}

func (s *spiderIdentityExchange) UnWatchOHLCVs(jobs [][2]string, _ map[string]interface{}) *errs.Error {
	s.jobs = append(s.jobs, jobs...)
	return nil
}

func (s *spiderTradeWatchExchange) WatchTrades([]string, map[string]interface{}) (chan *banexg.Trade, *errs.Error) {
	return s.trades, nil
}

type legacyFacadePanicExchange struct {
	banexg.BanExchange
}

func (legacyFacadePanicExchange) GetMarket(string) (*banexg.Market, *errs.Error) {
	panic("gap recovery accessed legacy symbol facade")
}

func TestGapRecoverySymbolUsesExplicitStateWithoutLegacyFacade(t *testing.T) {
	oldDefault := exg.Default
	exg.Default = legacyFacadePanicExchange{}
	t.Cleanup(func() { exg.Default = oldDefault })

	const pair = "RATE_US"
	symbols := orm.NewSymbolStateWithIdentity("china", "spot")
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{
		ID: 2, Exchange: "china", Market: "spot", Symbol: pair,
	}}); err != nil {
		t.Fatal(err)
	}

	got, err := gapRecoverySymbol(symbols, &orm.ExSymbol{
		ID: 1, Exchange: "china", Market: "spot", Symbol: pair,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 {
		t.Fatalf("gap recovery symbol ID = %d, want explicit-state ID 2", got.ID)
	}
}

func TestAutoFetchOhlcvUsesSymbolExchange(t *testing.T) {
	oldConfig, oldDefault := config.Exchange, exg.Default
	config.Exchange = &config.ExchangeConfig{Name: "china", Items: map[string]map[string]interface{}{}}
	exg.Default = fetchExchange{}
	t.Cleanup(func() { config.Exchange, exg.Default = oldConfig, oldDefault })

	_, rows, err := autoFetchOhlcv(&orm.ExSymbol{Exchange: "china", Market: "spot", Symbol: "RATE_US"}, "1m", 1_700_000_000_000, 1_700_000_060_000)
	if err != nil || len(rows) != 0 {
		t.Fatalf("autoFetchOhlcv used the wrong exchange: rows=%v err=%v", rows, err)
	}
}

func TestSeriesWatcherGapRecoveryUsesExplicitSymbolState(t *testing.T) {
	const pair = "RATE_US"
	const startMS = int64(1_700_000_040_000)
	oldConfig := config.Exchange
	config.Exchange = &config.ExchangeConfig{Name: "china", Items: map[string]map[string]interface{}{}}
	t.Cleanup(func() { config.Exchange = oldConfig })
	legacyRestore, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "china", Market: "spot", Symbol: pair, AggRules: `{"rate":"sum"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(legacyRestore)
	oldDefault := exg.Default
	exg.Default = noFetchExchange{}
	t.Cleanup(func() { exg.Default = oldDefault })

	symbols := orm.NewSymbolStateWithIdentity("china", "spot")
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{
		ID: 2, Exchange: "china", Market: "spot", Symbol: pair, AggRules: `{"rate":"avg"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	exs := symbols.GetExSymbol2("china", "spot", pair)
	w := &SeriesWatcher{
		symbols: symbols,
		jobs: map[string]map[string]*PairTFCache{
			"ohlcv": {pair: {TimeFrame: "2m", TFSecs: 120, exSymbol: exs, SubNextMS: startMS - 60_000}},
		},
	}
	var got *SeriesMsg
	w.OnDataMsg = func(msg *SeriesMsg) { got = msg }
	rows := []*orm.DataSeries{
		{Source: "rates", Sid: exs.ID, TimeMS: startMS, EndMS: startMS + 60_000, Values: map[string]any{"rate": 1.0}},
		{Source: "rates", Sid: exs.ID, TimeMS: startMS + 60_000, EndMS: startMS + 120_000, Values: map[string]any{"rate": 3.0}},
	}
	raw, marshalErr := utils2.Marshal(NotifySeries{TFSecs: 60, Interval: 60, Rows: rows})
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}

	w.onSpiderSeries(&utils.IOMsgRaw{Action: "ohlcv_china_spot_" + pair, Data: raw})

	if got == nil || len(got.Rows) != 1 {
		t.Fatalf("expected one recovered aggregate, got %+v", got)
	}
	if got.Rows[0].Sid != exs.ID || got.Rows[0].Values["rate"] != 2.0 {
		t.Fatalf("aggregate used wrong symbol catalog: %+v", got.Rows[0])
	}
}

func TestSaveKlines(t *testing.T) {
	testutil.RequireIntegration(t)
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

func TestLiveSpidersKeepQueuesAndShutdownIndependent(t *testing.T) {
	newServer := func() *utils.ServerIO {
		return utils.NewServerIO("", "")
	}
	first := NewLiveSpider(newServer(), nil, nil)
	second := NewLiveSpider(newServer(), nil, nil)
	if first.writeQ == second.writeQ || first.sidMap == nil || second.sidMap == nil {
		t.Fatal("spiders shared mutable queue or pending-series state")
	}
	first.workers.Add(1)
	go first.consumeSeriesWriteQ(1)
	second.workers.Add(1)
	go second.consumeSeriesWriteQ(1)
	first.Stop()
	first.Join()
	select {
	case <-second.ctx.Done():
		t.Fatal("stopping first spider cancelled second spider")
	default:
	}
	second.Stop()
	second.Join()
}

type fakeSpiderStorage struct {
	prepareCalls int
	err          *errs.Error
}

func (s *fakeSpiderStorage) Prepare(context.Context, *RuntimeDeps) *errs.Error {
	s.prepareCalls++
	return s.err
}

func testSpiderDeps() *RuntimeDeps {
	return &RuntimeDeps{
		Core:    &core.State{},
		Clock:   btime.NewClockState(true, nil),
		Config:  config.NewSnapshot(&config.Config{}),
		Symbols: orm.NewSymbolState(),
		Storage: orm.NewStorage(nil, true, "spider:test"),
	}
}

func TestPrepareLiveSpiderSkipsActivationWithoutStartup(t *testing.T) {
	resetDataSourcesForTest(t)
	source := newStubRegistrySource("spider_no_callback_source")
	if err := RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	storage := &fakeSpiderStorage{}
	spider, err := prepareLiveSpider(context.Background(), &utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}},
		testSpiderDeps(), nil, nil, storage)
	if err != nil {
		t.Fatal(err)
	}
	spider.Stop()
	spider.Join()
	if storage.prepareCalls != 1 || source.subscribeCount != 0 {
		t.Fatalf("prepare=%d subscriptions=%d, want one storage preparation and no activation", storage.prepareCalls, source.subscribeCount)
	}
}

func TestPrepareLiveSpiderActivatesSelectedSourcesOnce(t *testing.T) {
	resetDataSourcesForTest(t)
	alpha := newStubRegistrySource("spider_activation_alpha")
	beta := newStubRegistrySource("spider_activation_beta")
	for _, source := range []*stubSeriesSource{alpha, beta} {
		if err := RegisterDataSource(source); err != nil {
			t.Fatal(err)
		}
	}
	storage := &fakeSpiderStorage{}
	spider, err := prepareLiveSpider(context.Background(), &utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}},
		testSpiderDeps(), nil, func(ctx context.Context, _ *LiveSpider) error {
			_, err := ActivateDataSources(ctx, []*strat.DataSub{
				{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101}, TimeFrame: alpha.info.TimeFrame},
				{Source: beta.info.Name, ExSymbol: &orm.ExSymbol{ID: 202}, TimeFrame: beta.info.TimeFrame},
				{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 303}, TimeFrame: alpha.info.TimeFrame},
			}, stubDataSink{})
			return err
		}, storage)
	if err != nil {
		t.Fatal(err)
	}
	spider.Stop()
	spider.Join()
	if storage.prepareCalls != 1 || alpha.subscribeCount != 1 || beta.subscribeCount != 1 ||
		len(alpha.subscribedSubs) != 1 || len(alpha.subscribedSubs[0]) != 2 ||
		len(beta.subscribedSubs) != 1 || len(beta.subscribedSubs[0]) != 1 {
		t.Fatalf("unexpected typed startup activation: prepare=%d alpha=%+v beta=%+v", storage.prepareCalls, alpha.subscribedSubs, beta.subscribedSubs)
	}
}

func TestLiveSpiderQueuePreservesCompleteDataSeries(t *testing.T) {
	spider := NewLiveSpider(&utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}, nil,
		nil)
	row := &orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 100, EndMS: 200, TimeFrame: "1d", Closed: true,
		Values: map[string]any{"value": 3.14}, ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "CPI_US"}}
	job := &SaveSeries{Sid: row.Sid, TimeFrame: row.TimeFrame, Rows: []*orm.DataSeries{row}, MsgAction: "ohlcv_macro"}
	spider.writeQ <- job
	got := <-spider.writeQ
	if got.Rows[0] != row || got.Rows[0].Values["value"] != 3.14 || got.Rows[0].ExSymbol != row.ExSymbol {
		t.Fatalf("spider queue changed data series fields: %#v", got.Rows[0])
	}
}

func TestLiveSpiderStopJoinsActualWatchAndPersistenceWorkers(t *testing.T) {
	server := &utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}
	spider := NewLiveSpider(server, nil, nil)
	spider.workers.Add(1)
	go spider.consumeSeriesWriteQ(1)
	miner := &Miner{
		spider: spider, ExgName: "test", Market: banexg.MarketSpot,
		exchange: &spiderTradeWatchExchange{trades: make(chan *banexg.Trade)}, Trades: NewPairSubs(),
		retryWaits: btime.NewRetryWaits(0, nil),
	}
	miner.watchTrades([]string{"BTC/USDT"})
	done := make(chan struct{})
	go func() {
		spider.Stop()
		spider.Join()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("spider Stop/Join did not wait for blocked watch and persistence workers")
	}
}

func TestLiveSpiderJoinDefersMinerCleanupUntilWorkersExit(t *testing.T) {
	release := make(chan struct{})
	cleaned := make(chan struct{}, 2)
	spider := NewLiveSpider(&utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}, nil, nil)
	spider.miners["test:spot"] = &Miner{cleanup: func() { cleaned <- struct{}{} }}
	spider.workers.Add(1)
	go func() {
		defer spider.workers.Done()
		<-release
	}()

	spider.Stop()
	select {
	case <-cleaned:
		t.Fatal("Stop cleaned miner runtime before worker exit")
	default:
	}
	joined := make(chan struct{})
	go func() {
		spider.Join()
		close(joined)
	}()
	select {
	case <-cleaned:
		t.Fatal("Join cleaned miner runtime before worker exit")
	case <-joined:
		t.Fatal("Join returned before worker exit")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not finish after worker exit")
	}
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("Join did not clean miner runtime")
	}
	spider.Join()
	select {
	case <-cleaned:
		t.Fatal("miner runtime cleanup was not idempotent")
	default:
	}
}

func TestLiveSpiderStopRejectsRacingMinerCreation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cleaned := make(chan struct{}, 1)
	spider := NewLiveSpider(&utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}, nil,
		func(context.Context, string, string) (*RuntimeDeps, func(), *errs.Error) {
			close(started)
			<-release
			return nil, func() { cleaned <- struct{}{} }, nil
		})
	created := make(chan *Miner, 1)
	go func() { created <- spider.getMiner("test", banexg.MarketSpot) }()
	<-started
	joined := make(chan struct{})
	go func() {
		spider.Stop()
		spider.Join()
		close(joined)
	}()
	close(release)
	if miner := <-created; miner != nil {
		t.Fatalf("miner created after Spider.Stop: %#v", miner)
	}
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("racing miner runtime cleanup was not called")
	}
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Spider Stop/Join blocked on racing miner creation")
	}
}

func TestPairSubsConcurrentSubscribeClaimsPairsOnce(t *testing.T) {
	subs := NewPairSubs()
	const workers = 32
	var wait sync.WaitGroup
	results := make(chan []string, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- subs.GetNewSubs([]string{"BTC/USDT", "ETH/USDT"})
		}()
	}
	wait.Wait()
	close(results)
	claimed := make(map[string]int)
	for pairs := range results {
		for _, pair := range pairs {
			claimed[pair]++
		}
	}
	if claimed["BTC/USDT"] != 1 || claimed["ETH/USDT"] != 1 || subs.Len() != 2 {
		t.Fatalf("concurrent subscription claims = %#v, size=%d", claimed, subs.Len())
	}
}

func TestNewMinerRejectsMismatchedRuntimeIdentityAndCleansUp(t *testing.T) {
	cleaned := false
	spider := NewLiveSpider(&utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}, testSpiderDeps(),
		func(context.Context, string, string) (*RuntimeDeps, func(), *errs.Error) {
			deps := testSpiderDeps()
			deps.Exchange = &spiderIdentityExchange{name: "other", market: banexg.MarketSpot}
			deps.ExchangeName, deps.MarketType = "other", banexg.MarketSpot
			return deps, func() { cleaned = true }, nil
		})
	if miner, err := newMiner(spider, "expected", banexg.MarketSpot); err == nil || miner != nil {
		t.Fatalf("newMiner accepted mismatched runtime identity: miner=%#v err=%v", miner, err)
	}
	if !cleaned {
		t.Fatal("newMiner did not clean up rejected runtime")
	}
}

func TestNewMinerKeepsRetryBackoffPerMiner(t *testing.T) {
	spider := NewLiveSpider(&utils.ServerIO{Data: map[string]string{}, DataExp: map[string]int64{}}, testSpiderDeps(),
		func(_ context.Context, name, market string) (*RuntimeDeps, func(), *errs.Error) {
			deps := testSpiderDeps()
			deps.Exchange = &spiderIdentityExchange{name: name, market: market}
			deps.ExchangeName, deps.MarketType = name, market
			return deps, nil, nil
		})
	first, err := newMiner(spider, "first", banexg.MarketSpot)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newMiner(spider, "second", banexg.MarketSpot)
	if err != nil {
		t.Fatal(err)
	}
	if first.retryWaits == second.retryWaits {
		t.Fatal("miners share retry backoff state")
	}
}

func TestMinerUnSubPairsBuildsOnlyRealOHLCVJobs(t *testing.T) {
	exchange := &spiderIdentityExchange{}
	miner := &Miner{exchange: exchange, KLines: NewPairSubs()}
	miner.KLines.Set("BTC/USDT", "ETH/USDT")
	if err := miner.UnSubPairs(core.WsSubKLine, "BTC/USDT", "ETH/USDT"); err != nil {
		t.Fatal(err)
	}
	if len(exchange.jobs) != 2 || exchange.jobs[0][0] == "" || exchange.jobs[1][0] == "" {
		t.Fatalf("unexpected OHLCV unsubscribe jobs: %#v", exchange.jobs)
	}
}
