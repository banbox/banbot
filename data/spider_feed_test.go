package data

import (
	"context"
	"github.com/banbox/banbot/orm"
	"testing"
	"time"
)

func TestSpiderClosedSeriesObserverPreservesUserValuesAndFilters(t *testing.T) {
	spider := &LiveSpider{ctx: context.Background()}
	var facts []ClosedSeriesFact
	spider.SetClosedSeriesObserver(func(_ context.Context, fact ClosedSeriesFact) error { facts = append(facts, fact); return nil })
	values := map[string]any{"optional": nil, "custom": int64(7)}
	rows := []*orm.DataSeries{
		nil,
		{Sid: 1, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000, Closed: false, Values: values},
		{Sid: 1, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000, Closed: true, IsWarmUp: true, Values: values},
		{Sid: 1, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000, Closed: true, Values: values},
	}
	if err := spider.observeClosedSeries(rows); err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].EndMS != 120000 {
		t.Fatal("incorrect public clock facts", facts)
	}
	if rows[3].Values["optional"] != nil || rows[3].Values["custom"] != int64(7) {
		t.Fatal("observer narrowed arbitrary series values")
	}
}

func TestSpiderExecutionPinSurvivesRobotUnwatch(t *testing.T) {
	spider := &LiveSpider{pinnedFeeds: map[string]bool{"test\x00linear\x00BTC": true}}
	miner := &Miner{spider: spider, ExgName: "test", Market: "linear", KLineApis: NewPairSubs(), klineStates: map[string]*KLineState{"BTC": {Sid: 1}, "ETH": {Sid: 2}}}
	miner.KLineApis.Set("BTC", "ETH")
	if err := miner.UnSubPairs("ohlcv", "BTC", "ETH"); err != nil {
		t.Fatal(err)
	}
	if miner.KLineApis.Len() != 1 || miner.klineStates["BTC"] == nil || miner.klineStates["ETH"] != nil {
		t.Fatal("robot removed account-owned feed subscription")
	}
}

func TestSpiderPinnedFeedRejectsMismatchedSIDBeforeSubscription(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("test", "linear")
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{ID: 17, Exchange: "test", Market: "linear", Symbol: "BTC"}, {ID: 18, Exchange: "test", Market: "linear", Symbol: "ETH"}}); err != nil {
		t.Fatal(err)
	}
	spider := &LiveSpider{ctx: context.Background(), miners: map[string]*Miner{}}
	spider.miners["test:linear"] = &Miner{deps: &RuntimeDeps{Symbols: symbols}}
	if err := spider.PinClosedFeed("test", "linear", "ETH", 17); err == nil {
		t.Fatal("BTC SID was accepted as the clock for ETH")
	}
	if len(spider.pinnedFeeds) != 0 {
		t.Fatal("invalid SID created an execution subscription")
	}
}

func TestSpiderClosedFactsWaitForEarlierSymbolPersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstSaved := make(chan struct{})
	secondStarted := make(chan struct{})
	facts := make(chan int, 2)
	go func() {
		close(secondStarted)
		if waitSeriesPredecessor(ctx, firstSaved) {
			facts <- 2
		}
	}()
	<-secondStarted
	select {
	case <-facts:
		t.Fatal("later batch overtook pending persistence for its SID")
	case <-time.After(20 * time.Millisecond):
	}
	facts <- 1
	close(firstSaved)
	for _, want := range []int{1, 2} {
		select {
		case got := <-facts:
			if got != want {
				t.Fatal("closed facts reordered", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("ordered fact delivery stalled")
		}
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	if waitSeriesPredecessor(stopped, make(chan struct{})) {
		t.Fatal("shutdown ignored blocked predecessor")
	}
}

func TestSpiderQueuedSymbolDoesNotOccupyOtherSymbolsWriteSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guard := make(chan struct{}, 2)
	firstDone := make(chan struct{})
	if !acquireSeriesWriteSlot(ctx, nil, guard) {
		t.Fatal("first symbol failed to acquire slot")
	}
	secondAcquired := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		if acquireSeriesWriteSlot(ctx, firstDone, guard) {
			close(secondAcquired)
			<-guard
		}
	}()
	otherSaved := make(chan struct{})
	go func() {
		if acquireSeriesWriteSlot(ctx, nil, guard) {
			close(otherSaved)
			<-guard
		}
	}()
	select {
	case <-otherSaved:
	case <-time.After(time.Second):
		t.Fatal("queued A2 occupied the slot needed for B while A1 was blocked")
	}
	select {
	case <-secondAcquired:
		t.Fatal("A2 overtook A1")
	default:
	}
	<-guard
	close(firstDone)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("A2 did not resume after A1 finished")
	}
}
