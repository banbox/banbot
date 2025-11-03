package data

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

// TradeFeeder feeds trade data from files for backtesting
type TradeFeeder struct {
	*orm.ExSymbol

	cache     []*banexg.Trade
	index     int
	indexNext int
	loader    *WsDataLoader
	nextCache []*banexg.Trade
	waitNext  chan int

	// Time management
	startMS  int64
	nextMS   int64
	endMS    int64
	offsetMS int64

	timeStart int64
	num       int
}

// NewTradeFeeder creates a new trade data feeder
func NewTradeFeeder(exs *orm.ExSymbol, l *WsDataLoader) *TradeFeeder {
	return &TradeFeeder{
		ExSymbol: exs,
		cache:    make([]*banexg.Trade, 0),
		loader:   l,
		endMS:    config.TimeRange.EndMS,
	}
}

func (f *TradeFeeder) getSymbol() string {
	return f.Symbol
}

func (f *TradeFeeder) getNextMS() int64 {
	return f.nextMS
}

func (f *TradeFeeder) SetSeek(since int64) {
	f.startMS = since
	f.nextMS = since
	hourMS := int64(utils2.TFToSecs("1h") * 1000)
	f.offsetMS = utils2.AlignTfMSecs(since, hourMS)
	f.cache = nil
	f.nextCache = nil
	f.waitNext = nil
	f.index = 0
	f.indexNext = 0
	f.CallNext()
}

func (f *TradeFeeder) SetEndMS(ms int64) {
	f.endMS = ms
}

func (f *TradeFeeder) Type() string {
	return core.WsSubTrade
}

func (f *TradeFeeder) setBatchEnd() {
	if f.index >= f.indexNext {
		var end = f.index
		for end < len(f.cache) && f.cache[end].Timestamp == f.nextMS {
			end += 1
		}
		f.indexNext = end
	}
}

func (f *TradeFeeder) GetBatch() Batch {
	// Check if we've reached the end
	if f.nextMS >= f.endMS {
		return nil
	}

	f.setBatchEnd()
	batch := f.cache[f.index:f.indexNext]
	if len(batch) == 0 {
		return nil
	}

	return TradeBatch(batch)
}

func (f *TradeFeeder) RunBatch(batch Batch) *errs.Error {
	if trades, ok := batch.(TradeBatch); ok {
		if len(trades) == 0 {
			return nil
		}
		pairMap, _ := strat.WsSubJobs[core.WsSubTrade]
		if len(pairMap) == 0 || len(trades) == 0 {
			return nil
		}
		pair := trades[0].Symbol
		jobMap, _ := pairMap[pair]
		for job := range jobMap {
			job.Strat.OnWsTrades(job, pair, trades)
		}
	} else {
		return errs.NewMsg(errs.CodeRunTime, "type error: %T", batch)
	}
	return nil
}

func (f *TradeFeeder) CallNext() {
	if f.nextMS >= f.endMS {
		return
	}
	if f.indexNext <= f.index {
		f.setBatchEnd()
	}
	f.index = f.indexNext
	f.num += 1
	if f.index < len(f.cache) {
		f.nextMS = f.cache[f.index].Timestamp
		return
	}
	// 缓存读取完毕，加载下一批
	if f.timeStart > 0 {
		cost := btime.UTCStamp() - f.timeStart
		runNum := len(f.cache)
		log.Debug("run trade done", zap.Int64("costMS", cost), zap.Int("num", runNum), zap.Int("batch", f.num))
	}
	f.timeStart = btime.UTCStamp()
	if f.waitNext != nil {
		// 等待nextCache加载完成
		<-f.waitNext
		cost := btime.UTCStamp() - f.timeStart
		log.Debug("wait next ws batch", zap.Int64("cost", cost))
		f.waitNext = nil
	}
	if len(f.nextCache) == 0 {
		f.loadNextBatch()
	}
	f.num = 0
	f.cache = f.nextCache
	f.nextCache = nil
	f.index = 0
	f.indexNext = 0
	if len(f.cache) == 0 {
		f.nextMS = f.endMS
	} else {
		f.nextMS = f.cache[0].Timestamp
	}
	f.waitNext = make(chan int)
	go f.loadNextBatch()
}

func (f *TradeFeeder) loadNextBatch() {
	var err *errs.Error
	hourMS := int64(3600000)
	f.offsetMS += hourMS
	if f.offsetMS >= f.endMS {
		f.nextMS = f.endMS
		return
	}
	info := &WsSymbol{
		ExgId:  f.Exchange,
		Market: f.Market,
		WsType: core.WsSubTrade,
		Symbol: f.Symbol,
		Date:   btime.ToDateStr(f.offsetMS, core.DateFmt),
		Hour:   int(f.offsetMS / hourMS % 24),
	}
	f.nextCache, err = f.loader.LoadTrades(info)
	if err != nil {
		if err.Code == errs.CodeDataNotFound {
			log.Warn("no ws data", zap.String("item", info.String()), zap.String("err", err.Short()))
		} else {
			log.Error("load ws trades fail", zap.String("item", info.String()), zap.Error(err))
		}
	}
	if len(f.nextCache) == 0 {
		f.nextMS = f.endMS
	}
	if f.waitNext != nil {
		f.waitNext <- 1
	}
}
