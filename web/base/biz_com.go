package base

import (
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

var (
	exgInits    = map[banexg.BanExchange]bool{}
	exgInitLock deadlock.Mutex
	receiver    *data.SeriesWatcher
	legacyWsHub = NewWsHub(nil)
)

func InitExg(exchange banexg.BanExchange) *errs.Error {
	exgInitLock.Lock()
	defer exgInitLock.Unlock()
	if _, ok := exgInits[exchange]; ok {
		return nil
	}
	err := orm.InitExg(exchange)
	if err != nil {
		return err
	}
	exgInits[exchange] = true
	return nil
}

func GetExg(name, market, ctType string, load bool) (banexg.BanExchange, *errs.Error) {
	exchange, err := exg.GetWith(name, market, ctType)
	if err != nil {
		return nil, err
	}
	if !load {
		return exchange, nil
	}
	return exchange, InitExg(exchange)
}

func ArrSeriesRows(rows []*orm.DataSeries) ([][]float64, *errs.Error) {
	res := make([][]float64, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			return nil, errs.NewMsg(core.ErrInvalidBars, "series row is nil")
		}
		open, err := row.OpenValue()
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		high, err := row.HighValue()
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		low, err := row.LowValue()
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		closeVal, err := row.CloseValue()
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		volume, err := row.VolumeValue()
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		res = append(res, []float64{float64(row.TimeMS), open, high, low, closeVal, volume, row.BuyVolumeValue()})
	}
	return res, nil
}

func RunReceiver() {
	var err *errs.Error
	receiver, err = data.NewSeriesWatcher(config.SpiderAddr)
	if err != nil {
		log.Warn("connect spider fail", zap.String("addr", config.SpiderAddr), zap.String("err", err.Short()))
		return
	}
	receiver.OnDataMsg = seriesHandler
	// 暂时只监听默认交易所的默认市场
	exsList := orm.GetExSymbols(core.ExgName, core.Market)
	if len(exsList) == 0 {
		return
	}
	jobs := make([]data.WatchJob, 0, len(exsList))
	timeFrame := "1m"
	for _, exs := range exsList {
		jobs = append(jobs, data.WatchJob{Symbol: exs.Symbol, TimeFrame: timeFrame})
	}
	err = receiver.WatchJobs(core.ExgName, core.Market, core.WsSubKLine, jobs...)
	if err != nil {
		log.Error("subscribe spider fail", zap.Int("num", len(jobs)), zap.Error(err))
	}
	log.Info("subscribe series from spider success")
	err = receiver.RunForever()
	if err != nil {
		log.Error("receive spider fail", zap.Error(err))
	}
}

func seriesHandler(msg *data.SeriesMsg) { legacyWsHub.Publish(msg) }
