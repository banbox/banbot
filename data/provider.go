package data

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/zap"
	"math"
	"slices"
	"strings"
)

var (
	Main IProvider
)

type IProvider interface {
	LoopMain() *errs.Error
	SubWarmPairs(items map[string]map[string]int) *errs.Error
	UnSubPairs(pairs ...string) *errs.Error
}

type Provider[T IKlineFeeder] struct {
	holders   map[string]T
	newFeeder func(pair string, tfs []string) (T, *errs.Error)
}

func (p *Provider[IKlineFeeder]) UnSubPairs(pairs ...string) []string {
	var removed []string
	for _, pair := range pairs {
		if _, ok := p.holders[pair]; ok {
			delete(p.holders, pair)
			removed = append(removed, pair)
		}
	}
	return removed
}

type WarmJob struct {
	hold    IKlineFeeder
	tfWarms map[string]int
}

/*
SubWarmPairs
从数据提供者添加新的交易对订阅。

	items: pair[timeFrame]warmNum
	返回最小周期变化的交易对(新增/旧对新周期)、预热任务
*/
func (p *Provider[IKlineFeeder]) SubWarmPairs(items map[string]map[string]int) ([]IKlineFeeder, map[string]int64, *errs.Error) {
	var newHolds []IKlineFeeder
	var warmJobs []*WarmJob
	var err *errs.Error
	for pair, tfWarms := range items {
		hold, ok := p.holders[pair]
		if !ok {
			hold, err = p.newFeeder(pair, utils.KeysOfMap(tfWarms))
			if err != nil {
				return nil, nil, err
			}
			p.holders[pair] = hold
			newHolds = append(newHolds, hold)
			warmJobs = append(warmJobs, &WarmJob{hold: hold, tfWarms: tfWarms})
		} else {
			oldMinTf := hold.getStates()[0].TimeFrame
			newTfs := hold.subTfs(utils.KeysOfMap(tfWarms)...)
			curMinTf := hold.getStates()[0].TimeFrame
			if oldMinTf != curMinTf {
				newHolds = append(newHolds, hold)
			}
			if len(newTfs) > 0 {
				warmJobs = append(warmJobs, &WarmJob{
					hold:    hold,
					tfWarms: utils.CutMap(tfWarms, newTfs...),
				})
			}
		}
	}
	// 加载数据预热
	sinceMap := make(map[string]int64)
	exchange, err := exg.Get()
	if err != nil {
		return newHolds, sinceMap, err
	}
	curTimeMS := btime.TimeMS()
	log.Info(fmt.Sprintf("warmup for %d pairs", len(warmJobs)))
	for _, job := range warmJobs {
		symbol := job.hold.getSymbol()
		exs, err := orm.GetExSymbol(exchange, symbol)
		if err != nil {
			return newHolds, sinceMap, err
		}
		for tf, warmNum := range job.tfWarms {
			tfMSecs := int64(utils.TFToSecs(tf) * 1000)
			if tfMSecs < int64(60000) {
				continue
			}
			endMS := utils.AlignTfMSecs(curTimeMS, tfMSecs)
			startMS := endMS - tfMSecs*int64(warmNum)
			bars, err := orm.AutoFetchOHLCV(exchange, exs, tf, startMS, endMS, 0, false)
			if err != nil {
				return newHolds, sinceMap, err
			}
			key := fmt.Sprintf("%s|%s", symbol, tf)
			sinceMap[key] = job.hold.WarmTfs(map[string][]*banexg.Kline{tf: bars})
		}
	}
	return newHolds, sinceMap, err
}

type HistProvider[T IHistKlineFeeder] struct {
	Provider[T]
}

func NewHistProvider(callBack FnPairKline) *HistProvider[IHistKlineFeeder] {
	return &HistProvider[IHistKlineFeeder]{
		Provider: Provider[IHistKlineFeeder]{
			holders: make(map[string]IHistKlineFeeder),
			newFeeder: func(pair string, tfs []string) (IHistKlineFeeder, *errs.Error) {
				feeder, err := NewDBKlineFeeder(pair, callBack)
				if err != nil {
					return nil, err
				}
				feeder.subTfs(tfs...)
				return feeder, nil
			},
		},
	}
}

func (p *HistProvider[IHistKlineFeeder]) SubWarmPairs(items map[string]map[string]int) *errs.Error {
	_, sinceMap, err := p.Provider.SubWarmPairs(items)
	pairSince := make(map[string]int64)
	for key, val := range sinceMap {
		pair := strings.Split(key, "|")[0]
		if oldVal, ok := pairSince[pair]; ok && oldVal > 0 {
			pairSince[pair] = min(oldVal, val)
		} else {
			pairSince[pair] = val
		}
	}
	for pair, since := range pairSince {
		hold, ok := p.holders[pair]
		if !ok {
			continue
		}
		hold.initNext(since)
	}
	return err
}

func (p *HistProvider[IHistKlineFeeder]) UnSubPairs(pairs ...string) *errs.Error {
	_ = p.Provider.UnSubPairs(pairs...)
	return nil
}

func (p *HistProvider[IHistKlineFeeder]) LoopMain() *errs.Error {
	if len(p.holders) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no pairs to run")
	}
	var err *errs.Error
	exchange, err := exg.Get()
	if err != nil {
		return err
	}
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	for _, h := range p.holders {
		err = h.downIfNeed(sess, exchange)
		if err != nil {
			log.Error("download ohlcv fail", zap.String("pair", h.getSymbol()), zap.Error(err))
			conn.Release()
			return err
		}
	}
	conn.Release()
	var pBar *progressbar.ProgressBar
	log.Info("run data loop for backtest..")
	for {
		holds := utils.ValsOfMap(p.holders)
		slices.SortFunc(holds, func(a, b IHistKlineFeeder) int {
			va, vb := a.getNextMS(), b.getNextMS()
			if va == math.MaxInt64 || vb == math.MaxInt64 {
				if va == vb {
					return 0
				}
				if va == math.MaxInt64 {
					return int(math.MaxInt32 - int32(vb/1000))
				} else {
					return int(int32(va/1000) - math.MaxInt32)
				}
			}
			return int((va - vb) / 1000)
		})
		hold := holds[0]
		if hold.getNextMS() > btime.TimeMS() {
			break
		}
		// 触发回调
		count, err := hold.invoke()
		if err != nil {
			if err.Code == core.ErrEOF {
				break
			}
			log.Error("data loop main fail", zap.Error(err))
			return err
		}
		// 更新进度条
		if pBar == nil {
			var sumTotal int
			for _, h := range holds {
				sumTotal += h.getTotalLen()
			}
			pBar = progressbar.Default(int64(sumTotal))
		}
		err_ := pBar.Add(count)
		if err_ != nil {
			log.Error("procBar add fail", zap.Error(err_))
			return errs.New(core.ErrRunTime, err_)
		}
	}
	if pBar != nil {
		err_ := pBar.Close()
		if err_ != nil {
			log.Error("procBar close fail", zap.Error(err_))
			return errs.New(core.ErrRunTime, err_)
		}
	}
	return nil
}

type LiveProvider[T IKlineFeeder] struct {
	Provider[T]
	KLineWatcher
}

func NewLiveProvider(callBack FnPairKline) (*LiveProvider[IKlineFeeder], *errs.Error) {
	watcher, err := NewKlineWatcher(config.SpiderAddr)
	if err != nil {
		return nil, err
	}
	provider := &LiveProvider[IKlineFeeder]{
		Provider: Provider[IKlineFeeder]{
			holders: make(map[string]IKlineFeeder),
			newFeeder: func(pair string, tfs []string) (IKlineFeeder, *errs.Error) {
				feeder := NewKlineFeeder(pair, callBack)
				return feeder, nil
			},
		},
		KLineWatcher: *watcher,
	}
	watcher.OnKLineMsg = makeOnKlineMsg(provider)
	return provider, nil
}

func (p *LiveProvider[IKlineFeeder]) SubWarmPairs(items map[string]map[string]int) *errs.Error {
	newHolds, sinceMap, err := p.Provider.SubWarmPairs(items)
	if err != nil {
		return err
	}
	if len(newHolds) > 0 {
		var jobs []WatchJob
		for _, h := range newHolds {
			symbol, timeFrame := h.getSymbol(), h.getStates()[0].TimeFrame
			key := fmt.Sprintf("%s|%s", symbol, timeFrame)
			if since, ok := sinceMap[key]; ok {
				jobs = append(jobs, WatchJob{
					Symbol:    symbol,
					TimeFrame: timeFrame,
					Since:     since,
				})
			}
		}
		err := p.WatchJobs(core.ExgName, core.Market, "ohlcv", jobs...)
		if err != nil {
			return err
		}
		if len(core.BookPairs) > 0 {
			jobs = make([]WatchJob, 0, len(core.BookPairs))
			for p := range core.BookPairs {
				jobs = append(jobs, WatchJob{Symbol: p, TimeFrame: "1m"})
			}
			err = p.WatchJobs(core.ExgName, core.Market, "book", jobs...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *LiveProvider[IKlineFeeder]) UnSubPairs(pairs ...string) *errs.Error {
	removed := p.Provider.UnSubPairs(pairs...)
	if len(removed) == 0 {
		return nil
	}
	return p.UnWatchJobs(core.ExgName, core.Market, "ohlcv", pairs)
}

func (p *LiveProvider[IKlineFeeder]) LoopMain() *errs.Error {
	p.RunForever()
	return nil
}

func makeOnKlineMsg(p *LiveProvider[IKlineFeeder]) func(msg *KLineMsg) {
	return func(msg *KLineMsg) {
		if msg.ExgName != core.ExgName || msg.Market != core.Market {
			return
		}
		hold, ok := p.holders[msg.Pair]
		if !ok {
			return
		}
		tfMSecs := int64(msg.TFSecs * 1000)
		if msg.Interval >= msg.TFSecs {
			_, err := hold.onNewBars(tfMSecs, msg.Arr)
			if err != nil {
				log.Error("onNewBars fail", zap.String("p", msg.Pair), zap.Error(err))
			}
			return
		}
		// 更新频率低于bar周期，收到的可能未完成
		lastIdx := len(msg.Arr) - 1
		doneArr, lastBar := msg.Arr[:lastIdx], msg.Arr[lastIdx]
		waitBar := hold.getWaitBar()
		if waitBar != nil && waitBar.Time < lastBar.Time {
			doneArr = append([]*banexg.Kline{waitBar}, doneArr...)
			hold.setWaitBar(nil)
		}
		if len(doneArr) > 0 {
			_, err := hold.onNewBars(tfMSecs, doneArr)
			if err != nil {
				log.Error("onNewBars fail", zap.String("p", msg.Pair), zap.Error(err))
			}
			return
		}
		if msg.Interval <= 5 && hold.getStates()[0].TFSecs >= 60 {
			// 更新很快，需要的周期相对较长，则要求出现下一个bar时认为完成（走上面逻辑）
			hold.setWaitBar(lastBar)
			return
		}
		// 更新频率相对不高，或占需要的周期比率较大，近似完成认为完成
		endLackSecs := int((lastBar.Time + tfMSecs - btime.TimeMS()) / 1000)
		if endLackSecs*2 < msg.Interval {
			// 缺少的时间不足更新间隔的一半，认为完成。
			_, err := hold.onNewBars(tfMSecs, []*banexg.Kline{lastBar})
			if err != nil {
				log.Error("onNewBars fail", zap.String("p", msg.Pair), zap.Error(err))
			}
		} else {
			hold.setWaitBar(lastBar)
		}
	}
}