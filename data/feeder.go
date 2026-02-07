package data

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

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
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type FnPairKline = func(bar *orm.InfoKline)
type FuncEnvEnd = func(bar *banexg.PairTFKline, adj *orm.AdjInfo)
type FnGetInt64 = func() int64

type PairTFCache struct {
	TimeFrame  string
	TFSecs     int
	SubNextMS  int64         // Record the start timestamp of the next bar expected to be received. If it is inconsistent, the bar is missing and needs to be queried and updated. 记录子周期K线下一个期待收到的bar起始时间戳，如果不一致，则出现了bar缺失，需查询更新。
	NextMS     int64         // 当前周期下一个K线期望的时间戳
	WaitBar    *banexg.Kline // Record unfinished bars. Should be set to nil when completed 记录尚未完成的bar。已完成时应置为nil
	Latest     *banexg.Kline // Record the latest bar data, which may not be completed or may be completed 记录最新bar数据，可能未完成，可能已完成
	AlignOffMS int64
}

/*
Feeder
Each Feeder corresponds to a trading pair. Can contain multiple time dimensions.

Supports dynamic addition of time dimension.
Backtest mode: Call execution callbacks in sequence according to the next update time of the Feeder.
Real mode: Subscribe to new data for this trading pair's time period and execute a callback when it is awakened.
Support warm-up data. Each strategy + trading pair is preheated independently throughout the entire process, and cross-preheating is not allowed to avoid btime contamination.
LiveFeeder requires preheating for both new trading pairs and new cycles; HistFeeder only requires preheating for new cycles.
每个Feeder对应一个交易对。可包含多个时间维度。

	支持动态添加时间维度。
	回测模式：根据Feeder的下次更新时间，按顺序调用执行回调。
	实盘模式：订阅此交易对时间周期的新数据，被唤起时执行回调。
	支持预热数据。每个策略+交易对全程单独预热，不可交叉预热，避免btime被污染。
	LiveFeeder新交易对和新周期都需要预热；HistFeeder仅新周期需要预热
*/
type Feeder struct {
	*orm.ExSymbol
	States   []*PairTFCache
	hour     *TfKlineLoader
	WaitBar  *banexg.Kline
	CallBack FnPairKline
	OnEnvEnd FuncEnvEnd                 // If the futures main force switches or the stock is ex-rights, the position needs to be closed first 期货主力切换或股票除权，需先平仓
	tfBars   map[string][]*banexg.Kline // Cache the original K-line of each cycle (not restored) 缓存各周期的原始K线（未复权）
	adjs     []*orm.AdjInfo             // List of weighting factors 复权因子列表
	adj      *orm.AdjInfo
	isWarmUp bool // Is it currently in preheating state? 当前是否预热状态
}

func (f *Feeder) getStates() []*PairTFCache {
	return f.States
}

func (f *Feeder) getSymbol() string {
	return f.Symbol
}

func (f *Feeder) getWaitBar() *banexg.Kline {
	return f.WaitBar
}

func (f *Feeder) setWaitBar(bar *banexg.Kline) {
	f.WaitBar = bar
}

/*
SubTfs
Add monitoring to States and return the newly added TimeFrames
添加监听到States中，返回新增的TimeFrames
*/
func (f *Feeder) SubTfs(timeFrames []string, delOther bool) []string {
	var oldTfs = make(map[string]bool)
	var stateMap = make(map[string]*PairTFCache)
	var minTfSecs = 0 // 记录最小时间周期
	if len(f.States) > 0 {
		for _, sta := range f.States {
			oldTfs[sta.TimeFrame] = true
			stateMap[sta.TimeFrame] = sta
			if minTfSecs == 0 || sta.TFSecs < minTfSecs {
				minTfSecs = sta.TFSecs
			}
		}
	}
	// New records are added to adds, existing ones are deleted from oldTfs, and stateMap retains all
	// 新增的记录到adds中，已有的从oldTfs中删除，stateMap保留全部的
	exchange, err := exg.GetWith(f.Exchange, f.Market, "")
	if err != nil {
		log.Warn("get exchange fail", zap.String("ex", f.Exchange), zap.Error(err))
		return nil
	}
	exgID := exchange.Info().ID
	adds := make([]string, 0, len(timeFrames))
	for _, tf := range timeFrames {
		if _, ok := oldTfs[tf]; ok {
			delete(oldTfs, tf)
			continue
		}
		tfSecs := utils2.TFToSecs(tf)
		sta := &PairTFCache{
			TimeFrame:  tf,
			TFSecs:     tfSecs,
			AlignOffMS: int64(exg.GetAlignOff(exgID, tfSecs) * 1000),
		}
		stateMap[tf] = sta
		if minTfSecs == 0 || sta.TFSecs < minTfSecs {
			minTfSecs = sta.TFSecs
		}
		adds = append(adds, tf)
	}
	// If you need to delete the unintroduced state, record the state of the minimum period to prevent rebuilding from blank again.
	// 如果需要删除未传入的，记录下最小周期的state，防止再次从空白重建
	var minDel *PairTFCache
	if delOther && len(oldTfs) > 0 {
		// Delete the time period passed in this time
		// 删除此次为传入的时间周期
		for tf := range oldTfs {
			if sta, ok := stateMap[tf]; ok {
				if sta.TFSecs == minTfSecs {
					minDel = sta
				}
				delete(stateMap, tf)
			}
		}
	}
	var newStates = utils.ValsOfMap(stateMap)
	// Sort all periods from small to large. The first one must be the least common multiple of all subsequent states, so that all subsequent states can be updated from the first one.
	// 对所有周期从小到大排序，第一个必须是后续所有states的最小公倍数，以便能从第一个更新后续所有
	slices.SortFunc(newStates, func(a, b *PairTFCache) int {
		return a.TFSecs - b.TFSecs
	})
	hourSecs := 3600
	maxTfSecs := newStates[len(newStates)-1].TFSecs
	if maxTfSecs > hourSecs {
		// 1h及以上k线，不应从小周期k线归集，否则会有差异，应直接使用数据库从交易所获取的。
		if _, ok := stateMap["1h"]; !ok {
			// 当需要1h以上级别数据，但未订阅1h时，需插入1h，以便后续从1h归集
			sta := &PairTFCache{
				TimeFrame:  "1h",
				TFSecs:     hourSecs,
				AlignOffMS: int64(exg.GetAlignOff(exgID, hourSecs) * 1000),
			}
			stateMap["1h"] = sta
			newStates = utils.ValsOfMap(stateMap)
			slices.SortFunc(newStates, func(a, b *PairTFCache) int {
				return a.TFSecs - b.TFSecs
			})
		}
	}
	secs := make([]int, len(newStates))
	for i, v := range newStates {
		secs[i] = v.TFSecs
	}
	minSecs := utils.GcdInts(secs)
	if minSecs != newStates[0].TFSecs {
		if minDel != nil && minDel.TFSecs == minSecs {
			newStates = append([]*PairTFCache{minDel}, newStates...)
		} else {
			minTf := utils2.SecsToTF(minSecs)
			newStates = append([]*PairTFCache{{TFSecs: minSecs, TimeFrame: minTf}}, newStates...)
		}
	}
	if maxTfSecs >= 3600 {
		// 使用1h及以上周期数据，额外添加1h的loader
		// 当使用DBKlineFeeder时，如果最小周期是1h，应将f.hour置为nil
		if f.hour == nil {
			f.hour = NewTfKlineLoader(f.ExSymbol, "1h")
		}
	} else {
		f.hour = nil
	}
	f.States = newStates
	return adds
}

/*
Update State and trigger callback (internal automatic restoration)
bars original unweighted K-line
更新State并触发回调（内部自动复权）
bars 原始未复权的K线
*/
func (f *Feeder) onStateOhlcvs(state *PairTFCache, bars []*banexg.Kline, lastOk bool) []*banexg.Kline {
	if len(bars) == 0 {
		return nil
	}
	state.Latest = bars[len(bars)-1]
	if state.WaitBar != nil && state.WaitBar.Time < bars[0].Time {
		bars = append([]*banexg.Kline{state.WaitBar}, bars...)
	}
	state.WaitBar = nil
	finishBars := bars
	if !lastOk {
		finishBars = bars[:len(bars)-1]
		state.WaitBar = state.Latest
	}
	tfMSecs := int64(state.TFSecs * 1000)
	for len(finishBars) > 0 && finishBars[0].Time < state.NextMS {
		finishBars = finishBars[1:]
	}
	if len(finishBars) > 0 {
		state.NextMS = finishBars[len(finishBars)-1].Time + tfMSecs
		f.addTfKlines(state.TimeFrame, finishBars)
		adjBars := f.adj.Apply(finishBars, core.AdjFront)
		f.fireCallBacks(state.TimeFrame, tfMSecs, adjBars, f.adj)
	}
	return finishBars
}

func (f *Feeder) getTfKlines(tf string, endMS int64, limit int, pBar *utils.PrgBar) ([]*banexg.Kline, *errs.Error) {
	bars, _ := f.tfBars[tf]
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	if len(bars) > 0 && bars[len(bars)-1].Time+tfMSecs == endMS {
		// 缓存有，直接返回
		bars = orm.ApplyAdj(f.adjs, bars, core.AdjFront, endMS, limit)
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return bars, nil
	}
	exchange, err := exg.GetWith(f.Exchange, f.Market, "")
	if err != nil {
		return nil, err
	}
	adjs, bars, err := orm.AutoFetchOHLCV(exchange, f.ExSymbol, tf, 0, endMS, limit, false, pBar)
	if err != nil {
		return nil, err
	}
	f.tfBars[tf] = bars
	bars = orm.ApplyAdj(adjs, bars, core.AdjFront, 0, 0)
	return bars, nil
}

func (f *Feeder) addTfKlines(tf string, bars []*banexg.Kline) {
	olds, _ := f.tfBars[tf]
	if len(olds) > core.NumTaCache*2 {
		olds = olds[len(olds)-core.NumTaCache*3/2:]
	}
	f.tfBars[tf] = append(olds, bars...)
}

func (f *Feeder) fireCallBacks(timeFrame string, tfMSecs int64, bars []*banexg.Kline, adj *orm.AdjInfo) {
	isLive := core.LiveMode
	pair := f.Symbol
	for _, bar := range bars {
		if !isLive || f.isWarmUp {
			btime.CurTimeMS = bar.Time + tfMSecs
		}
		f.CallBack(&orm.InfoKline{
			PairTFKline: &banexg.PairTFKline{Kline: *bar, Symbol: pair, TimeFrame: timeFrame},
			Adj:         adj,
			IsWarmUp:    f.isWarmUp,
		})
	}
	if isLive && !f.isWarmUp {
		// 检查是否延迟
		lastTime := bars[len(bars)-1].Time
		delay := btime.TimeMS() - (lastTime + tfMSecs)
		if delay > tfMSecs && tfMSecs >= 60000 {
			barNum := delay / tfMSecs
			log.Warn(fmt.Sprintf("%s/%s bar too late, delay %v bars, %v", pair, timeFrame, barNum, lastTime))
		}
	}
}

type IKlineFeeder interface {
	getSymbol() string
	getWaitBar() *banexg.Kline
	setWaitBar(bar *banexg.Kline)
	/*
		SubTfs Subscribe to data for a specified time period for the current target. Multiple 为当前标的订阅指定时间周期的数据，可多个
	*/
	SubTfs(timeFrames []string, delOther bool) []string
	/*
		WarmTfs The preheating time period gives the number of K lines to the specified time. 预热时间周期给定K线数量到指定时间
	*/
	WarmTfs(curMS int64, tfNums map[string]int, pBar *utils.PrgBar) (int64, map[string][2]int, *errs.Error)
	onNewBars(barTfMSecs int64, bars []*banexg.Kline) (bool, *errs.Error)
	getStates() []*PairTFCache
}

/*
KlineFeeder
Each Feeder corresponds to a trading pair. Can contain multiple time dimensions. Real use.

Supports dynamic addition of time dimension.
Supports returning preheating data. Each strategy + trading pair is preheated independently throughout the entire process, and cross-preheating is not allowed to avoid btime contamination.

Backtest mode: Use derived structure: DbKlineFeeder

Real mode: Subscribe to new data for this trading pair's time period and execute a callback when it is awakened.
Check whether this trading pair has been refreshed in the spider monitor. If not, send a message to the crawler monitor.
每个Feeder对应一个交易对。可包含多个时间维度。实盘使用。

	支持动态添加时间维度。
	支持返回预热数据。每个策略+交易对全程单独预热，不可交叉预热，避免btime被污染。

	回测模式：使用派生结构体：DbKlineFeeder

	实盘模式：订阅此交易对时间周期的新数据，被唤起时执行回调。
	检查此交易对是否已在spider监听刷新，如没有则发消息给爬虫监听。
*/
type KlineFeeder struct {
	Feeder
	PreFire  float64        // Ratio of triggering bar early 提前触发bar的比率
	adjIdx   int            // adjs的索引
	warmNums map[string]int // Preheating quantity in each cycle 各周期预热数量
	showLog  bool
}

func NewKlineFeeder(exs *orm.ExSymbol, callBack FnPairKline, showLog bool) (*KlineFeeder, *errs.Error) {
	adjs, err := orm.GetAdjs(exs.ID)
	if err != nil {
		return nil, err
	}
	return &KlineFeeder{
		Feeder: Feeder{
			ExSymbol: exs,
			CallBack: callBack,
			tfBars:   make(map[string][]*banexg.Kline),
			adjs:     adjs,
		},
		PreFire: config.PreFire,
		showLog: showLog,
	}, nil
}

func (f *KlineFeeder) WarmTfs(curMS int64, tfNums map[string]int, pBar *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	if len(tfNums) == 0 {
		tfNums = f.warmNums
		if len(tfNums) == 0 {
			return 0, nil, nil
		}
	} else {
		f.warmNums = tfNums
	}
	maxEndMs := int64(0)
	skips := make(map[string][2]int)
	hourDone := f.hour == nil
	for tf, warmNum := range tfNums {
		tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
		endMS := utils2.AlignTfMSecs(curMS, tfMSecs)
		if tfMSecs < int64(60000) || warmNum <= 0 {
			maxEndMs = max(maxEndMs, endMS)
			continue
		}
		bars, err := f.getTfKlines(tf, endMS, warmNum, pBar)
		if err != nil {
			return 0, nil, err
		}
		if len(bars) == 0 && f.showLog {
			skips[fmt.Sprintf("%s_%s", f.Symbol, tf)] = [2]int{warmNum, 0}
			continue
		}
		if warmNum != len(bars) && f.showLog {
			skips[fmt.Sprintf("%s_%s", f.Symbol, tf)] = [2]int{warmNum, len(bars)}
		}
		curEnd := f.warmTf(tf, bars)
		if !hourDone && tfMSecs == 3600000 {
			f.hour.SetSeek(curEnd)
			hourDone = true
		}
		maxEndMs = max(maxEndMs, curEnd)
	}
	if !hourDone && maxEndMs > 0 {
		f.hour.SetSeek(utils2.AlignTfMSecs(maxEndMs, 3600000))
	}
	return maxEndMs, skips, nil
}

/*
WarmTfs
Warm-up cycle data. When dynamically adding cycles to an existing HistDataFeeder, this method should be called to warm up the data.
If TaEnv already exists it will be reset.

LiveFeeder should also call this function when initializing
The incoming bars are the K-lines after the restoration of rights.

Returns the ending timestamp (i.e. the starting timestamp of the next bar)
预热周期数据。当动态添加周期到已有的HistDataFeeder时，应调用此方法预热数据。
如果TaEnv已存在会被重置。

	LiveFeeder在初始化时也应当调用此函数
	传入的bars是复权后的K线

返回结束的时间戳（即下一个bar开始时间戳）
*/
func (f *KlineFeeder) warmTf(tf string, bars []*banexg.Kline) int64 {
	if len(bars) == 0 {
		return 0
	}
	bakBt := core.BackTestMode
	core.BackTestMode = true
	defer func() {
		core.BackTestMode = bakBt
	}()
	f.isWarmUp = true
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	lastMS := bars[len(bars)-1].Time + tfMSecs
	envKey := strings.Join([]string{f.Symbol, tf}, "_")
	if env, ok := strat.Envs[envKey]; ok {
		env.Reset()
	}
	if len(f.adjs) > 0 {
		// 按复权信息分批调用
		cache := make([]*banexg.Kline, 0, len(bars))
		var pAdj = f.adjs[0]
		var pi = 1
		forEnd := false
		for i, k := range bars {
			for k.Time >= pAdj.StopMS {
				if len(cache) > 0 {
					f.fireCallBacks(tf, tfMSecs, cache, pAdj)
					cache = make([]*banexg.Kline, 0, len(bars))
				}
				if pi >= len(f.adjs) {
					f.fireCallBacks(tf, tfMSecs, bars[i:], nil)
					forEnd = true
					pAdj = nil
					break
				}
				pAdj = f.adjs[pi]
				pi += 1
			}
			if forEnd {
				break
			}
			cache = append(cache, k)
		}
		if len(cache) > 0 {
			f.fireCallBacks(tf, tfMSecs, cache, pAdj)
		}
	} else {
		f.fireCallBacks(tf, tfMSecs, bars, nil)
	}
	for _, sta := range f.States {
		if sta.TimeFrame == tf {
			sta.SubNextMS = lastMS
			break
		}
	}
	f.isWarmUp = false
	return lastMS
}

/*
onNewBars
There is newly completed sub-period candle data, try to update
bars are K lines that have not been re-righted and will be re-righted internally.
有新完成的子周期蜡烛数据，尝试更新
bars 是未复权的K线，内部会进行复权
*/
func (f *KlineFeeder) onNewBars(barTfMSecs int64, bars []*banexg.Kline) (bool, *errs.Error) {
	state := f.States[0]
	staMSecs := int64(state.TFSecs * 1000)
	var ohlcvs []*banexg.Kline
	var lastOk bool
	infoBy := f.InfoBy()
	if barTfMSecs < staMSecs {
		var olds []*banexg.Kline
		if state.WaitBar != nil {
			olds = append(olds, state.WaitBar)
		}
		ohlcvs, lastOk = utils.BuildOHLCV(bars, staMSecs, f.PreFire, olds, barTfMSecs, state.AlignOffMS, infoBy)
	} else if barTfMSecs == staMSecs {
		ohlcvs, lastOk = bars, true
	} else {
		barTf := utils2.SecsToTF(int(barTfMSecs / 1000))
		return false, errs.NewMsg(core.ErrInvalidBars, "bar intv invalid, %v expect %v, cur: %v", f.Symbol, state.TimeFrame, barTf)
	}
	if len(ohlcvs) == 0 {
		return false, nil
	}
	endMS := bars[len(bars)-1].Time + barTfMSecs
	var hourBars []*banexg.Kline
	useHour := false
	hourMSecs, hourAlignOff := int64(3600000), int64(0)
	firstReadHours := false
	if f.hour != nil {
		useHour = true
		firstReadHours = f.hour.FirstRead
		hourBars = f.hour.ReadTo(endMS, true)
	}
	minState, minOhlcvs := state, ohlcvs
	// 应该按周期从大到小触发
	if len(f.States) > 1 {
		// For the 2nd and subsequent coarse grains. OHLC updates from the first
		// Even if the first one is not completed, the coarser period dimension must be updated, otherwise data loss will occur
		// 对于第2个及后续的粗粒度。从第一个得到的OHLC更新
		// 即使第一个没有完成，也要更新更粗周期维度，否则会造成数据丢失
		if barTfMSecs < staMSecs {
			// The last unfinished data should be kept here
			// 这里应该保留最后未完成的数据
			ohlcvs, _ = utils.BuildOHLCV(bars, staMSecs, f.PreFire, nil, barTfMSecs, state.AlignOffMS, infoBy)
		} else {
			// 前面过滤了>，这里一定相等
			ohlcvs = bars
		}
		for i := len(f.States) - 1; i >= 1; i-- {
			state = f.States[i]
			srcMSecs, srcAlignOff := staMSecs, state.AlignOffMS
			if useHour && state.TFSecs >= 3600 {
				if len(hourBars) == 0 {
					continue
				}
				bars = hourBars
				srcMSecs, srcAlignOff = hourMSecs, hourAlignOff
			} else {
				bars = ohlcvs
			}
			subEndMS := bars[len(bars)-1].Time + srcMSecs
			olds, err := state.fillLacks(f.Symbol, int(srcMSecs/1000), bars[0].Time, subEndMS)
			if err != nil {
				return false, err
			}
			if state.WaitBar != nil {
				olds = append(olds, state.WaitBar)
			}
			bigTfMSecs := int64(state.TFSecs * 1000)
			curOhlcvs, lastDone := utils.BuildOHLCV(bars, bigTfMSecs, f.PreFire, olds, srcMSecs, srcAlignOff, infoBy)
			f.onStateOhlcvs(state, curOhlcvs, lastDone)
		}
	}
	isWarmUp := false
	if useHour && minState.TFSecs >= 3600 {
		if len(hourBars) == 0 {
			return false, nil
		}
		minOhlcvs = hourBars
		lastOk = true
		isWarmUp = firstReadHours
	}
	//Subsequence period dimension <= current dimension. When receiving the data sent by the spider, there may be 3 or more ohlcvs
	//子序列周期维度<=当前维度。当收到spider发送的数据时，这里可能是3个或更多ohlcvs
	if isWarmUp {
		// 对于4h及以上的实盘时，第一次会读取1h的一些k线，应当视为预热
		f.isWarmUp = true
	}
	doneBars := f.onStateOhlcvs(minState, minOhlcvs, lastOk)
	if isWarmUp {
		f.isWarmUp = false
	}
	return len(doneBars) > 0, nil
}

type Batch interface {
	TimeMS() int64
}

type IHistFeeder interface {
	getNextMS() int64
	SetSeek(since int64)
	SetEndMS(ms int64)
	GetBatch() Batch
	RunBatch(bar Batch) *errs.Error
	CallNext()
	Type() string
	getSymbol() string
}

type IHistKlineFeeder interface {
	IKlineFeeder
	IHistFeeder
	// DownIfNeed Download the entire range of K lines, which needs to be called before SetSeek  下载整个范围的K线，需在SetSeek前调用
	DownIfNeed(sess *orm.Queries, exchange banexg.BanExchange, pBar *utils.PrgBar) *errs.Error
}

/*
DBKlineFeeder
Historical data feedback device. Is the base class for file feedback and database feedback.

Backtest mode: Read 3K bars each time, and backtest triggers in sequence according to nextMS size.
历史数据反馈器。是文件反馈器和数据库反馈器的基类。

	回测模式：每次读取3K个bar，按nextMS大小依次回测触发。
*/
type DBKlineFeeder struct {
	KlineFeeder
	*TfKlineLoader
	TradeTimes [][2]int64 // Trading time 可交易时间
}

/*
get the end timestamp of next bar
*/
func (f *DBKlineFeeder) getNextMS() int64 {
	return f.nextMS
}

func (f *DBKlineFeeder) SetEndMS(ms int64) {
	f.EndMS = ms
	if f.hour != nil {
		f.hour.EndMS = ms
	}
}

func (f *DBKlineFeeder) SetSeek(since int64) {
	f.TfKlineLoader.SetSeek(since)
	if f.hour != nil {
		f.hour.SetSeek(since)
	}
}

func (f *DBKlineFeeder) Type() string {
	return core.WsSubKLine
}

/*
SubTfs
Add monitoring to States and return the newly added TimeFrames
添加监听到States中，返回新增的TimeFrames
*/
func (f *DBKlineFeeder) SubTfs(timeFrames []string, delOther bool) []string {
	arr := f.Feeder.SubTfs(timeFrames, delOther)
	minTF := ""
	if len(f.States) > 0 {
		minTF = f.States[0].TimeFrame
	}
	if minTF == "1h" {
		// 已有loader加载1h，将hour置为nil
		f.hour = nil
	}
	f.SetTimeFrame(minTF)
	return arr
}

func (f *DBKlineFeeder) CallNext() {
	f.TfKlineLoader.SetNext()
	if f.rowIdx > 0 {
		// 缓存索引移动
		curMS := f.nextMS - f.TFMSecs
		if f.adj != nil && curMS >= f.adj.StopMS {
			old := f.caches[f.rowIdx-1]
			tf := f.Timeframe
			f.OnEnvEnd(&banexg.PairTFKline{
				Symbol:    f.Symbol,
				TimeFrame: tf,
				Kline: banexg.Kline{
					Time:   old.Time + f.TFMSecs,
					Open:   old.Close,
					High:   old.Close,
					Low:    old.Close,
					Close:  old.Close,
					Volume: old.Close,
				},
			}, f.adj)
			// Warm up again
			// 重新复权预热
			_, skips, err := f.WarmTfs(curMS, nil, nil)
			if err != nil {
				log.Error("next warm tf fail", zap.Error(err))
			} else if len(skips) > 0 {
				log.Warn("warm lacks", zap.String("items", StrWarmLacks(skips)))
			}
			f.setAdjIdx()
		}
	} else if f.rowIdx == 0 {
		// 重新加载了数据
		f.setAdjIdx()
	}
}

func (f *DBKlineFeeder) setAdjIdx() {
	for f.adjIdx < len(f.adjs) {
		adj := f.adjs[f.adjIdx]
		if f.nextMS < adj.StopMS {
			f.adj = adj
			return
		}
		f.adjIdx += 1
	}
	f.adj = nil
}

func (f *DBKlineFeeder) GetBatch() Batch {
	bar := f.GetBar()
	if bar != nil {
		return bar
	}
	return nil
}

func (f *DBKlineFeeder) RunBatch(batch Batch) *errs.Error {
	if bar, ok := batch.(*banexg.Kline); ok {
		_, err := f.onNewBars(f.TFMSecs, []*banexg.Kline{bar})
		return err
	}
	return nil
}

func NewDBKlineFeeder(exs *orm.ExSymbol, callBack FnPairKline, showLog bool) (*DBKlineFeeder, *errs.Error) {
	exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return nil, err
	}
	var tradeTimes [][2]int64
	market, err := exchange.GetMarket(exs.Symbol)
	if err == nil {
		tradeTimes = market.GetTradeTimes()
	}
	feeder, err := NewKlineFeeder(exs, callBack, showLog)
	if err != nil {
		return nil, err
	}
	res := &DBKlineFeeder{
		KlineFeeder:   *feeder,
		TfKlineLoader: NewTfKlineLoader(exs, ""),
		TradeTimes:    tradeTimes,
	}
	return res, nil
}

/*
TfKlineLoader 用于分批加载某个品种的指定周期K线，然后逐个读取的场景
*/
type TfKlineLoader struct {
	*orm.ExSymbol
	Timeframe string
	TFMSecs   int64

	EndMS     int64
	FirstRead bool
	rowIdx    int             // The index of the next Bar in the cache, -1 means it has ended 缓存中下一个Bar的索引，-1表示已结束
	caches    []*banexg.Kline // Cached Bar, fire one by one, reload after reading 缓存的Bar，逐个fire，读取完重新加载
	nextMS    int64           // The 13-digit millisecond end timestamp of the next bar, math.MaxInt32 indicates the end 下一个bar的结束13位毫秒时间戳，math.MaxInt32表示结束
	offsetMS  int64
}

func NewTfKlineLoader(exs *orm.ExSymbol, tf string) *TfKlineLoader {
	tfMSecs := int64(0)
	if tf != "" {
		tfMSecs = int64(utils2.TFToSecs(tf) * 1000)
	}
	endMS := config.TimeRange.EndMS
	if core.LiveMode {
		// 实时模式下，EndMS截取为当前对齐时间
		endMS = btime.UTCStamp()
		if tfMSecs > 0 {
			endMS = utils2.AlignTfMSecs(endMS, tfMSecs)
		}
	}
	return &TfKlineLoader{
		ExSymbol:  exs,
		Timeframe: tf,
		TFMSecs:   tfMSecs,
		EndMS:     endMS,
		FirstRead: true,
	}
}

func (f *TfKlineLoader) GetBar() *banexg.Kline {
	if f.rowIdx < 0 || f.rowIdx >= len(f.caches) {
		return nil
	}
	bar := f.caches[f.rowIdx]
	return bar
}

func (f *TfKlineLoader) SetSeek(since int64) {
	f.Reset(since)
	f.SetNext()
}

func (f *TfKlineLoader) SetTimeFrame(tf string) {
	if f.Timeframe == tf {
		return
	}
	f.Timeframe = tf
	f.TFMSecs = int64(utils2.TFToSecs(tf) * 1000)
	f.EndMS = utils2.AlignTfMSecs(f.EndMS, f.TFMSecs)
	f.Reset(0)
}

func (f *TfKlineLoader) Reset(since int64) {
	if since == 0 {
		// 这里不能为0，不然会从后往前读取K线，导致缺失
		since = core.MSMinStamp
	}
	f.rowIdx = 0
	f.nextMS = 0
	f.offsetMS = since
	f.caches = nil
}

func (f *TfKlineLoader) ReadTo(end int64, force bool) []*banexg.Kline {
	end = utils2.AlignTfMSecs(end, 3600000)
	if force && f.EndMS < end {
		f.EndMS = end
		if f.nextMS == math.MaxInt64 {
			f.Reset(f.offsetMS)
		}
	}
	if f.rowIdx >= len(f.caches) {
		// 缓存为空，未读取完（f.rowIdx >= 0时必定nextMS < math.MaxInt64）
		f.SetNext()
	}
	var result []*banexg.Kline
	for f.rowIdx >= 0 && f.rowIdx < len(f.caches) {
		bar := f.caches[f.rowIdx]
		barEnd := bar.Time + f.TFMSecs
		if barEnd > end {
			break
		}
		result = append(result, bar)
		f.SetNext()
		if barEnd == end {
			break
		}
	}
	if f.FirstRead {
		f.FirstRead = false
	}
	return result
}

/*
DownIfNeed
Download data for a specified interval
pBar is used for progress update, the total is 1000, and the amount is updated each time
下载指定区间的数据
pBar 用于进度更新，总和为1000，每次更新此次的量
*/
func (f *TfKlineLoader) DownIfNeed(sess *orm.Queries, exchange banexg.BanExchange, pBar *utils.PrgBar) *errs.Error {
	if f.Timeframe == "" || f.DelistMs > 0 {
		return nil
	}
	downTf, err := orm.GetDownTF(f.Timeframe)
	if err != nil {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return err
	}
	if sess == nil {
		ctx := context.Background()
		var conn *pgxpool.Conn
		sess, conn, err = orm.Conn(ctx)
		if err != nil {
			if pBar != nil {
				pBar.Add(core.StepTotal)
			}
			return err
		}
		defer conn.Release()
	}
	_, err = sess.DownOHLCV2DB(exchange, f.ExSymbol, downTf, btime.TimeMS(), f.EndMS, pBar)
	return err
}

func (f *TfKlineLoader) SetNext() {
	if f.rowIdx+1 < len(f.caches) {
		f.rowIdx += 1
		nextStartMS := f.caches[f.rowIdx].Time
		f.nextMS = nextStartMS + f.TFMSecs
		return
	}
	// 检查是否还有可读取内容
	endMS := f.EndMS
	if endMS > 0 && f.nextMS >= endMS {
		f.rowIdx = -1
		// 将nextMS置为math.MaxInt64前，应备份到offsetMS，以便实盘读取更新的k线
		f.offsetMS = max(f.offsetMS, f.nextMS)
		f.nextMS = math.MaxInt64
		return
	}
	// After the cache reading is completed, re-read the database
	// 缓存读取完毕，重新读取数据库
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		f.rowIdx = -1
		f.offsetMS = max(f.offsetMS, f.nextMS)
		f.nextMS = math.MaxInt64
		log.Error("get conn fail while loading kline", zap.Error(err))
		return
	}
	defer conn.Release()
	batchSize := 3000
	if core.BackTestMode {
		// QuestDB performs better with fewer, larger range queries than many small ones.
		batchSize = 20000
	}
	_, bars, err := sess.GetOHLCV(f.ExSymbol, f.Timeframe, f.offsetMS, endMS, batchSize, true)
	if err != nil || len(bars) == 0 {
		f.rowIdx = -1
		f.offsetMS = max(f.offsetMS, f.nextMS)
		f.nextMS = math.MaxInt64
		if err != nil {
			log.Error("load kline fail", zap.Error(err))
		}
		return
	}
	f.caches = bars
	f.rowIdx = 0
	f.nextMS = bars[0].Time + f.TFMSecs
	f.offsetMS = bars[len(bars)-1].Time + f.TFMSecs
}

func StrWarmLacks(skips map[string][2]int) string {
	if len(skips) == 0 {
		return ""
	}
	var b strings.Builder
	for k, arr := range skips {
		b.WriteString(k)
		b.WriteString(": ")
		lackPct := float64(arr[0]-arr[1]) * 100 / float64(arr[0])
		b.WriteString(strconv.FormatFloat(lackPct, 'f', 0, 64))
		b.WriteString("%, ")
	}
	var res = b.String()
	return res[:len(res)-2]
}
