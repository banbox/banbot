package biz

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

var (
	accOdMgrs     = make(map[string]IOrderMgr)
	accLiveOdMgrs = make(map[string]*LiveOrderMgr)
)

const ExitTagOCOCancel = "oco_cancel"

type IOrderMgr interface {
	ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
	EditOrder(od *ormo.InOutOrder, action string)
	RelayOrders(orders []*ormo.InOutOrder) *errs.Error
	EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error)
	ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error)
	ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error)
	UpdateByDataSeries(allOpens []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error
	ExitAndFill(orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error
	OnEnvEnd(evt *orm.DataSeries) *errs.Error
	CleanUp() *errs.Error
}

type IOrderMgrLive interface {
	IOrderMgr
	SyncExgOrders() ([]*ormo.InOutOrder, []*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
	WatchMyTrades()
	TrialUnMatchesForever()
	ConsumeOrderQueue()
}

type FuncHandleIOrder = func(order *ormo.InOutOrder) *errs.Error

// runtimeOrderConfig is the immutable, hot-path view of configuration needed
// by one order manager. It is copied when the manager is bound so Runtime
// order processing never consults the process-wide config facade.
type runtimeOrderConfig struct {
	stakeCurrency        []string
	takeOverStrategy     string
	orderType            string
	limitVolSecs         int
	putLimitSecs         int
	orderBookTTL         int64
	stopEnterBars        int
	maxOpenOrders        int
	maxSimulOpen         int
	backtestNetCost      float64
	legacyIntrabar       bool
	slippageBps          float64
	gapBars              int     // bt_gap_bars (also arms the delist exit)
	gapMinHours          float64 // bt_gap_min_hours
	delistPenaltyBps     float64 // bt_delist_penalty_bps (071)
	fundingApr           float64
	legacyApproxFunding  bool
	accountLeverage      float64
	accountMaxOpenOrders int
}

type fundingRatePoint struct {
	atMS int64
	rate float64
}

type OrderMgr struct {
	callBack        func(order *ormo.InOutOrder, isEnter bool)
	afterEnter      FuncHandleIOrder
	afterExit       FuncHandleIOrder
	prices          *com.PriceState
	clock           *btime.ClockState
	exchange        banexg.BanExchange
	dump            *orm.DumpSink
	runtimeCore     *core.State
	symbols         *orm.SymbolState
	wallet          *BanWallets
	runtimeDeps     bool
	walletDeps      RuntimeDeps
	runtimeCfg      runtimeOrderConfig
	Account         string
	BarMS           int64
	simulOpen       int // Simultaneously open number in the current bar
	simulOpenSt     map[string]int
	seenSeriesMu    sync.RWMutex
	seenSeriesEndMS map[string]int64

	fundingMu       sync.Mutex
	fundingRepo     orm.SeriesRepo
	fundingLoaded   map[string]bool
	fundingStored   map[string][]fundingRatePoint
	fundingStreamed map[string][]fundingRatePoint
}

func accountMaxOpenOrders(accounts map[string]*config.AccountConfig, account string) int {
	if acc := accounts[account]; acc != nil && acc.MaxOpenOrders > 0 {
		return acc.MaxOpenOrders
	}
	return 0
}

func (o *OrderMgr) stakeCurrency() []string {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.stakeCurrency
	}
	return config.StakeCurrency
}

func (o *OrderMgr) takeOverStrategy() string {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.takeOverStrategy
	}
	return config.TakeOverStrat
}

func (o *OrderMgr) orderType() string {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.orderType
	}
	return config.OrderType
}

func (o *OrderMgr) limitVolSecs() int {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.limitVolSecs
	}
	return config.LimitVolSecs
}

func (o *OrderMgr) putLimitSecs() int {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.putLimitSecs
	}
	return config.PutLimitSecs
}

func (o *OrderMgr) orderBookTTL() int64 {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.orderBookTTL
	}
	return config.OdBookTtl
}

func (o *OrderMgr) stopEnterBars() int {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.stopEnterBars
	}
	return config.StopEnterBars
}

func (o *OrderMgr) maxOpenOrders() int {
	if o != nil && o.runtimeDeps {
		if o.runtimeCfg.accountMaxOpenOrders > 0 {
			return o.runtimeCfg.accountMaxOpenOrders
		}
		return o.runtimeCfg.maxOpenOrders
	}
	return config.MaxOpenOrders
}

func (o *OrderMgr) maxSimulOpen() int {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.maxSimulOpen
	}
	return config.MaxSimulOpen
}

func (o *OrderMgr) backtestNetCost() float64 {
	if o != nil && o.runtimeDeps {
		return o.runtimeCfg.backtestNetCost
	}
	return config.BTNetCost
}

func (o *OrderMgr) accountLeverage() float64 {
	var leverage float64
	if o != nil && o.runtimeDeps {
		leverage = o.runtimeCfg.accountLeverage
	} else {
		leverage = config.GetAccLeverage(o.Account)
	}
	if leverage == 0 {
		return 1 // An omitted leverage must not cause infinite margin requirements.
	}
	return leverage
}

func (o *OrderMgr) takeOverTF(pair, defTF string) string {
	if o == nil || !o.runtimeDeps {
		return config.GetTakeOverTF(pair, defTF)
	}
	if o.runtimeCore != nil {
		if tf, ok := o.runtimeCore.StrategyTimeFrame(o.takeOverStrategy(), pair); ok && tf != "" {
			return tf
		}
	}
	return defTF
}

func (o *OrderMgr) priceNow() int64 {
	if o != nil && o.clock != nil {
		return o.clock.TimeMS()
	}
	if o != nil && o.runtimeDeps {
		return 0
	}
	return btime.TimeMS()
}

func (o *OrderMgr) priceSafeExp(symbol, side string, expMS int64) float64 {
	if o != nil && o.prices != nil {
		return o.prices.GetPriceSafeExpAt(o.priceNow(), symbol, side, expMS)
	}
	if o != nil && o.runtimeDeps {
		return -1
	}
	return com.GetPriceSafeExp(symbol, side, expMS)
}

func (o *OrderMgr) priceExp(symbol, side string, expMS int64) float64 {
	price := o.priceSafeExp(symbol, side, expMS)
	if price < 0 {
		panic(fmt.Errorf("invalid symbol for price: %s", symbol))
	}
	return price
}

func (o *OrderMgr) lastBarPrice(symbol string) float64 {
	if o != nil && o.prices != nil {
		return o.prices.GetLastBarPriceAt(symbol)
	}
	if o != nil && o.runtimeDeps {
		return -1
	}
	return com.GetLastBarPrice(symbol)
}

// lastBarTime is the time the last bar price of symbol was registered (0 = never).
func (o *OrderMgr) lastBarTime(symbol string) int64 {
	if o != nil && o.prices != nil {
		return o.prices.GetLastBarTimeAt(symbol)
	}
	if o != nil && o.runtimeDeps {
		return 0
	}
	return com.GetLastBarTime(symbol)
}

func (o *OrderMgr) exchangeClient() banexg.BanExchange {
	if o != nil && o.runtimeDeps {
		return o.exchange
	}
	return exg.Default
}

func (o *OrderMgr) marketType() string {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.Market
		}
		if o.exchange != nil {
			if info := o.exchange.Info(); info != nil {
				return info.MarketType
			}
		}
		return ""
	}
	return core.Market
}

func (o *OrderMgr) exchangeName() string {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil && o.runtimeCore.ExgName != "" {
			return o.runtimeCore.ExgName
		}
		if o.exchange != nil {
			if info := o.exchange.Info(); info != nil {
				return info.ID
			}
		}
		return ""
	}
	return core.ExgName
}

func (o *OrderMgr) isContract() bool {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.IsContract || banexg.IsContract(o.runtimeCore.Market)
		}
		return banexg.IsContract(o.marketType())
	}
	return core.IsContract
}

func (o *OrderMgr) isLive() bool {
	if o != nil && o.runtimeDeps {
		return o.runtimeCore != nil && o.runtimeCore.LiveMode
	}
	return core.LiveMode
}

func (o *OrderMgr) isEnvReal() bool {
	if o != nil && o.runtimeDeps {
		return o.runtimeCore != nil && o.runtimeCore.EnvReal
	}
	return core.EnvReal
}

func (o *OrderMgr) isBacktest() bool {
	if o != nil && o.runtimeDeps {
		return o.runtimeCore != nil && o.runtimeCore.BackTestMode
	}
	return core.BackTestMode
}

func (o *OrderMgr) runMode() string {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.RunMode
		}
		return ""
	}
	return core.RunMode
}

func (o *OrderMgr) runEnv() string {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.RunEnv
		}
		return ""
	}
	return core.RunEnv
}

func (o *OrderMgr) pairIsBanned(pair string, nowMS int64) bool {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore == nil {
			return false
		}
		return o.runtimeCore.IsPairBanned(pair, nowMS)
	}
	return core.LegacyPairIsBanned(pair, nowMS)
}

func (o *OrderMgr) noEnterUntilFor(account string) (int64, bool) {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore == nil {
			return 0, false
		}
		return o.runtimeCore.NoEnterUntilFor(account)
	}
	return core.LegacyNoEnterUntilFor(account)
}

func (o *OrderMgr) setNoEnterUntil(account string, untilMS int64) {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			o.runtimeCore.SetNoEnterUntil(account, untilMS)
		}
		return
	}
	core.SetLegacyNoEnterUntil(account, untilMS)
}

func (o *OrderMgr) checkWallets() bool {
	if o != nil && o.runtimeDeps {
		return o.runtimeCore != nil && o.runtimeCore.ShouldCheckWallets()
	}
	return core.CheckWallets
}

func (o *OrderMgr) stopAll() func() {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			return o.runtimeCore.StopAll
		}
		return nil
	}
	return core.StopAll
}

func (o *OrderMgr) setBotRunning(running bool) {
	if o != nil && o.runtimeDeps {
		if o.runtimeCore != nil {
			o.runtimeCore.SetBotRunning(running)
		}
		return
	}
	core.BotRunning = running
}

func (o *OrderMgr) setExit(od *ormo.InOutOrder, exitAt int64, tag, orderType string, limit float64) {
	if o == nil || !o.runtimeDeps {
		od.SetExit(exitAt, tag, orderType, limit)
		return
	}
	if exitAt == 0 {
		exitAt = o.priceNow()
	}
	if od.ExitAt == 0 {
		if tag == "" {
			tag = core.ExitTagUnknown
		}
		od.ExitTag = tag
		od.ExitAt = exitAt
		od.DirtyMain = true
	}
	if od.Exit == nil {
		odSide := banexg.OdSideSell
		if od.Short {
			odSide = banexg.OdSideBuy
		}
		if o.runtimeCore != nil {
			o.runtimeCore.AddSimOrder()
		}
		od.Exit = &ormo.ExOrder{
			TaskID:    od.TaskID,
			InoutID:   od.ID,
			Symbol:    od.Symbol,
			Enter:     false,
			OrderType: orderType,
			Side:      odSide,
			CreateAt:  exitAt,
			UpdateAt:  exitAt,
			Price:     limit,
			Amount:    od.Enter.Filled,
			Status:    ormo.OdStatusInit,
		}
		od.DirtyExit = true
		return
	}
	if orderType != "" {
		od.Exit.OrderType = orderType
		od.DirtyExit = true
	}
	if limit > 0 {
		od.Exit.Price = limit
		od.DirtyExit = true
	}
}

func (o *OrderMgr) inSimOrderMatch() bool {
	if o != nil && o.runtimeDeps {
		return o.runtimeCore != nil && o.runtimeCore.InSimOrderMatch()
	}
	return core.SimOrderMatch
}

func (o *OrderMgr) canClose(od *ormo.InOutOrder) bool {
	if od == nil || od.ExitTag != "" {
		return false
	}
	if od.Timeframe == "ws" || o.inSimOrderMatch() {
		return true
	}
	if o == nil || !o.runtimeDeps {
		matchTf := od.Timeframe
		if o != nil {
			if refined := o.refineTimeFrame(od.Strategy, od.Timeframe); refined != "" {
				matchTf = refined
			}
		}
		if matchTf != od.Timeframe {
			tfMSecs := int64(utils.TFToSecs(matchTf) * 1000)
			return float64(btime.TimeMS()-od.RealEnterMS()) > float64(tfMSecs)*0.9
		}
		return od.CanClose()
	}
	matchTf := o.refineTimeFrame(od.Strategy, od.Timeframe)
	if matchTf == "" {
		matchTf = od.Timeframe
	}
	tfMSecs := int64(utils.TFToSecs(matchTf) * 1000)
	return float64(o.priceNow()-od.RealEnterMS()) > float64(tfMSecs)*0.9
}

func (o *OrderMgr) updateOrderFee(od *ormo.InOutOrder, price float64, forEnter bool) *errs.Error {
	if o == nil || !o.runtimeDeps {
		err := od.UpdateFee(price, forEnter)
		if err == nil && !forEnter && od.Exit != nil && o != nil && core.BackTestMode {
			o.settleBacktestFundingWithMode(od, config.Data.BTFundingApr, core.IsContract, core.Market == banexg.MarketInverse, config.Data.BTLegacyApproxFunding, o.priceNow())
		}
		return err
	}
	exchange := o.exchangeClient()
	if exchange == nil {
		return errs.NewMsg(core.ErrExgNotInit, "exchange is required to calculate fee for %s", od.Symbol)
	}
	exOrder := od.Enter
	if !forEnter {
		exOrder = od.Exit
	}
	if exOrder == nil {
		return errs.NewMsg(errs.CodeRunTime, "fee order is nil for %s", od.Symbol)
	}
	fee, err := execution.LegacyOrderFee(exchange, exOrder.Symbol, &exOrder.OrderType,
		exOrder.Side, exOrder.Filled, price)
	if err != nil {
		return err
	}
	if fee == nil {
		return errs.NewMsg(errs.CodeRunTime, "exchange returned nil fee for %s", od.Symbol)
	}
	exOrder.Fee = fee.Cost
	exOrder.FeeQuote = fee.QuoteCost
	exOrder.FeeType = fee.Currency
	if forEnter {
		od.DirtyEnter = true
	} else {
		od.DirtyExit = true
		if o.isBacktest() {
			o.settleBacktestFundingWithMode(od, o.runtimeCfg.fundingApr, o.isContract(), o.marketType() == banexg.MarketInverse, o.runtimeCfg.legacyApproxFunding, o.priceNow())
		}
	}
	return nil
}

// RecordFundingSeries records a non-warmup settled funding_rate event observed
// during backtest replay so OrderMgr can settle exact bilateral funding even
// when series events are fed directly without a backing database.
func (o *OrderMgr) RecordFundingSeries(symbol string, evt *orm.DataSeries) {
	if o == nil || !o.isBacktest() || evt == nil || evt.IsWarmUp || symbol == "" {
		return
	}
	rate, ok := evt.FloatValueDefault("rate")
	if !ok || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return
	}
	atMS := evt.EndMS
	if atMS <= 0 {
		atMS = evt.TimeMS
	}
	if atMS <= 0 {
		return
	}
	o.fundingMu.Lock()
	defer o.fundingMu.Unlock()
	if o.fundingStreamed == nil {
		o.fundingStreamed = make(map[string][]fundingRatePoint)
	}
	pts := o.fundingStreamed[symbol]
	if n := len(pts); n > 0 && pts[n-1].atMS == atMS {
		pts[n-1].rate = rate
		return
	}
	o.fundingStreamed[symbol] = append(pts, fundingRatePoint{atMS: atMS, rate: rate})
}

func (o *OrderMgr) symbolFundingPoints(symbol string) ([]fundingRatePoint, bool) {
	if o == nil || symbol == "" {
		return nil, false
	}
	o.fundingMu.Lock()
	if o.fundingLoaded == nil {
		o.fundingLoaded = make(map[string]bool)
	}
	if o.fundingStored == nil {
		o.fundingStored = make(map[string][]fundingRatePoint)
	}
	loaded := o.fundingLoaded[symbol]
	repo := o.fundingRepo
	o.fundingMu.Unlock()

	if !loaded {
		var stored []fundingRatePoint
		if repo == nil && o.runtimeDeps && o.walletDeps.Storage != nil {
			repo = orm.NewSeriesRepo(o.walletDeps.Storage)
		}
		if repo != nil {
			if exs, err := o.exSymbolCur(symbol); err == nil && exs != nil && exs.ID > 0 {
				var startMS int64 = 0
				var endMS int64 = math.MaxInt64 / 2
				if cfg := o.walletDeps.ConfigView(); cfg != nil && cfg.TimeRange != nil {
					if cfg.TimeRange.StartMS > 8*3600*1000 {
						startMS = cfg.TimeRange.StartMS - 8*3600*1000
					}
					if cfg.TimeRange.EndMS > 0 {
						endMS = cfg.TimeRange.EndMS + 8*3600*1000
					}
				}
				info := orm.NewFundingRateSeriesInfo(orm.DefaultFundingRateTF)
				if rows, qErr := repo.QuerySeriesRange(context.Background(), info, exs.ID, startMS, endMS, 0); qErr == nil && len(rows) > 0 {
					stored = make([]fundingRatePoint, 0, len(rows))
					for _, row := range rows {
						if row == nil {
							continue
						}
						rate, ok := extractFundingRateFloat(row.Values["rate"])
						if !ok {
							continue
						}
						atMS := row.EndMS
						if atMS <= 0 {
							atMS = row.TimeMS
						}
						if atMS > 0 {
							stored = append(stored, fundingRatePoint{atMS: atMS, rate: rate})
						}
					}
				}
			}
		}
		o.fundingMu.Lock()
		o.fundingLoaded[symbol] = true
		o.fundingStored[symbol] = stored
		o.fundingMu.Unlock()
	}

	o.fundingMu.Lock()
	stored := o.fundingStored[symbol]
	streamed := o.fundingStreamed[symbol]
	if len(stored) == 0 && len(streamed) == 0 {
		o.fundingMu.Unlock()
		return nil, false
	}
	merged := mergeFundingRatePoints(stored, streamed)
	o.fundingMu.Unlock()
	return merged, len(merged) > 0
}

func extractFundingRateFloat(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float64:
		return v, !math.IsNaN(v) && !math.IsInf(v, 0)
	case float32:
		f := float64(v)
		return f, !math.IsNaN(f) && !math.IsInf(f, 0)
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

func mergeFundingRatePoints(a, b []fundingRatePoint) []fundingRatePoint {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		cp := append([]fundingRatePoint(nil), b...)
		sort.SliceStable(cp, func(i, j int) bool { return cp[i].atMS < cp[j].atMS })
		return dedupeFundingRatePoints(cp)
	}
	cp := make([]fundingRatePoint, 0, len(a)+len(b))
	cp = append(cp, a...)
	cp = append(cp, b...)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].atMS < cp[j].atMS })
	return dedupeFundingRatePoints(cp)
}

func dedupeFundingRatePoints(pts []fundingRatePoint) []fundingRatePoint {
	out := pts[:0]
	for _, pt := range pts {
		if len(out) > 0 && out[len(out)-1].atMS == pt.atMS {
			out[len(out)-1] = pt
			continue
		}
		out = append(out, pt)
	}
	return out
}

// settleBacktestFunding settles bilateral perpetual funding over (enterMS, exitMS]
// when real funding_rate data is available for od.Symbol (long pays positive rate
// and receives negative rate; short receives positive rate and pays negative rate).
// When no funding_rate series exists for the symbol, it falls back to addApproxFundingWithMode.
func (o *OrderMgr) settleBacktestFunding(od *ormo.InOutOrder, aprPct float64, contract, inverse bool, nowMS int64) {
	o.settleBacktestFundingWithMode(od, aprPct, contract, inverse, false, nowMS)
}

func (o *OrderMgr) settleBacktestFundingWithMode(od *ormo.InOutOrder, aprPct float64, contract, inverse, legacyUnilateral bool, nowMS int64) {
	if !contract || od == nil || od.Enter == nil || od.Exit == nil {
		return
	}
	if inverse {
		if pts, ok := o.symbolFundingPoints(od.Symbol); ok && len(pts) > 0 {
			od.SetInfo("funding_skipped", "inverse")
			return
		}
		addApproxFundingWithMode(od, aprPct, contract, inverse, legacyUnilateral, nowMS)
		return
	}
	pts, hasReal := o.symbolFundingPoints(od.Symbol)
	if !hasReal {
		addApproxFundingWithMode(od, aprPct, contract, inverse, legacyUnilateral, nowMS)
		return
	}
	enterMS := od.RealEnterMS()
	exitMS := od.Exit.UpdateAt
	if exitMS <= enterMS {
		exitMS = nowMS
	}
	if exitMS <= enterMS {
		return
	}
	entryPrice := od.Enter.Average
	if entryPrice <= 0 {
		entryPrice = od.InitPrice
	}
	notional := entryPrice * od.Enter.Filled
	if notional <= 0 {
		return
	}
	signedNotional := notional
	if od.Short {
		signedNotional = -notional
	}
	startIdx := sort.Search(len(pts), func(i int) bool { return pts[i].atMS > enterMS })
	var totalCost float64
	crossed := 0
	for i := startIdx; i < len(pts) && pts[i].atMS <= exitMS; i++ {
		totalCost += signedNotional * pts[i].rate
		crossed++
	}
	od.SetInfo("funding_mode", "real")
	if crossed == 0 || totalCost == 0 {
		od.SetInfo("funding", 0.0)
		return
	}
	od.Exit.Fee += totalCost
	od.Exit.FeeQuote += totalCost
	od.DirtyExit = true
	od.SetInfo("funding", totalCost)
}

// addApproxFunding (055, bilateral symmetric since BtFeatureLevel 15) settles a constant-rate approximate
// funding transfer on the exit leg of a linear contract order: entry notional x apr% x holding years.
// Under positive aprPct, longs pay (+cost added to Exit.Fee/FeeQuote) and shorts receive (-cost added to
// Exit.Fee/FeeQuote, increasing order profit), so an equal-notional 1:1 long+short hedge incurs zero net
// funding distortion. Inverse coin-margined contracts are skipped and tagged funding_skipped=inverse.
func addApproxFunding(od *ormo.InOutOrder, aprPct float64, contract, inverse bool, nowMS int64) {
	addApproxFundingWithMode(od, aprPct, contract, inverse, false, nowMS)
}

func addApproxFundingWithMode(od *ormo.InOutOrder, aprPct float64, contract, inverse, legacyUnilateral bool, nowMS int64) {
	if aprPct == 0 || !contract || od == nil || od.Enter == nil || od.Exit == nil {
		return
	}
	if legacyUnilateral && (aprPct < 0 || od.Short) {
		return
	}
	if inverse {
		od.SetInfo("funding_skipped", "inverse")
		return
	}
	exitMS := od.Exit.UpdateAt
	if exitMS <= od.EnterAt {
		exitMS = nowMS
	}
	cost := fundingApproxCost(od.Enter.Average*od.Enter.Filled, aprPct, exitMS-od.EnterAt)
	if od.Short {
		cost = -cost
	}
	if cost == 0 {
		return
	}
	od.Exit.Fee += cost
	od.Exit.FeeQuote += cost
	od.DirtyExit = true
	od.SetInfo("funding", cost)
}

// fundingApproxCost is notional x apr% x (holdMS in years).
func fundingApproxCost(notional, aprPct float64, holdMS int64) float64 {
	if aprPct == 0 || math.IsNaN(aprPct) || math.IsInf(aprPct, 0) || notional <= 0 || holdMS <= 0 {
		return 0
	}
	return notional * aprPct / 100 * float64(holdMS) / (365 * 86400000)
}

func (o *OrderMgr) localExit(od *ormo.InOutOrder, exitAt int64, tag string, price float64, msg, odType string) *errs.Error {
	if o == nil || !o.runtimeDeps {
		return od.LocalExit(exitAt, tag, price, msg, odType)
	}
	if price == 0 {
		price = o.priceSafeExp(od.Symbol, "", com.Day10MSecs)
		if price <= 0 {
			if od.Enter.Average > 0 {
				price = od.Enter.Average
			} else if od.Enter.Price > 0 {
				price = od.Enter.Price
			} else {
				price = od.InitPrice
			}
		}
	}
	if exitAt == 0 {
		exitAt = o.priceNow()
	}
	if od.Enter.Status < ormo.OdStatusClosed {
		od.Enter.Status = ormo.OdStatusClosed
		if err := o.updateOrderFee(od, price, true); err != nil {
			return err
		}
		od.DirtyEnter = true
	}
	if odType == "" {
		odType = banexg.OdTypeMarket
	}
	o.setExit(od, exitAt, tag, odType, price)
	od.Exit.Status = ormo.OdStatusClosed
	od.Exit.Filled = od.Enter.Filled
	od.Exit.Average = od.Exit.Price
	od.Status = ormo.InOutStatusFullExit
	if err := o.updateOrderFee(od, price, false); err != nil {
		return err
	}
	od.UpdateProfits(price)
	od.DirtyMain = true
	od.DirtyExit = true
	if msg != "" {
		od.SetInfo(ormo.KeyStatusMsg, msg)
	}
	return od.Save()
}

func (o *OrderMgr) bindRuntimeDeps(deps RuntimeDeps) {
	o.runtimeDeps = true
	o.runtimeCore = deps.Core
	if o.runtimeCore == nil {
		o.runtimeCore, _ = core.NewState(nil)
	}
	o.clock = deps.Clock
	o.exchange = deps.Exchange
	o.dump = deps.Dump
	o.symbols = deps.Symbols
	if deps.Market != nil {
		o.prices = deps.Market.Prices
	}
	if o.clock == nil {
		backtest := o.runtimeCore.BackTestMode
		o.clock = btime.NewClockState(backtest, nil)
	}
	if o.prices == nil {
		exgName := o.runtimeCore.ExgName
		o.prices = com.NewPriceStateWithExchange(exgName, deps.Exchange)
	}
	deps.Core = o.runtimeCore
	deps.Clock = o.clock
	if deps.Orders != nil {
		deps.Orders.SetLive(o.runtimeCore.LiveMode)
	}
	if deps.Market == nil {
		deps.Market = &com.MarketState{Prices: o.prices}
	} else if deps.Market.Prices == nil {
		deps.Market.Prices = o.prices
	}
	o.walletDeps = deps
	o.runtimeCfg = makeRuntimeOrderConfig(deps, o.Account)
	if deps.Trading != nil {
		o.wallet = deps.Trading.Wallet(o.Account)
	} else {
		o.wallet = getRuntimeWallets(o.Account)
	}
	o.wallet.bindRuntimeDeps(deps)
}

func makeRuntimeOrderConfig(deps RuntimeDeps, account string) runtimeOrderConfig {
	result := runtimeOrderConfig{}
	if deps.Config == nil {
		return result
	}
	cfg := deps.Config.View()
	if cfg == nil {
		return result
	}
	result.stakeCurrency = cfg.StakeCurrency
	result.takeOverStrategy = cfg.TakeOverStrat
	result.orderType = cfg.OrderType
	result.limitVolSecs = cfg.LimitVolSecs
	if result.limitVolSecs == 0 {
		result.limitVolSecs = 10
	}
	result.putLimitSecs = cfg.PutLimitSecs
	if result.putLimitSecs == 0 {
		result.putLimitSecs = 180
	}
	result.orderBookTTL = cfg.OdBookTtl
	if result.orderBookTTL == 0 {
		result.orderBookTTL = 500
	}
	result.stopEnterBars = cfg.StopEnterBars
	result.maxOpenOrders = cfg.MaxOpenOrders
	result.maxSimulOpen = cfg.MaxSimulOpen
	result.backtestNetCost = cfg.BTNetCost
	if result.backtestNetCost == 0 {
		result.backtestNetCost = 15
	}
	result.legacyIntrabar = cfg.BTLegacyIntrabar
	result.slippageBps = cfg.BTSlippageBps
	result.gapBars = cfg.BTGapBars
	result.gapMinHours = cfg.BTGapMinHours
	result.delistPenaltyBps = cfg.BTDelistPenaltyBps
	result.fundingApr = cfg.BTFundingApr
	result.legacyApproxFunding = cfg.BTLegacyApproxFunding
	result.accountLeverage = cfg.Leverage
	if acc := cfg.Accounts[account]; acc != nil {
		if acc.Leverage > 0 {
			result.accountLeverage = acc.Leverage
		}
		if acc.MaxOpenOrders > 0 {
			result.accountMaxOpenOrders = acc.MaxOpenOrders
		}
	}
	return result
}

func (o *OrderMgr) orderState() *ormo.OrderState {
	if o != nil && o.runtimeDeps {
		return o.walletDeps.Orders
	}
	return nil
}

func (o *OrderMgr) fireOdChange(od *ormo.InOutOrder, evt int) {
	if o != nil && o.runtimeDeps {
		strat.FireOdChangeWithState(o.walletDeps.Strategies, o.Account, od, evt)
		return
	}
	strat.FireOdChange(o.Account, od, evt)
}

func (o *OrderMgr) openOrders() (map[int64]*ormo.InOutOrder, *deadlock.Mutex) {
	if state := o.orderState(); state != nil {
		return state.GetOpenODs(o.Account)
	}
	return ormo.GetOpenODs(o.Account)
}

func (o *OrderMgr) taskID() int64 {
	if state := o.orderState(); state != nil {
		return state.GetTaskID(o.Account)
	}
	return ormo.GetTaskID(o.Account)
}

func (o *OrderMgr) taskAccount(taskID int64) string {
	if state := o.orderState(); state != nil {
		return state.GetTaskAcc(taskID)
	}
	return ormo.GetTaskAcc(taskID)
}

// exSymbolCur resolves through the manager-owned symbol catalog for typed
// runtimes. Legacy managers retain the package facade for compatibility.
func (o *OrderMgr) exSymbolCur(symbol string) (*orm.ExSymbol, *errs.Error) {
	if o != nil && o.runtimeDeps {
		if o.symbols == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "runtime symbol state is required")
		}
		return o.symbols.GetExSymbolCur(symbol)
	}
	return orm.GetExSymbolCur(symbol)
}

// exSymbolMap resolves through the manager-owned symbol catalog for typed
// runtimes. A nil map is intentional when the required runtime state is
// absent: callers then return their normal unknown-symbol error.
func (o *OrderMgr) exSymbolMap() map[string]*orm.ExSymbol {
	if o != nil && o.runtimeDeps {
		if o.symbols == nil {
			return nil
		}
		return o.symbols.GetExSymbolMap(o.exchangeName(), o.marketType())
	}
	return orm.GetExSymbolMap(o.exchangeName(), o.marketType())
}

func (o *OrderMgr) priceSymbolParts(symbol string) ([4]string, *errs.Error) {
	if o != nil && o.runtimeDeps {
		return exg.ResolveRuntimePriceSymbol(o.exchangeClient(), symbol)
	}
	return exg.ResolvePriceSymbol(exg.Default, symbol)
}

func (o *OrderMgr) walletsForOrder() *BanWallets {
	if o != nil && o.runtimeDeps {
		if o.wallet == nil {
			if o.walletDeps.Trading != nil {
				o.wallet = o.walletDeps.Trading.Wallet(o.Account)
			} else {
				o.wallet = getRuntimeWallets(o.Account)
			}
			o.wallet.bindRuntimeDeps(o.walletDeps)
		}
		return o.wallet
	}
	return GetWallets(o.Account)
}

func (o *OrderMgr) executionDeps() *RuntimeDeps {
	if o.runtimeDeps {
		return &o.walletDeps
	}
	return nil
}

func (o *OrderMgr) refineTimeFrame(strategy, timeframe string) string {
	if o.runtimeDeps {
		if o.walletDeps.Strategies != nil {
			return o.walletDeps.Strategies.RefineTimeFrame(strategy, timeframe)
		}
		return timeframe
	}
	refined, _ := config.GetStratRefineTF(strategy, timeframe)
	return refined
}

func seriesEndMSForMatch(evt *orm.DataSeries) int64 {
	if evt == nil {
		return 0
	}
	if evt.EndMS > evt.TimeMS {
		return evt.EndMS
	}
	if evt.TimeFrame != "" {
		return evt.TimeMS + int64(utils.TFToSecs(evt.TimeFrame))*1000
	}
	return evt.TimeMS
}

func (o *OrderMgr) recordSeriesObserved(symbol, tf string, endMS int64) {
	if o == nil || symbol == "" || tf == "" || endMS <= 0 {
		return
	}
	key := symbol + "\x00" + tf
	o.seenSeriesMu.Lock()
	if o.seenSeriesEndMS == nil {
		o.seenSeriesEndMS = make(map[string]int64)
	}
	if endMS > o.seenSeriesEndMS[key] {
		o.seenSeriesEndMS[key] = endMS
	}
	o.seenSeriesMu.Unlock()
}

func (o *OrderMgr) lastObservedSeriesEndMS(symbol, tf string) int64 {
	if o == nil || symbol == "" || tf == "" {
		return 0
	}
	key := symbol + "\x00" + tf
	o.seenSeriesMu.RLock()
	endMS := o.seenSeriesEndMS[key]
	o.seenSeriesMu.RUnlock()
	return endMS
}

// effectiveMatchTimeFrame resolves the sub-bar matching timeframe for od, gracefully
// degrading to od.Timeframe on a coarse bar in backtests ONLY when no finer refine_tf
// sub-bar was delivered inside (evt.TimeMS, evtEndMS] (e.g. synthetic coarse-only tests).
func (o *OrderMgr) effectiveMatchTimeFrame(od *ormo.InOutOrder, evt *orm.DataSeries) string {
	if od == nil {
		return ""
	}
	matchTf := o.refineTimeFrame(od.Strategy, od.Timeframe)
	if matchTf == "" {
		matchTf = od.Timeframe
	}
	if evt == nil || matchTf == od.Timeframe || evt.TimeFrame == matchTf {
		return matchTf
	}
	if evt.TimeFrame == od.Timeframe && o.isBacktest() {
		subSecs := utils.TFToSecs(matchTf)
		mainSecs := utils.TFToSecs(od.Timeframe)
		if subSecs > 0 && subSecs < mainSecs && o.lastObservedSeriesEndMS(od.Symbol, matchTf) <= evt.TimeMS {
			return od.Timeframe
		}
	}
	return matchTf
}

func (o *OrderMgr) syncJobOrders(job *strat.StratJob) {
	if o == nil || job == nil {
		return
	}
	openOds, lock := o.openOrders()
	lock.Lock()
	curOrders := executionOpenOrders(openOds, o.executionDeps())
	lock.Unlock()
	pending := job.PendingEntryCount()
	job.UpdateOrders(curOrders)
	if pending > 0 {
		job.AddOrderCount(pending)
	}
}

func (o *OrderMgr) ensureLatestPrice(symbol string) *errs.Error {
	if o == nil || !o.runtimeDeps {
		return com.EnsureLatestPrice(symbol)
	}
	if o.priceSafeExp(symbol, "", com.PriceExpireMS) > 0 {
		return nil
	}
	return o.prices.RefreshLatestPriceAt(o.priceNow(), o.exchange, symbol)
}

func (o *OrderMgr) legacyIntrabarEnabled() bool {
	if o != nil && o.runtimeDeps {
		return o.isBacktest() && o.runtimeCfg.legacyIntrabar
	}
	return legacyIntrabarEnabled()
}

func GetOdMgr(account string) IOrderMgr {
	if !core.EnvReal {
		account = config.DefAcc
	}
	val, _ := accOdMgrs[account]
	return val
}

// GetOdMgrWithState resolves an account manager from an explicit runtime
// registry. It is the typed counterpart to the legacy package facade.
func GetOdMgrWithState(state *TradingState, account string) IOrderMgr {
	if state == nil {
		return nil
	}
	return state.OrderManager(account)
}

func GetAllOdMgrWithState(state *TradingState) map[string]IOrderMgr {
	if state == nil {
		return nil
	}
	return state.OrderManagersSnapshot()
}

func GetAllOdMgr() map[string]IOrderMgr {
	var result = make(map[string]IOrderMgr)
	if core.EnvReal {
		for acc, mgr := range accLiveOdMgrs {
			result[acc] = mgr
		}
	} else {
		for acc, mgr := range accOdMgrs {
			result[acc] = mgr
		}
	}
	return result
}

func GetLiveOdMgr(account string) *LiveOrderMgr {
	if !core.EnvReal {
		panic("call GetLiveOdMgr in FakeEnv is forbidden: " + core.RunEnv)
	}
	val, _ := accLiveOdMgrs[account]
	return val
}

// GetLiveOdMgrWithState resolves a live manager from an explicit runtime.
func GetLiveOdMgrWithState(state *TradingState, account string) *LiveOrderMgr {
	if state == nil {
		return nil
	}
	return state.LiveManager(account)
}

func CleanUpOdMgr() *errs.Error {
	var err *errs.Error
	for account := range executionAccountNames() {
		var curErr *errs.Error
		if core.EnvReal {
			if mgr, ok := accLiveOdMgrs[account]; ok {
				curErr = mgr.CleanUp()
			}
		} else {
			if mgr, ok := accOdMgrs[account]; ok {
				curErr = mgr.CleanUp()
			}
		}
		if curErr != nil {
			if err != nil {
				log.Error("clean odMgr fail", zap.String("acc", account), zap.Error(curErr))
			} else {
				err = curErr
			}
		}
	}
	return err
}

// CleanUpOdMgrWithState closes only managers owned by an explicit runtime.
// The legacy function above remains the serialized compatibility path.
func CleanUpOdMgrWithState(state *TradingState) *errs.Error {
	if state == nil {
		return nil
	}
	managers := state.OrderManagersSnapshot()
	accounts := slices.Sorted(maps.Keys(managers))
	var firstErr *errs.Error
	for _, account := range accounts {
		manager := managers[account]
		if manager == nil {
			continue
		}
		if currentErr := manager.CleanUp(); currentErr != nil {
			if firstErr == nil {
				firstErr = currentErr
			} else {
				log.Error("clean runtime odMgr fail", zap.String("acc", account), zap.Error(currentErr))
			}
		}
	}
	return firstErr
}

func (o *OrderMgr) allowOrderEnter(exs *orm.ExSymbol, tf string, enters []*strat.EnterReq) ([]*strat.EnterReq, map[string]int) {
	openOds, lock := o.openOrders()
	lock.Lock()
	openNum := len(openOds)
	stratOdNum := make(map[string]int)
	for _, od := range openOds {
		stratOdNum[od.Strategy]++
	}
	lock.Unlock()
	return o.allowOrderEnterWithCounts(exs, tf, enters, openNum, stratOdNum, nil, 0)
}

// allowOrderEnterWithCounts shares the admission rules with the account-owned
// bridge. That path supplies committed counts instead of a Runtime projection.
func (o *OrderMgr) allowOrderEnterWithCounts(exs *orm.ExSymbol, tf string, enters []*strat.EnterReq, openNum int, stratOdNum map[string]int, policies map[string]*config.RunPolicyConfig, barStopMS int64) ([]*strat.EnterReq, map[string]int) {
	curMS := o.priceNow()
	rawNum := len(enters)
	if o.pairIsBanned(exs.Symbol, curMS) {
		return nil, map[string]int{"BanPair": rawNum}
	}
	if o.runMode() == core.RunModeOther {
		// Does not involve order mode, prohibit opening orders
		// 不涉及订单模式，禁止开单
		return nil, map[string]int{"NoOrderMode": rawNum}
	}
	pairZapField := zap.String("pair", exs.Symbol)
	stopUntil, _ := o.noEnterUntilFor(o.Account)
	if curMS < stopUntil {
		if o.isLive() {
			o.Logger().Warn("any enter forbid", pairZapField)
		}
		o.addAccFailOpens(strat.FailOpenNoEntry, len(enters))
		return nil, map[string]int{"AccNoEntry": rawNum}
	}
	if barStopMS == 0 {
		tfMSecs := int64(utils.TFToSecs(tf) * 1000)
		barStopMS = utils.AlignTfMSecs(curMS, tfMSecs)
	}
	if o.BarMS < barStopMS {
		o.BarMS = barStopMS
		o.simulOpen = 0
		o.simulOpenSt = make(map[string]int)
	}
	maxOpenNum := o.maxOpenOrders()
	orgNum := len(enters)
	enters = o.checkOrderNum(enters, openNum, maxOpenNum, "max_open_orders")
	if maxSimulOpen := o.maxSimulOpen(); len(enters) > 0 && maxSimulOpen > 0 {
		enters = o.checkOrderNum(enters, o.simulOpen, maxSimulOpen, "max_simul_open")
	}
	if orgNum > len(enters) {
		o.addAccFailOpens(strat.FailOpenNumLimit, orgNum-len(enters))
	}
	if len(enters) == 0 {
		return nil, map[string]int{"OpenTooMuch": rawNum}
	}
	numCut := rawNum - len(enters)
	tagMap := map[string]int{}
	if numCut > 0 {
		tagMap["OpenTooMuch"] = numCut
	}
	// Check whether the maximum number of orders opened by the strategy is exceeded
	// 检查是否超出策略最大开单数量
	skipNum := 0
	res := make([]*strat.EnterReq, 0, len(enters))
	for _, req := range enters {
		num, _ := stratOdNum[req.StratName]
		simulNum, _ := o.simulOpenSt[req.StratName]
		// Runtime-owned strategy registries do not necessarily populate the
		// legacy global PairStrats map. Missing metadata must not turn an
		// otherwise valid request into a nil-pointer panic; common account and
		// strategy-name limits above still apply.
		var pol *config.RunPolicyConfig
		var stgy *strat.TradeStrat
		if o.runtimeDeps {
			if o.walletDeps.Strategies != nil {
				stgy = o.walletDeps.Strategies.Get(exs.Symbol, req.StratName)
			}
		} else {
			stgy = strat.Get(exs.Symbol, req.StratName)
		}
		if stgy != nil {
			pol = stgy.Policy
		}
		if declared := policies[req.StratName]; declared != nil {
			pol = declared
		}
		if pol != nil {
			if pol.MaxOpen > 0 && num >= pol.MaxOpen {
				skipNum += 1
				continue
			}
			if pol.MaxSimulOpen > 0 && simulNum >= pol.MaxSimulOpen {
				skipNum += 1
				continue
			}
		}
		stratOdNum[req.StratName] = num + 1
		o.simulOpenSt[req.StratName] = simulNum + 1
		o.simulOpen += 1
		res = append(res, req)
	}
	if skipNum > 0 {
		o.addAccFailOpens(strat.FailOpenNumLimitPol, skipNum)
	}
	numCut = rawNum - len(enters)
	if numCut > 0 {
		tagMap["OpenTooMuch"] = numCut
	}
	return res, tagMap
}

func (o *OrderMgr) addAccFailOpens(tag string, num int) {
	if num <= 0 {
		return
	}
	if o != nil && o.runtimeDeps && o.walletDeps.Strategies != nil {
		o.walletDeps.Strategies.AddAccFailOpens(o.Account, tag, num)
		return
	}
	strat.AddAccFailOpens(o.Account, tag, num)
}

func (o *OrderMgr) checkOrderNum(enters []*strat.EnterReq, oldNum, maxNum int, tag string) []*strat.EnterReq {
	return checkOrderNumWithLive(enters, oldNum, maxNum, tag, o.isLive())
}

func checkOrderNum(enters []*strat.EnterReq, oldNum, maxNum int, tag string) []*strat.EnterReq {
	return checkOrderNumWithLive(enters, oldNum, maxNum, tag, core.LiveMode)
}

func checkOrderNumWithLive(enters []*strat.EnterReq, oldNum, maxNum int, tag string, live bool) []*strat.EnterReq {
	cutNum := oldNum + len(enters) - maxNum
	if maxNum > 0 && cutNum > 0 {
		if maxNum > oldNum {
			enters = enters[:maxNum-oldNum]
			if live {
				log.Warn("cut enters by", zap.String("tag", tag),
					zap.Int("left", len(enters)), zap.Int("cut", cutNum))
			}
		} else {
			enters = nil
			if live {
				log.Warn("skip enters by", zap.String("tag", tag), zap.Int("cut", cutNum))
			}
		}
	}
	return enters
}

/*
ProcessOrders
Execute order entry and exit requests
Create pending orders, the returned orders are not actually entered or exited;
Backtest: the caller executes the entry/exit order according to the next bar and updates the status
Live trading: monitor the exchange to return the order status to update the entry and exit
执行订单入场出场请求
创建待执行订单，返回的订单实际并未入场或出场；
回测：调用方根据下一个bar执行入场/出场订单，更新状态
实盘：监听交易所返回订单状态更新入场出场
*/
func (o *OrderMgr) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	if job == nil || !job.BeginOrderProcessing() {
		return nil, nil, nil
	}
	finished := false
	defer func() {
		if !finished {
			// Error paths leave the active drain so a later caller can retry. The
			// normal empty-queue path uses finishOrderProcessing below, which
			// atomically closes the producer hand-off race.
			job.EndOrderProcessing()
		}
	}()
	var entOrders, extOrders []*ormo.InOutOrder
	exs := job.Symbol
	for {
		enters, exits := job.DrainOrderRequests()
		if len(enters) == 0 && len(exits) == 0 {
			if job.FinishOrderProcessing() {
				finished = true
				break
			}
			continue
		}
		var batchEntOrders, batchExtOrders []*ormo.InOutOrder
		if len(enters) > 0 {
			rawNum := len(enters)
			logRejected := o.isLive()
			for _, req := range enters {
				logRejected = logRejected || req.Log
			}
			var reasons map[string]int
			enters, reasons = o.allowOrderEnter(exs, job.TimeFrame, enters)
			if logRejected && len(enters) < rawNum {
				o.Logger().Warn("skip enters by allowOrderEnter", zap.Any("tags", reasons))
			}
			for _, ent := range enters {
				iorder, err := o.enterOrder(exs, job.TimeFrame, ent, false)
				if err != nil {
					return entOrders, extOrders, err
				}
				batchEntOrders = append(batchEntOrders, iorder)
				entOrders = append(entOrders, iorder)
			}
		}
		if len(exits) > 0 {
			for _, exit := range exits {
				iorders, err := o.ExitOpenOrders(exs.Symbol, exit)
				if err != nil {
					return entOrders, extOrders, err
				}
				batchExtOrders = append(batchExtOrders, iorders...)
				extOrders = append(extOrders, iorders...)
			}
		}
		if len(batchEntOrders) > 0 || len(batchExtOrders) > 0 {
			o.syncJobOrders(job)
		}
		if job.Strat.OnOrderChange != nil && (len(batchEntOrders) > 0 || len(batchExtOrders) > 0) {
			for _, od := range batchEntOrders {
				job.Strat.OnOrderChange(job, od, strat.OdChgEnter)
			}
			for _, od := range batchExtOrders {
				job.Strat.OnOrderChange(job, od, strat.OdChgExit)
			}
		}
	}
	if len(entOrders) > 0 || len(extOrders) > 0 {
		o.syncJobOrders(job)
	}
	return entOrders, extOrders, nil
}

func (o *LocalOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {

}

func (o *OrderMgr) RelayOrders(orders []*ormo.InOutOrder) *errs.Error {
	symbolMap := o.exSymbolMap()
	taskId := o.taskID()
	for _, odr := range orders {
		exs, ok := symbolMap[odr.Symbol]
		if !ok {
			return errs.NewMsg(errs.CodeNoMarketForPair, "%s not found", odr.Symbol)
		}
		price := o.priceExp(odr.Symbol, odr.Enter.Side, com.Day10MSecs)
		curTime := o.priceNow()
		od := &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				TaskID:    taskId,
				Symbol:    odr.Symbol,
				Sid:       int64(exs.ID),
				Timeframe: odr.Timeframe,
				Short:     odr.Short,
				Status:    odr.Status,
				EnterTag:  odr.EnterTag,
				InitPrice: odr.InitPrice,
				Stop:      odr.Stop,
				QuoteCost: odr.QuoteCost,
				ExitTag:   odr.ExitTag,
				Leverage:  odr.Leverage,
				EnterAt:   odr.EnterAt,
				ExitAt:    odr.ExitAt,
				Strategy:  odr.Strategy,
				StgVer:    odr.StgVer,
				Info:      odr.IOrder.Info,
				// ignore: MaxPftRate,MaxDrawDown,Profit,ProfitRate
			},
			Enter: &ormo.ExOrder{
				TaskID:    taskId,
				Symbol:    odr.Symbol,
				Enter:     true,
				OrderType: odr.Enter.OrderType,
				//OrderID:   odr.Enter.OrderID,
				Side:     odr.Enter.Side,
				CreateAt: curTime,
				UpdateAt: curTime,
				Price:    price,
				Amount:   odr.Enter.Amount,
				Status:   ormo.OdStatusInit,
			},
			Info:       make(map[string]interface{}),
			OCOGroup:   odr.OCOGroup,
			DirtyMain:  true,
			DirtyEnter: true,
		}
		if state := o.orderState(); state != nil {
			od.BindState(state)
		}
		if odr.Exit != nil && odr.Exit.Filled > 0 {
			od.Enter.Amount -= odr.Exit.Filled
			od.QuoteCost = od.Enter.Price * od.Enter.Amount
		}
		if len(odr.Info) > 0 {
			maps.Copy(od.Info, odr.Info)
		}
		err := od.Save()
		if err == nil {
			if o.afterEnter != nil {
				err = o.afterEnter(od)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OrderMgr) EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error) {
	return o.enterOrder(exs, tf, req, true)
}

func (o *OrderMgr) enterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq, doCheck bool) (*ormo.InOutOrder, *errs.Error) {
	isSpot := o.marketType() == banexg.MarketSpot
	if req.Short && isSpot {
		return nil, errs.NewMsg(core.ErrRunTime, "short oder is invalid for spot")
	}
	if doCheck {
		enters, reasons := o.allowOrderEnter(exs, tf, []*strat.EnterReq{req})
		if len(enters) == 0 {
			o.Logger().Warn("skip enter by allowOrderEnter", zap.Any("reasons", reasons))
			return nil, nil
		}
	}
	if req.Leverage == 0 {
		req.Leverage = 1
		if !isSpot {
			exchange := o.exchangeClient()
			if exchange != nil {
				exInfo := exchange.Info()
				if exInfo != nil && exInfo.FixedLvg {
					req.Leverage, _ = exchange.GetLeverage(exs.Symbol, 0, o.Account)
				} else {
					req.Leverage = o.accountLeverage()
				}
			}
		}
	}
	stgVer := 0
	if o.runtimeDeps && o.walletDeps.Strategies != nil {
		stgVer, _ = o.walletDeps.Strategies.Version(req.StratName)
	} else {
		stgVer, _ = strat.GetVersion(req.StratName)
	}
	odSide := banexg.OdSideBuy
	if req.Short {
		odSide = banexg.OdSideSell
	}
	if o.isLive() {
		err := o.ensureLatestPrice(exs.Symbol)
		if err != nil {
			return nil, err
		}
	}
	price := o.priceSafeExp(exs.Symbol, odSide, com.PriceExpireMS)
	if price < 0 {
		return nil, errs.NewMsg(errs.CodeRunTime, "no valid price: %v", exs.Symbol)
	}
	legacyIntrabar := o.legacyIntrabarEnabled()
	if legacyEntryStopAlreadyCrossedWith(req.Short, req.Stop, price, legacyIntrabar) {
		req.Stop = 0
	}
	enterPrice := entryInitPriceWith(req.Short, req.Stop, req.Limit, price, legacyIntrabar)
	curTimeMS := o.priceNow()
	taskId := o.taskID()
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Sid:       int64(exs.ID),
			Timeframe: tf,
			Short:     req.Short,
			Status:    ormo.InOutStatusInit,
			EnterTag:  req.Tag,
			InitPrice: enterPrice,
			Stop:      req.Stop,
			Leverage:  req.Leverage,
			EnterAt:   curTimeMS,
			Strategy:  req.StratName,
			StgVer:    int64(stgVer),
		},
		Enter: &ormo.ExOrder{
			TaskID:    taskId,
			Symbol:    exs.Symbol,
			Enter:     true,
			OrderType: core.OrderTypeEnums[req.OrderType],
			Side:      odSide,
			Price:     req.Limit,
			Amount:    req.Amount,
			Status:    ormo.OdStatusInit,
			CreateAt:  curTimeMS,
			UpdateAt:  curTimeMS,
		},
		Info:       map[string]interface{}{},
		OCOGroup:   req.OCOGroup,
		DirtyMain:  true,
		DirtyEnter: true,
	}
	if state := o.orderState(); state != nil {
		od.BindState(state)
	}
	if req.Limit > 0 && req.OrderType == core.OrderTypeEmpty {
		od.Enter.OrderType = banexg.OdTypeLimit
	}
	if od.Enter.OrderType == "" {
		od.Enter.OrderType = o.orderType()
	}
	if req.Limit > 0 {
		if req.Stop == 0 {
			od.InitPrice = req.Limit
		}
		if req.StopBars == 0 {
			req.StopBars = o.stopEnterBars()
		}
		if req.StopBars > 0 {
			stopAfter := o.priceNow() + int64(req.StopBars*utils.TFToSecs(od.Timeframe))*1000
			od.SetInfo(ormo.OdInfoStopAfter, stopAfter)
			od.SetInfo(ormo.OdInfoStopBars, req.StopBars)
		}
	}
	if req.OCOGroup != "" {
		od.SetInfo(ormo.OdInfoOCOGroup, req.OCOGroup)
	}
	od.SetInfo(ormo.OdInfoLegalCost, req.LegalCost)
	if req.CallbackPct > 0 {
		od.SetInfo(ormo.OdInfoCallbackPct, req.CallbackPct)
		if req.ActivationPrice > 0 {
			od.SetInfo(ormo.OdInfoActivePrice, req.ActivationPrice)
		}
	}
	if req.StopLoss > 0 {
		err := od.SetExitTrigger(ormo.OdInfoStopLoss, &ormo.ExitTrigger{
			Price: req.StopLoss,
			Limit: req.StopLossLimit,
			Rate:  req.StopLossRate,
			Tag:   req.StopLossTag,
		}, enterPrice)
		if err != nil {
			return od, err
		}
	}
	if len(req.TakeProfits) > 0 {
		rungs := make([]*ormo.TPLadderRung, len(req.TakeProfits))
		for idx, tp := range req.TakeProfits {
			exitRate := tp.ExitRate
			if exitRate == 0 && len(req.TakeProfits) == 1 {
				exitRate = 1
			}
			rungs[idx] = &ormo.TPLadderRung{
				Price:             tp.Price,
				Val:               tp.Val,
				Rate:              tp.Rate,
				Limit:             tp.Limit,
				ExitRate:          exitRate,
				MoveSLToBreakeven: tp.MoveSLToBreakeven,
				Tag:               tp.Tag,
			}
		}
		od.SetTakeProfits(rungs)
	} else if req.TakeProfit > 0 {
		err := od.SetExitTrigger(ormo.OdInfoTakeProfit, &ormo.ExitTrigger{
			Price: req.TakeProfit,
			Limit: req.TakeProfitLimit,
			Rate:  req.TakeProfitRate,
			Tag:   req.TakeProfitTag,
		}, enterPrice)
		if err != nil {
			return od, err
		}
	}
	if req.ClientID != "" {
		od.SetInfo(ormo.OdInfoClientID, req.ClientID)
	}
	if len(req.Infos) > 0 {
		for k, v := range req.Infos {
			od.SetInfo(k, v)
		}
	}
	err := od.Save()
	if err != nil {
		return od, err
	}
	if o.afterEnter != nil {
		err = o.afterEnter(od)
	}
	return od, err
}

func legacyEntryStopAlreadyCrossed(short bool, stop, price float64) bool {
	return legacyEntryStopAlreadyCrossedWith(short, stop, price, legacyIntrabarEnabled())
}

func legacyEntryStopAlreadyCrossedWith(short bool, stop, price float64, legacyIntrabar bool) bool {
	if !legacyIntrabar || stop <= 0 {
		return false
	}
	if short {
		return price <= stop
	}
	return price > stop
}

func entryInitPrice(short bool, stop, limit, price float64) float64 {
	return entryInitPriceWith(short, stop, limit, price, legacyIntrabarEnabled())
}

func entryInitPriceWith(short bool, stop, limit, price float64, legacyIntrabar bool) float64 {
	if !legacyIntrabar {
		if short && stop > 0 && stop < price {
			if limit > stop {
				return limit
			}
			return stop
		}
		if !short && stop > price {
			if limit > 0 && limit < stop {
				return limit
			}
			return stop
		}
	}
	if short && limit > price {
		return limit
	}
	if !short && limit > 0 && limit < price {
		return limit
	}
	return price
}

func (o *OrderMgr) ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error) {
	// Filter matching orders 筛选匹配的订单
	var matches []*ormo.InOutOrder
	openOds, lock := o.openOrders()
	if req.OrderID > 0 {
		// Specify the exact order ID to exit 精确指定退出的订单ID
		lock.Lock()
		od, ok := openOds[req.OrderID]
		lock.Unlock()
		if !ok {
			return nil, errs.NewMsg(errs.CodeParamInvalid, "req orderId not found: %d", req.OrderID)
		}
		req.Force = true
		matches = append(matches, od)
	} else {
		parts := strings.Split(pairs, ",")
		pairMap := make(map[string]bool)
		for _, p := range parts {
			if p == "" {
				continue
			}
			pairMap[p] = true
		}
		dirtBoth := req.Dirt == core.OdDirtBoth
		isShort := req.Dirt == core.OdDirtShort
		lock.Lock()
		for _, od := range openOds {
			if req.StratName != "" && od.Strategy != req.StratName {
				continue
			}
			if len(pairMap) > 0 {
				if _, ok := pairMap[od.Symbol]; !ok {
					continue
				}
			}
			if !dirtBoth && isShort != od.Short {
				continue
			}
			if req.EnterTag != "" && od.EnterTag != req.EnterTag {
				continue
			}
			if od.ExitTag != "" || (od.Exit != nil && od.Exit.Amount > 0) {
				// Order Exited 订单已退出
				continue
			}
			if req.UnFillOnly && od.Enter.Filled >= od.Enter.Amount {
				continue
			}
			if req.FilledOnly && od.Enter.Filled < core.AmtDust {
				continue
			}
			matches = append(matches, od)
		}
		lock.Unlock()
	}
	if len(matches) == 0 {
		if o.isLive() {
			fields := req.GetZapFields(nil, zap.String("acc", o.Account), zap.String("pair", pairs),
				zap.Int("all", len(openOds)))
			o.Logger().Warn("no match orders to exit", fields...)
		}
		return nil, nil
	}
	var exitAmount float64
	useRate := req.ExitRate > 0 && req.ExitRate < 1
	if useRate || req.Amount <= 0 {
		// Calculate the amount to withdraw 计算要退出的数量
		allAmount := float64(0)
		for _, od := range matches {
			allAmount += od.Enter.Amount
			if od.Exit != nil {
				allAmount -= od.Exit.Amount
			}
		}
		exitAmount = allAmount
		if useRate {
			exitAmount = allAmount * req.ExitRate
		}
	} else {
		exitAmount = req.Amount
	}
	isTakeProfit := false
	if req.Limit > 0 && core.IsLimitOrder(req.OrderType) {
		symbol := matches[0].Symbol
		for _, od := range matches[1:] {
			if od.Symbol != symbol {
				return nil, errs.NewMsg(errs.CodeParamInvalid, "ExitReq.Limit invalid for multi pairs")
			}
		}
		odSide := ""
		if req.Dirt == core.OdDirtLong {
			odSide = banexg.OdSideSell
		} else if req.Dirt == core.OdDirtShort {
			odSide = banexg.OdSideBuy
		}
		price := o.priceExp(symbol, odSide, com.Day10MSecs)
		if price > 0 && (req.Limit-price)*float64(req.Dirt) > 0 {
			isTakeProfit = true
		}
	}
	slices.SortFunc(matches, func(a, b *ormo.InOutOrder) int {
		return compareExitOpenOrders(a, b, isTakeProfit || req.FilledOnly)
	})
	var result []*ormo.InOutOrder
	var part *ormo.InOutOrder
	var err *errs.Error
	for i, od := range matches {
		if !req.Force && !o.canClose(od) {
			continue
		}
		dust := od.Enter.Amount * 0.01
		if exitAmount < dust {
			if isTakeProfit {
				// reset TakeProfit for remaining orders
				// 剩余订单重置TakeProfit
				for _, odr := range matches[i:] {
					err = odr.SetTakeProfit(nil)
					if err != nil {
						return result, err
					}
					_, err = o.postOrderExit(odr)
					if err != nil {
						return result, err
					}
				}
			}
			break
		}
		if req.FilledOnly && od.Enter.Filled < od.Enter.Amount {
			// Only exit the entered orders, the current order is partially entered and divided into sub-orders
			// 只退出已入场的订单，当前订单部分入场，切分成子订单
			cutAmt := min(exitAmount, od.Enter.Filled)
			part = od.CutPart(cutAmt, 0)
			err = od.Save()
			if err != nil {
				return result, err
			}
			od = part
		}
		q := req.Clone()
		q.ExitRate = min(1, exitAmount/od.Enter.Amount)
		if isTakeProfit && od.Status >= ormo.InOutStatusPartEnter {
			err = od.SetTakeProfit(&ormo.ExitTrigger{
				Price: q.Limit,
				Limit: q.Limit,
				Rate:  q.ExitRate,
				Tag:   q.Tag,
			})
			if err != nil {
				return result, err
			}
			part, err = o.postOrderExit(od)
		} else {
			part, err = o.exitOrder(od, q)
		}
		if err != nil {
			return result, err
		}
		if part != nil {
			exitAmount -= part.Enter.Amount * q.ExitRate
			result = append(result, part)
		}
	}
	return result, nil
}

func compareExitOpenOrders(a, b *ormo.InOutOrder, preferFilled bool) int {
	fillA := a.Enter.Filled * a.InitPrice
	fillB := b.Enter.Filled * b.InitPrice
	fillChg := cmp.Compare(math.Round(fillA*100), math.Round(fillB*100))
	// For profit taking or filled only, descending order by filled amount.
	// 对于止盈或退出已入场的，优先按已入场金额降序
	if preferFilled && fillChg != 0 {
		// 止盈单，优先按入场金额倒序
		return -fillChg
	}
	costA := a.Enter.Amount * a.InitPrice
	unfillA := costA - fillA
	costB := b.Enter.Amount * b.InitPrice
	unfillB := costB - fillB
	// First, in descending order by unsold amount. 首先按未成交金额倒序
	res := cmp.Compare(math.Round(unfillB*100), math.Round(unfillA*100))
	if res != 0 {
		return res
	}
	// Secondly, in ascending order by deposit amount 其次按已入场金额升序
	if fillChg != 0 {
		return fillChg
	}
	// Last entry time ascending, tie-break by ID for determinism
	enterDiff := a.RealEnterMS() - b.RealEnterMS()
	if enterDiff < 0 {
		return -1
	}
	if enterDiff > 0 {
		return 1
	}
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	return 0
}

func (o *OrderMgr) ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	if od.ExitTag != "" || (od.Exit != nil && od.Exit.Amount > 0) {
		// Exit一旦有值，表示全部退出
		return nil, nil
	}
	if req.Dirt != 0 && (req.Dirt < 0) != od.Short {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "`ExitReq.Dirt` mismatch with Order")
	}
	if req.Limit > 0 && core.IsLimitOrder(req.OrderType) {
		odSide := ""
		if req.Dirt == core.OdDirtLong {
			odSide = banexg.OdSideSell
		} else if req.Dirt == core.OdDirtShort {
			odSide = banexg.OdSideBuy
		}
		price := o.priceExp(od.Symbol, odSide, com.Day10MSecs)
		if price > 0 && (req.Limit-price)*float64(req.Dirt) > 0 {
			// It is a valid limit order, set to take profit
			// 是有效的限价出场单，设置到止盈中
			_ = od.SetTakeProfit(&ormo.ExitTrigger{
				Price: req.Limit,
				Rate:  req.ExitRate,
				Tag:   req.Tag,
			})
			return o.postOrderExit(od)
		}
	}
	return o.exitOrder(od, req)
}

func (o *OrderMgr) exitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	// It has been confirmed externally that it is not a limit price stop profit
	// 外部已确认不是限价止盈
	odType := core.OrderTypeEnums[req.OrderType]
	if odType == "" {
		odType = o.orderType()
	}
	if req.ExitRate < 0.99 && req.ExitRate > 0 {
		// The portion to be exited is less than 99%, so a small order is split out for exit.
		// 要退出的部分不足99%，分割出一个小订单，用于退出。
		part := o.CutOrder(od, req.ExitRate, 0)
		req.ExitRate = 1
		err := od.Save()
		if err != nil {
			o.Logger().Error("save cutPart parent order fail", zap.String("key", od.Key()), zap.Error(err))
		}
		return o.exitOrder(part, req)
	}
	o.setExit(od, 0, req.Tag, odType, req.Limit)
	return o.postOrderExit(od)
}

func (o *OrderMgr) postOrderExit(od *ormo.InOutOrder) (*ormo.InOutOrder, *errs.Error) {
	err := od.Save()
	if err != nil {
		return od, err
	}
	if o.afterExit != nil {
		err = o.afterExit(od)
	}
	return od, err
}

/*
UpdateByDataSeries
Use the price to update the profit of the order, etc. It may trigger a margin call
使用价格更新订单的利润等。可能会触发爆仓
*/
func (o *OrderMgr) UpdateByDataSeries(allOpens []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	closeVal, err := evt.CloseValue()
	if err != nil {
		return errs.New(core.ErrInvalidBars, err)
	}
	highVal, err := evt.HighValue()
	if err != nil {
		return errs.New(core.ErrInvalidBars, err)
	}
	lowVal, lowErr := evt.LowValue()
	if lowErr != nil {
		lowVal = closeVal
	}
	symbol := evt.Symbol()
	tf := evt.TimeFrame
	for _, od := range allOpens {
		if od.Symbol != symbol || od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		matchTf := o.effectiveMatchTimeFrame(od, evt)
		if tf != matchTf {
			continue
		}
		od.UpdateProfits(closeVal)
		activePrice := od.GetInfoFloat64(ormo.OdInfoActivePrice)
		if activePrice > 0 {
			if od.Short && activePrice >= min(lowVal, closeVal) || !od.Short && activePrice <= max(highVal, closeVal) {
				od.SetInfo(ormo.OdInfoActivePrice, 0)
			} else {
				continue
			}
		}
		bestRef := highVal
		if od.Short {
			bestRef = lowVal
		}
		if bestRef > 0 {
			if err := od.UpdateTrailing(bestRef); err != nil {
				return err
			}
		}
		err := od.UpdateTrailing(closeVal)
		if err != nil {
			return err
		}
	}
	return nil
}

func seriesOHLCVCompat(evt *orm.DataSeries) *orm.SeriesOHLCV {
	if evt == nil {
		return nil
	}
	view, err := evt.OHLCV(evt.ExSymbol)
	if err != nil {
		return nil
	}
	return view
}

func (o *OrderMgr) CutOrder(od *ormo.InOutOrder, enterRate, exitRate float64) *ormo.InOutOrder {
	part := od.CutPart(od.Enter.Amount*enterRate, od.Enter.Amount*exitRate)
	// Here the key of part is the same as the original one, so part is used as src_key
	// 这里part的key和原始的一样，所以part作为src_key
	tgtKey, srcKey := od.Key(), part.Key()
	parts, parseErr := o.priceSymbolParts(od.Symbol)
	if parseErr != nil {
		// Without base/quote the wallet split cannot be done; do not call
		// CutPart with empty coin names.
		o.Logger().Error("resolve order symbol parts fail", zap.String("symbol", od.Symbol), zap.Error(parseErr))
		return part
	}
	base, quote := parts[0], parts[1]
	wallets := o.walletsForOrder()
	wallets.CutPart(srcKey, tgtKey, base, 1-enterRate)
	wallets.CutPart(srcKey, tgtKey, quote, 1-enterRate)
	return part
}

/*
finishOrder
It will be saved internally to the database during the actual trading.
实盘时内部会保存到数据库。
*/
func (o *OrderMgr) finishOrder(od *ormo.InOutOrder) *errs.Error {
	od.UpdateProfits(0)
	err := od.Save()
	if o != nil && o.runtimeDeps {
		account := o.walletDeps.DefaultAccount
		if account == "" && o.runtimeCore != nil && !o.runtimeCore.EnvReal {
			account = o.Account
		}
		if account == "" {
			account = o.Account
		}
		if o.walletDeps.Strategies != nil && o.runtimeCore != nil && o.orderState() != nil && o.Account == account {
			if cfg := o.walletDeps.Strategies.GetStratPerf(od.Symbol, od.Strategy); cfg != nil && cfg.Enable {
				if err2 := strat.CalcJobScoresWithState(o.walletDeps.Strategies, o.runtimeCore, o.orderState(), account,
					od.Symbol, od.Timeframe, od.Strategy); err2 != nil {
					o.Logger().Error("calc job performance fail", zap.Error(err2),
						zap.Strings("job", []string{od.Symbol, od.Timeframe, od.Strategy}))
				}
			}
		}
	} else {
		cfg := strat.GetStratPerf(od.Symbol, od.Strategy)
		if cfg != nil && cfg.Enable && o.Account == config.DefAcc {
			if err2 := strat.CalcJobScores(od.Symbol, od.Timeframe, od.Strategy); err2 != nil {
				o.Logger().Error("calc job performance fail", zap.Error(err2),
					zap.Strings("job", []string{od.Symbol, od.Timeframe, od.Strategy}))
			}
		}
	}
	return err
}

func (o *OrderMgr) CleanUp() *errs.Error {
	return nil
}

func CloseAccOrders(acc string, odList []*ormo.InOutOrder, req *strat.ExitReq) (int, int, *errs.Error) {
	var odMgr IOrderMgr
	if core.EnvReal {
		// Avoid wrapping a nil *LiveOrderMgr into a non-nil interface.
		if live := GetLiveOdMgr(acc); live != nil {
			odMgr = live
		}
	} else {
		odMgr = GetOdMgr(acc)
	}
	if odMgr == nil {
		return 0, len(odList), errs.NewMsg(errs.CodeRunTime, "order manager not found for account: %s", acc)
	}

	closeNum, failNum := 0, 0
	var errMsg strings.Builder
	for _, od := range odList {
		r := req.Clone()
		r.StratName = od.Strategy
		r.OrderID = od.ID
		_, err2 := odMgr.ExitOrder(od, r)
		if err2 != nil {
			failNum += 1
			errMsg.WriteString(fmt.Sprintf("Order %v: %v\n", od.ID, err2.Short()))
		} else {
			closeNum += 1
		}
	}
	if failNum > 0 {
		return closeNum, failNum, errs.NewMsg(errs.CodeRunTime, "%s", errMsg.String())
	}
	return closeNum, failNum, nil
}
