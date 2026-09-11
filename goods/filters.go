package goods

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/floats"
)

func (f *BaseFilter) IsDisable() bool {
	return f.Disable
}

func (f *BaseFilter) GetName() string {
	return f.Name
}

func (f *AgeFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.filterWithRuntimeDeps(nil, symbols, timeMS)
}

func (f *AgeFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if state == nil {
		return f.filterWithRuntimeDeps(nil, symbols, timeMS)
	}
	return f.filterWithRuntimeDeps(&RuntimeDeps{Symbols: state, Exchange: exchange}, symbols, timeMS)
}

func (f *AgeFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.filterWithRuntimeDeps(deps, symbols, timeMS)
}

func (f *AgeFilter) filterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if f.Min == 0 && f.Max == 0 {
		return symbols, nil
	}
	var state *orm.SymbolState
	var exchange banexg.BanExchange
	var coreState *core.State
	if deps != nil {
		state = deps.Symbols
		exchange = deps.Exchange
		coreState = deps.Core
		if state == nil {
			return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for age filter")
		}
		if f.AllowEmpty && coreState == nil {
			return nil, errs.NewMsg(core.ErrRunTime, "runtime core state is required for age filter with allow_empty")
		}
	}
	if exchange == nil {
		if deps != nil {
			return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for age filter")
		}
		exchange = exg.Default
	}
	dayMs := int64(utils2.TFToSecs("1d") * 1000)
	result := make([]string, 0, len(symbols))
	exInfo := exchange.Info()
	var exsMap map[int32]*orm.ExSymbol
	if state == nil {
		exsMap = orm.GetExSymbols(core.ExgName, core.Market)
	} else {
		exsMap = state.GetExSymbols(exInfo.ID, exInfo.MarketType)
	}
	var sess *orm.Queries
	var conn *pgxpool.Conn
	var err *errs.Error
	if deps != nil && deps.Storage != nil {
		sess, conn, err = deps.Storage.Conn(context.Background())
	} else if state != nil {
		sess, conn, err = state.Conn(context.Background())
	} else {
		sess, conn, err = orm.Conn(nil)
	}
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	pairMap := make(map[string]*orm.ExSymbol)
	for _, exs := range exsMap {
		pairMap[exs.Symbol] = exs
	}
	careMap := make(map[int32]*orm.ExSymbol)
	for _, p := range symbols {
		if exs, ok := pairMap[p]; ok {
			careMap[exs.ID] = exs
		} else {
			return nil, errs.NewMsg(errs.CodeNoMarketForPair, "unknown %v", p)
		}
	}
	err = orm.EnsureListDatesWithState(sess, state, exchange, careMap, nil)
	if err != nil {
		return nil, err
	}
	minStartMS := timeMS - dayMs*int64(f.Min)
	valids := make(map[string]bool)
	for _, exs := range careMap {
		if exs.ListMs > 0 {
			days := int((timeMS - exs.ListMs) / dayMs)
			if f.Max > 0 && days > f.Max {
				continue
			} else if f.Min > 0 && days < f.Min {
				if f.AllowEmpty {
					if coreState != nil {
						coreState.SetPairBanUntil(exs.Symbol, minStartMS)
					} else {
						core.BanPairsUntil[exs.Symbol] = minStartMS
					}
				} else {
					continue
				}
			}
			valids[exs.Symbol] = true
		} else {
			log.Info("listMs is empty", zap.String("key", exs.Symbol))
		}
		// ListMs=0表示尚未开始交易
	}
	for _, p := range symbols {
		if _, ok := valids[p]; ok {
			result = append(result, p)
		}
	}
	return result, nil
}

func (f *VolumePairFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.filterWithRuntimeDeps(nil, symbols, timeMS)
}

func (f *VolumePairFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if state == nil {
		return f.filterWithRuntimeDeps(nil, symbols, timeMS)
	}
	return f.filterWithRuntimeDeps(&RuntimeDeps{Symbols: state, Exchange: exchange}, symbols, timeMS)
}

func (f *VolumePairFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.filterWithRuntimeDeps(deps, symbols, timeMS)
}

func (f *VolumePairFilter) filterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	var state *orm.SymbolState
	var exchange banexg.BanExchange
	var cfg *config.Config
	if deps != nil {
		state = deps.Symbols
		exchange = deps.Exchange
		cfg = deps.Config
		if state == nil {
			return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for volume filter")
		}
		if exchange == nil {
			return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for volume filter")
		}
		if cfg == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "runtime config is required for volume filter")
		}
	}
	var klineOptions orm.KlineRuntimeOptions
	if deps == nil {
		klineOptions = orm.LegacyKlineRuntimeOptions()
	} else {
		klineOptions = orm.NewKlineRuntimeOptions(deps.Core, cfg, runtimeFilterNow(deps, timeMS), deps.Storage)
	}
	var symbolVols []*SymbolVol
	backTf, backNum := utils.SecsToTfNum(utils2.TFToSecs(f.BackPeriod))
	var err *errs.Error
	symbolVols, err = getSymbolVolsWithOptions(state, exchange, symbols, backTf, backNum, timeMS, true, klineOptions)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(symbolVols, compareSymbolVol)
	minValue := f.MinValue
	if !f.AllowEmpty && minValue == 0 {
		minValue = core.AmtDust
	}
	if minValue > 0 {
		for i, v := range symbolVols {
			if v.Vol >= minValue {
				continue
			}
			symbolVols = symbolVols[:i]
			break
		}
	}
	showLog := ShowLog
	if deps != nil {
		showLog = deps.ShowLog
	}
	resPairs, _ := filterByMinCostWithRuntime(exchange, symbolVols, cfg, showLog)
	if f.LimitRate > 0 && f.LimitRate < 1 {
		num := int(math.Round(f.LimitRate * float64(len(resPairs))))
		resPairs = resPairs[:num]
	}
	if f.Limit > 0 && f.Limit < len(resPairs) {
		resPairs = resPairs[:f.Limit]
	}
	return resPairs, nil
}

func (f *VolumePairFilter) GenSymbolsWithRuntimeDeps(deps *RuntimeDeps, timeMS int64) ([]string, *errs.Error) {
	var state *orm.SymbolState
	var exchange banexg.BanExchange
	var cfg *config.Config
	if deps != nil {
		state = deps.Symbols
		exchange = deps.Exchange
		cfg = deps.Config
	}
	if exchange == nil {
		if deps != nil {
			return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for volume pair producer")
		}
		exchange = exg.Default
	}
	marketMap := exchange.GetCurMarkets()
	pairs := volumeMarketSymbolsWithConfig(marketMap, cfg)
	if deps == nil {
		return f.filterWithRuntimeDeps(nil, pairs, timeMS)
	}
	copyDeps := *deps
	copyDeps.Symbols = state
	copyDeps.Exchange = exchange
	copyDeps.Config = cfg
	return f.filterWithRuntimeDeps(&copyDeps, pairs, timeMS)
}

func depsCore(deps *RuntimeDeps) *core.State {
	if deps == nil {
		return nil
	}
	return deps.Core
}

func (d *RuntimeDeps) querySeries(exs *orm.ExSymbol, timeframe string, startMS, endMS int64, limit int) ([]*orm.AdjInfo, []*orm.DataSeries, *errs.Error) {
	if d == nil {
		return orm.GetSeries(exs, timeframe, startMS, endMS, limit, false)
	}
	if d.Symbols == nil {
		return nil, nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for historical filter")
	}
	var sess *orm.Queries
	var conn *pgxpool.Conn
	var err *errs.Error
	if d.Storage != nil {
		sess, conn, err = d.Storage.Conn(context.Background())
	} else {
		sess, conn, err = d.Symbols.Conn(context.Background())
	}
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()
	sess = sess.WithSeriesSymbolState(d.Symbols).WithKlineRuntimeOptions(
		orm.NewKlineRuntimeOptions(d.Core, d.Config, runtimeFilterNow(d, endMS), d.Storage))
	return sess.GetSeries(exs, timeframe, startMS, endMS, limit, false)
}

type SymbolVol struct {
	Symbol string
	Vol    float64
	Price  float64
}

func compareSymbolVol(a, b *SymbolVol) int {
	if result := cmp.Compare(b.Vol, a.Vol); result != 0 {
		return result
	}
	return cmp.Compare(a.Symbol, b.Symbol)
}

func GetSymbolVols(symbols []string, tf string, num int, endMS int64, withEmpty bool) ([]*SymbolVol, *errs.Error) {
	return GetSymbolVolsWithSymbolState(nil, exg.Default, symbols, tf, num, endMS, withEmpty)
}

func GetSymbolVolsWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, tf string, num int, endMS int64, withEmpty bool) ([]*SymbolVol, *errs.Error) {
	return getSymbolVolsWithOptions(state, exchange, symbols, tf, num, endMS, withEmpty,
		orm.LegacyKlineRuntimeOptions())
}

// GetSymbolVolsWithRuntimeDeps loads historical volume using the supplied
// runtime policy. The explicit path never snapshots package-level backtest,
// download, clock, or storage settings.
func GetSymbolVolsWithRuntimeDeps(deps *RuntimeDeps, symbols []string, tf string, num int, endMS int64, withEmpty bool) ([]*SymbolVol, *errs.Error) {
	if deps == nil {
		return GetSymbolVolsWithSymbolState(nil, exg.Default, symbols, tf, num, endMS, withEmpty)
	}
	if deps.Symbols == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for volume data")
	}
	if deps.Exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for volume data")
	}
	storage := deps.Storage
	if storage == nil {
		storage = deps.Symbols.Storage()
	}
	options := orm.NewKlineRuntimeOptions(deps.Core, deps.Config, runtimeFilterNow(deps, endMS), storage)
	return getSymbolVolsWithOptions(deps.Symbols, deps.Exchange, symbols, tf, num, endMS, withEmpty, options)
}

func getSymbolVolsWithOptions(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, tf string, num int,
	endMS int64, withEmpty bool, options orm.KlineRuntimeOptions,
) ([]*SymbolVol, *errs.Error) {
	if exchange == nil && options.Storage != nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "exchange is required for volume data")
	}
	var symbolVols = make([]*SymbolVol, 0)
	callBack := func(symbol string, _ string, klines []*banexg.Kline, adjs []*orm.AdjInfo) {
		if len(klines) == 0 || len(klines) < num {
			if withEmpty {
				symbolVols = append(symbolVols, &SymbolVol{symbol, 0, 0})
			}
		} else {
			total := float64(0)
			slices.Reverse(klines)
			if len(klines) > num {
				klines = klines[:num]
			}
			for _, k := range klines {
				total += k.Close * k.Volume
			}
			vol := total / float64(len(klines))
			// 已倒序，选择第一个最近价格；此价格可能不是实时最新价格，但为保持品种刷新历史一致性，应固定使用此价格
			price := klines[0].Close
			if withEmpty || vol > 0 {
				symbolVols = append(symbolVols, &SymbolVol{symbol, vol, price})
			}
		}
	}
	if exchange == nil {
		exchange = exg.Default
	}
	err := orm.FastBulkOHLCVWithSymbolStateAndOptions(state, exchange, symbols, tf, 0, endMS, num, callBack, options)
	if err != nil {
		return nil, err
	}
	if len(symbolVols) == 0 {
		return nil, errs.NewMsg(core.ErrRunTime, "No data found for %d pairs at %v", len(symbols), endMS)
	}
	return symbolVols, nil
}

func runtimeFilterNow(deps *RuntimeDeps, fallback int64) int64 {
	if deps != nil && deps.Clock != nil {
		return deps.Clock.TimeMS()
	}
	return fallback
}

func filterByMinCost(symbols []*SymbolVol) ([]string, map[string]float64) {
	return filterByMinCostWithExchange(exg.Default, symbols)
}

func filterByMinCostWithExchange(exchange banexg.BanExchange, symbols []*SymbolVol) ([]string, map[string]float64) {
	return filterByMinCostWithRuntime(exchange, symbols, nil, ShowLog)
}

func filterByMinCostWithRuntime(exchange banexg.BanExchange, symbols []*SymbolVol, cfg *config.Config, showLog bool) ([]string, map[string]float64) {
	res := make([]string, 0, len(symbols))
	skip := make(map[string]float64)
	if exchange == nil {
		exchange = exg.Default
	}
	accCost := float64(0)
	accounts := config.Accounts
	if cfg != nil {
		accounts = cfg.Accounts
	}
	for name, account := range accounts {
		if account.NoTrade {
			continue
		}
		curCost := stakeAmount(cfg, name, account)
		if curCost > accCost {
			accCost = curCost
		}
	}
	for _, item := range symbols {
		mar, err := exchange.GetMarket(item.Symbol)
		if err != nil {
			if showLog {
				log.Warn("no market found", zap.String("symbol", item.Symbol))
			}
			skip[item.Symbol] = 0
			continue
		}
		if mar.Limits == nil || mar.Limits.Amount == nil {
			skip[item.Symbol] = 0
			continue
		}
		minAmt := mar.Limits.Amount.Min
		minCost := minAmt * item.Price
		if accCost < minCost {
			skip[item.Symbol] = minCost
		} else {
			res = append(res, item.Symbol)
		}
	}
	if len(skip) > 0 {
		var b strings.Builder
		for key, amt := range skip {
			b.WriteString(fmt.Sprintf("%s: %v  ", key, amt))
		}
		if showLog {
			log.Warn("skip symbols as cost too big", zap.Int("num", len(skip)), zap.String("more", b.String()))
		}
	}
	return res, skip
}

func stakeAmount(cfg *config.Config, name string, account *config.AccountConfig) float64 {
	if cfg == nil {
		return config.GetStakeAmount(name)
	}
	amount := cfg.StakeAmount
	if account != nil && account.StakePctAmt > 0 {
		amount = account.StakePctAmt
	}
	if account != nil && account.StakeRate > 0 {
		amount *= account.StakeRate
	}
	if account != nil && account.MaxStakeAmt > 0 && account.MaxStakeAmt < amount {
		amount = account.MaxStakeAmt
	} else if cfg.MaxStakeAmt > 0 && cfg.MaxStakeAmt < amount {
		amount = cfg.MaxStakeAmt
	}
	return amount
}

func (f *VolumePairFilter) GenSymbols(timeMS int64) ([]string, *errs.Error) {
	return f.GenSymbolsWithSymbolState(nil, exg.Default, timeMS)
}

func (f *VolumePairFilter) GenSymbolsWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, timeMS int64) ([]string, *errs.Error) {
	if exchange == nil {
		exchange = exg.Default
	}
	symbols := volumeMarketSymbols(exchange.GetCurMarkets())
	if len(symbols) == 0 {
		return nil, errs.NewMsg(errs.CodeRunTime, "no symbols generate from VolumePairFilter")
	}
	return f.FilterWithSymbolState(state, exchange, symbols, timeMS)
}

func volumeMarketSymbols(markets banexg.MarketMap) []string {
	return volumeMarketSymbolsWithConfig(markets, nil)
}

func volumeMarketSymbolsWithConfig(markets banexg.MarketMap, cfg *config.Config) []string {
	pairs := make([]string, 0, len(markets))
	stakeCurrencies := config.StakeCurrencyMap
	if cfg != nil {
		stakeCurrencies = make(map[string]bool, len(cfg.StakeCurrency))
		for _, currency := range cfg.StakeCurrency {
			stakeCurrencies[currency] = true
		}
	}
	for _, pair := range slices.Sorted(maps.Keys(markets)) {
		quote := ""
		if market := markets[pair]; market != nil {
			quote = market.Quote
		}
		if quote == "" {
			_, quote, _, _ = core.SplitSymbol(pair)
		}
		if _, ok := stakeCurrencies[quote]; ok {
			pairs = append(pairs, pair)
		}
	}
	return pairs
}

func (f *PriceFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
}

func (f *PriceFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return filterByOHLCVWithSymbolState(state, exchange, symbols, "1h", timeMS, 1, core.AdjFront, func(s string, klines []*banexg.Kline) bool {
		if len(klines) == 0 {
			return f.AllowEmpty
		}
		return f.validatePriceWithExchange(s, klines[len(klines)-1].Close, exchange)
	})
}

func (f *PriceFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if deps == nil {
		return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
	}
	return filterByOHLCVWithRuntimeDeps(deps, symbols, "1h", timeMS, 1, core.AdjFront,
		func(s string, klines []*banexg.Kline) bool {
			if len(klines) == 0 {
				return f.AllowEmpty
			}
			return f.validatePriceWithExchange(s, klines[len(klines)-1].Close, deps.Exchange)
		})
}

func (f *PriceFilter) validatePrice(symbol string, price float64) bool {
	return f.validatePriceWithExchange(symbol, price, exg.Default)
}

func (f *PriceFilter) validatePriceWithExchange(symbol string, price float64, exchange banexg.BanExchange) bool {
	if exchange == nil {
		exchange = exg.Default
	}
	if f.Precision > 0 {
		pip, err := exchange.PriceOnePip(symbol)
		if err != nil {
			log.Error("get one pip of price fail", zap.String("symbol", symbol))
			return false
		}
		chgPrec := pip / price
		if chgPrec > f.Precision {
			log.Info("PriceFilter drop, 1 unit fail", zap.String("pair", symbol), zap.Float64("p", chgPrec))
			return false
		}
	}

	if f.MaxUnitValue > 0 {
		market, err := exchange.GetMarket(symbol)
		if err != nil {
			log.Error("PriceFilter drop, market not exist", zap.String("pair", symbol))
			return false
		}
		minPrec := market.Precision.Amount
		if minPrec > 0 {
			if market.Precision.ModeAmount != banexg.PrecModeTickSize {
				minPrec = math.Pow(0.1, minPrec)
			}
			unitVal := minPrec * price
			if unitVal > f.MaxUnitValue {
				log.Info("PriceFilter drop, unit value too small", zap.String("pair", symbol),
					zap.Float64("uv", unitVal))
				return false
			}
		}
	}

	if f.Min > 0 && price < f.Min {
		log.Info("PriceFilter drop, price too small", zap.String("pair", symbol), zap.Float64("price", price))
		return false
	}

	if f.Max > 0 && f.Max < price {
		log.Info("PriceFilter drop, price too big", zap.String("pair", symbol), zap.Float64("price", price))
		return false
	}
	return true
}

func (f *RateOfChangeFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
}

func (f *RateOfChangeFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return filterByOHLCVWithSymbolState(state, exchange, symbols, "1d", timeMS, f.BackDays, core.AdjFront, f.validate)
}

func (f *RateOfChangeFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return filterByOHLCVWithRuntimeDeps(deps, symbols, "1d", timeMS, f.BackDays, core.AdjFront, f.validate)
}

func (f *RateOfChangeFilter) validate(pair string, arr []*banexg.Kline) bool {
	if len(arr) == 0 {
		return f.AllowEmpty
	}
	hhigh := arr[0].High
	llow := arr[0].Low
	for _, k := range arr[1:] {
		hhigh = max(hhigh, k.High)
		llow = min(llow, k.Low)
	}
	roc := float64(0)
	if llow > 0 {
		roc = (hhigh - llow) / llow
	}
	if f.Min > roc {
		log.Info("RateOfChangeFilter drop by min", zap.String("pair", pair), zap.Float64("roc", roc))
		return false
	}
	if f.Max > 0 && f.Max < roc {
		log.Info("RateOfChangeFilter drop by max", zap.String("pair", pair), zap.Float64("roc", roc))
		return false
	}
	return true
}

func filterByOHLCV(symbols []string, timeFrame string, endMS int64, limit int, adj int, cb func(string, []*banexg.Kline) bool) ([]string, *errs.Error) {
	return filterByOHLCVWithSymbolState(nil, exg.Default, symbols, timeFrame, endMS, limit, adj, cb)
}

func filterByOHLCVWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeFrame string, endMS int64, limit int, adj int, cb func(string, []*banexg.Kline) bool) ([]string, *errs.Error) {
	if exchange == nil {
		exchange = exg.Default
	}
	var has = make(map[string]struct{})
	handle := func(pair string, _ string, arr []*banexg.Kline, adjs []*orm.AdjInfo) {
		arr = orm.ApplyAdj(adjs, arr, adj, endMS, 0)
		if cb(pair, arr) {
			has[pair] = struct{}{}
		}
	}
	err := orm.FastBulkOHLCVWithSymbolState(state, exchange, symbols, timeFrame, 0, endMS, limit, handle)
	if err != nil {
		return nil, err
	}
	var res = make([]string, 0, len(has))
	for _, pair := range symbols {
		if _, ok := has[pair]; ok {
			res = append(res, pair)
		}
	}
	return res, nil
}

func filterByOHLCVWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeFrame string, endMS int64, limit int,
	adj int, cb func(string, []*banexg.Kline) bool,
) ([]string, *errs.Error) {
	if deps == nil {
		return filterByOHLCVWithSymbolState(nil, exg.Default, symbols, timeFrame, endMS, limit, adj, cb)
	}
	if deps.Exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for historical filter")
	}
	if deps.Symbols == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for historical filter")
	}
	options := orm.NewKlineRuntimeOptions(deps.Core, deps.Config, runtimeFilterNow(deps, endMS), deps.Storage)
	var has = make(map[string]struct{})
	handle := func(pair string, _ string, arr []*banexg.Kline, adjs []*orm.AdjInfo) {
		arr = orm.ApplyAdj(adjs, arr, adj, endMS, 0)
		if cb(pair, arr) {
			has[pair] = struct{}{}
		}
	}
	if err := orm.FastBulkOHLCVWithSymbolStateAndOptions(deps.Symbols, deps.Exchange, symbols, timeFrame,
		0, endMS, limit, handle, options); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(has))
	for _, pair := range symbols {
		if _, ok := has[pair]; ok {
			result = append(result, pair)
		}
	}
	return result, nil
}

func (f *CorrelationFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
}

func (f *CorrelationFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.filterWithRuntimeDeps(nil, state, exchange, symbols, timeMS)
}

func (f *CorrelationFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if deps == nil {
		return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
	}
	return f.filterWithRuntimeDeps(deps, deps.Symbols, deps.Exchange, symbols, timeMS)
}

func (f *CorrelationFilter) filterWithRuntimeDeps(deps *RuntimeDeps, state *orm.SymbolState,
	exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	if deps != nil {
		if exchange == nil {
			return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for correlation filter")
		}
		if state == nil {
			return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for correlation filter")
		}
	}
	if f.Timeframe == "" || f.BackNum == 0 || f.Max == 0 && f.TopN == 0 && f.TopRate == 0 {
		return symbols, nil
	}
	if f.BackNum < 10 {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "`CorrelationFilter.back_num` should >= 10, cur: %v", f.BackNum)
	}
	if f.TopRate > 0 {
		rateNum := int(math.Round(float64(len(symbols)) * f.TopRate))
		if f.TopN == 0 || f.TopN > rateNum {
			f.TopN = rateNum
		}
	}
	var skips []string
	var names = make([]string, 0, len(symbols))
	var dataArr = make([][]float64, 0, len(symbols))
	for _, pair := range symbols {
		var exs *orm.ExSymbol
		var err *errs.Error
		if state == nil {
			exs, err = orm.GetExSymbolCur(pair)
		} else {
			exs, err = state.GetExSymbolCur(pair)
		}
		if err != nil {
			skips = append(skips, pair)
			continue
		}
		var rows []*orm.DataSeries
		if deps == nil {
			_, rows, err = orm.GetSeries(exs, f.Timeframe, 0, timeMS, f.BackNum, false)
		} else {
			_, rows, err = deps.querySeries(exs, f.Timeframe, 0, timeMS, f.BackNum)
		}
		if err != nil || len(rows)*2 < f.BackNum {
			skips = append(skips, pair)
			continue
		}
		prices := make([]float64, 0, len(rows))
		badRow := false
		for _, row := range rows {
			closeVal, err_ := row.CloseValue()
			if err_ != nil {
				skips = append(skips, pair)
				badRow = true
				break
			}
			prices = append(prices, closeVal)
		}
		if badRow {
			continue
		}
		names = append(names, pair)
		if len(prices) > f.BackNum {
			prices = prices[:f.BackNum]
		}
		dataArr = append(dataArr, prices)
	}
	nameNum := len(names)
	if nameNum <= 3 {
		log.Warn("too less symbols, skip CorrelationFilter", zap.Int("num", nameNum))
		return symbols, nil
	}
	if len(skips) > 0 {
		log.Warn("skip for klines too less", zap.Strings("codes", skips))
	}
	mat, avgs, err_ := utils.CalcCorrMat(f.BackNum, dataArr, true)
	if err_ != nil {
		return nil, errs.New(errs.CodeRunTime, err_)
	}
	if f.Sort != "asc" && f.Sort != "desc" {
		// Use default sorting 使用默认排序
		result := make([]string, 0, nameNum)
		for i, avg := range avgs {
			if f.Min != 0 && avg < f.Min {
				continue
			}
			if f.Max != 0 && avg > f.Max {
				continue
			}
			result = append(result, names[i])
			if f.TopN > 0 && len(result) >= f.TopN {
				break
			}
		}
		return result, nil
	}
	// 按要求基于平均相似度排序
	lefts := make(map[int]bool)
	for i := range avgs {
		lefts[i] = true
	}
	isAsc := f.Sort == "asc"
	it := &IdVal{Id: 0, Val: avgs[0]}
	for id := 1; id < len(avgs); id++ {
		if betterCorrelationCandidate(avgs[id], id, it.Val, it.Id, isAsc) {
			it = &IdVal{Id: id, Val: avgs[id]}
		}
	}
	sels := make([]*IdVal, 0, len(avgs))
	sels = append(sels, it)
	delete(lefts, it.Id)
	for len(lefts) > 0 {
		// 针对每个剩余标的，计算与所有sels的平均相似度
		it = nil
		for id := range lefts {
			vals := make([]float64, 0, len(sels))
			for _, v := range sels {
				vals = append(vals, mat.At(id, v.Id))
			}
			avg := floats.Sum(vals) / float64(len(vals))
			if it == nil || betterCorrelationCandidate(avg, id, it.Val, it.Id, isAsc) {
				it = &IdVal{Id: id, Val: avg}
			}
		}
		sels = append(sels, &IdVal{Id: it.Id, Val: avgs[it.Id]})
		delete(lefts, it.Id)
	}
	// 按规则过滤
	result := make([]string, 0, nameNum)
	for _, item := range sels {
		if f.Min != 0 && item.Val < f.Min {
			continue
		}
		if f.Max != 0 && item.Val > f.Max {
			continue
		}
		result = append(result, names[item.Id])
		if f.TopN > 0 && len(result) >= f.TopN {
			break
		}
	}
	return result, nil
}

func betterCorrelationCandidate(value float64, id int, currentValue float64, currentID int, ascending bool) bool {
	valueFinite := !math.IsNaN(value) && !math.IsInf(value, 0)
	currentFinite := !math.IsNaN(currentValue) && !math.IsInf(currentValue, 0)
	if valueFinite != currentFinite {
		return valueFinite
	}
	if !valueFinite || value == currentValue {
		return id < currentID
	}
	if ascending {
		return value < currentValue
	}
	return value > currentValue
}

type IdVal struct {
	Id  int
	Val float64
}

func (f *VolatilityFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return f.FilterWithSymbolState(nil, exg.Default, symbols, timeMS)
}

func (f *VolatilityFilter) FilterWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return filterByOHLCVWithSymbolState(state, exchange, symbols, "1d", timeMS, f.BackDays, core.AdjFront, func(s string, klines []*banexg.Kline) bool {
		if len(klines) == 0 {
			return f.AllowEmpty
		}
		var data = make([]float64, 0, len(klines))
		for i, v := range klines[1:] {
			data = append(data, v.Close/klines[i].Close)
		}
		res := utils.StdDevVolatility(data, 1)
		if res < f.Min || f.Max > 0 && res > f.Max {
			log.Info("VolatilityFilter drop", zap.String("pair", s), zap.Float64("v", res))
			return false
		}
		return true
	})
}

func (f *VolatilityFilter) FilterWithRuntimeDeps(deps *RuntimeDeps, symbols []string, timeMS int64) ([]string, *errs.Error) {
	return filterByOHLCVWithRuntimeDeps(deps, symbols, "1d", timeMS, f.BackDays, core.AdjFront,
		func(s string, klines []*banexg.Kline) bool {
			if len(klines) == 0 {
				return f.AllowEmpty
			}
			data := make([]float64, 0, len(klines))
			for i, value := range klines[1:] {
				data = append(data, value.Close/klines[i].Close)
			}
			result := utils.StdDevVolatility(data, 1)
			if result < f.Min || f.Max > 0 && result > f.Max {
				log.Info("VolatilityFilter drop", zap.String("pair", s), zap.Float64("v", result))
				return false
			}
			return true
		})
}

func (f *SpreadFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	return symbols, nil
}

func (f *BlockFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	if len(f.Pairs) == 0 {
		return symbols, nil
	}
	if f.pairMap == nil {
		f.pairMap = make(map[string]bool)
		for _, p := range f.Pairs {
			f.pairMap[p] = true
		}
	}
	res := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if _, ok := f.pairMap[s]; !ok {
			res = append(res, s)
		}
	}
	return res, nil
}

func (f *OffsetFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	var res = symbols
	if f.Reverse {
		slices.Reverse(res)
	}
	if f.Offset < len(res) {
		res = res[f.Offset:]
	}
	if f.Rate > 0 && f.Rate < 1 {
		num := int(math.Round(float64(len(res)) * f.Rate))
		res = res[:num]
	}
	if f.Limit > 0 && f.Limit < len(res) {
		res = res[:f.Limit]
	}
	return res, nil
}

func (f *ShuffleFilter) Filter(symbols []string, timeMS int64) ([]string, *errs.Error) {
	rand.New(rand.NewSource(int64(f.Seed))).Shuffle(len(symbols), func(i, j int) {
		symbols[i], symbols[j] = symbols[j], symbols[i]
	})
	return symbols, nil
}
