package goods

import (
	"fmt"
	"slices"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
)

var (
	pairProducer IProducer
	filters      = make([]IFilter, 0, 10)
	ShowLog      = true
)

func Setup() *errs.Error {
	if len(config.PairFilters) == 0 {
		return nil
	}
	fts, err := GetPairFilters(config.PairFilters, false)
	if err != nil {
		return err
	}
	producer, ok := fts[0].(IProducer)
	if !ok {
		return errs.NewMsg(core.ErrBadConfig, "first pair filter must be IProducer")
	}
	pairProducer = producer
	filters = fts[1:]
	return nil
}

func GetPairFilters(items []*config.CommonPairFilter, withInvalid bool) ([]IFilter, *errs.Error) {
	return GetPairFiltersWithConfig(items, withInvalid, nil)
}

// GetPairFiltersWithConfig creates filter instances using the supplied
// runtime configuration. A nil config preserves the legacy package facade.
func GetPairFiltersWithConfig(items []*config.CommonPairFilter, withInvalid bool, cfg *config.Config) ([]IFilter, *errs.Error) {
	fts := make([]IFilter, 0, len(items))
	// 未启用定期刷新，则允许成交量为空的品种
	allowEmpty := true
	if cfg == nil {
		if config.PairMgr != nil {
			allowEmpty = config.PairMgr.Cron == ""
		}
	} else if cfg.PairMgr != nil {
		allowEmpty = cfg.PairMgr.Cron == ""
	}

	for _, item := range items {
		// Use the new registry system to create filters
		output, err := CreateFilter(item, allowEmpty)
		if err != nil {
			return nil, err
		}

		// Special handling for BlockFilter to parse pairs
		if blockFts, ok := output.(*BlockFilter); ok {
			var parseErr *errs.Error
			blockFts.Pairs, parseErr = parsePairsWithConfig(cfg, blockFts.Pairs...)
			if parseErr != nil {
				return nil, parseErr
			}
		}

		if withInvalid || !output.IsDisable() {
			fts = append(fts, output)
		}
	}
	return fts, nil
}

func parsePairsWithConfig(cfg *config.Config, pairs ...string) ([]string, *errs.Error) {
	if cfg == nil {
		return config.ParsePairs(pairs...)
	}
	exchangeName, market, quote := "", cfg.MarketType, ""
	if cfg.Exchange != nil {
		exchangeName = cfg.Exchange.Name
	}
	if len(cfg.StakeCurrency) > 0 {
		quote = cfg.StakeCurrency[0]
	}
	if config.ExchangeUsesOpaqueSymbols(exchangeName) {
		return slices.Clone(pairs), nil
	}
	result := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if strings.Contains(pair, "/") {
			result = append(result, pair)
			continue
		}
		if quote == "" {
			return nil, errs.NewMsg(core.ErrBadConfig, "`stake_currency` is required")
		}
		switch market {
		case banexg.MarketSpot:
			result = append(result, fmt.Sprintf("%s/%s", pair, quote))
		case banexg.MarketLinear:
			result = append(result, fmt.Sprintf("%s/%s:%s", pair, quote, quote))
		case banexg.MarketInverse:
			result = append(result, fmt.Sprintf("%s/%s:%s", pair, quote, pair))
		default:
			return nil, errs.NewMsg(core.ErrBadConfig, "option market don't support short pair")
		}
	}
	return result, nil
}

/*
RefreshPairList

刷新交易品种，如果alignStart=true，则计算当前时间前一个cron的触发时间对应的交易品种
更新core.Pairs和core.PairsMap
*/
func RefreshPairList(timeMS int64) ([]string, *errs.Error) {
	return RefreshPairListWithSymbolState(nil, exg.Default, timeMS)
}

// RefreshPairListWithSymbolState keeps symbol catalog and historical lookup
// on the supplied state. Custom filters without the optional state-aware
// interface continue through their existing IFilter method.
func RefreshPairListWithSymbolState(state *orm.SymbolState, exchange banexg.BanExchange, timeMS int64) ([]string, *errs.Error) {
	if exchange == nil {
		exchange = exg.Default
	}
	var allowFilter = false
	var err *errs.Error
	pairs, _ := config.GetStaticPairs()
	if len(pairs) > 0 {
		if !useFrozenStaticPairs(pairs) {
			pairVols, err := GetSymbolVolsWithSymbolState(state, exchange, pairs, "1h", 1, timeMS, true)
			if err != nil {
				return nil, err
			}
			pairs, _ = filterByMinCost(pairVols)
		}
		allowFilter = config.PairMgr.ForceFilters
	} else {
		allowFilter = true
		if producer, ok := pairProducer.(SymbolStateProducer); ok {
			pairs, err = producer.GenSymbolsWithSymbolState(state, exchange, timeMS)
		} else {
			pairs, err = pairProducer.GenSymbols(timeMS)
		}
		if err != nil {
			return nil, err
		}
		if ShowLog {
			log.Info(fmt.Sprintf("gen symbols from %s, num: %d", pairProducer.GetName(), len(pairs)))
		}
	}
	if state == nil {
		err = orm.EnsureCurSymbols(pairs)
	} else {
		err = orm.EnsureCurSymbolsWithSymbolState(state, exchange, pairs)
	}
	if err != nil {
		return nil, err
	}
	if allowFilter {
		for _, flt := range filters {
			if flt.IsDisable() {
				continue
			}
			oldNum := len(pairs)
			if stateFilter, ok := flt.(SymbolStateFilter); ok {
				pairs, err = stateFilter.FilterWithSymbolState(state, exchange, pairs, timeMS)
			} else {
				pairs, err = flt.Filter(pairs, timeMS)
			}
			if err != nil {
				return nil, err
			}
			if oldNum > len(pairs) && ShowLog {
				log.Info(fmt.Sprintf("left %d symbols after %s", len(pairs), flt.GetName()))
			}
		}
	}
	// 数量和偏移限制
	mgrCfg := config.PairMgr
	if mgrCfg.Offset > 0 {
		if mgrCfg.Offset < len(pairs) {
			pairs = pairs[mgrCfg.Offset:]
		} else {
			pairs = nil
		}
	}
	if mgrCfg.Limit > 0 && mgrCfg.Limit < len(pairs) {
		pairs = pairs[:mgrCfg.Limit]
	}

	core.Pairs = nil
	core.PairsMap = make(map[string]bool)
	for _, p := range pairs {
		core.Pairs = append(core.Pairs, p)
		core.PairsMap[p] = true
	}
	for _, p := range config.RunPolicy {
		for _, pair := range p.Pairs {
			core.PairsMap[pair] = true
		}
	}

	for pair := range core.BanPairsUntil {
		if _, ok := core.PairsMap[pair]; !ok {
			delete(core.BanPairsUntil, pair)
		}
	}
	return pairs, nil
}

// RefreshPairListWithRuntimeDeps refreshes one runtime's pair universe. It
// only updates the supplied core state; explicit runners never overwrite the
// process-wide pair and ban registries.
func RefreshPairListWithRuntimeDeps(deps *RuntimeDeps, timeMS int64) ([]string, *errs.Error) {
	if deps == nil {
		return RefreshPairList(timeMS)
	}
	if deps.Symbols == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for pair refresh")
	}
	if deps.Exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for pair refresh")
	}
	if deps.Config == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime config is required for pair refresh")
	}
	if deps.Core == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime core state is required for pair refresh")
	}
	cfg := deps.Config
	pairs, _ := staticPairsWithConfig(cfg)
	allowFilter := false
	var err *errs.Error
	if len(pairs) > 0 {
		if !useFrozenStaticPairsWithRuntime(cfg, deps.Core, pairs) {
			pairVols, volErr := GetSymbolVolsWithRuntimeDeps(deps, pairs, "1h", 1, timeMS, true)
			if volErr != nil {
				return nil, volErr
			}
			pairs, _ = filterByMinCostWithRuntime(deps.Exchange, pairVols, cfg, deps.ShowLog)
		}
		allowFilter = pairMgrForceFilters(cfg)
	} else if len(pairs) == 0 {
		allowFilter = true
		filters, filterErr := GetPairFiltersWithConfig(pairFilters(cfg), false, cfg)
		if filterErr != nil {
			return nil, filterErr
		}
		if len(filters) == 0 {
			return nil, errs.NewMsg(core.ErrBadConfig, "pair filters are required for dynamic pairs")
		}
		producer, ok := filters[0].(IProducer)
		if !ok {
			return nil, errs.NewMsg(core.ErrBadConfig, "first pair filter must be IProducer")
		}
		if runtimeProducer, ok := producer.(RuntimeProducer); ok {
			pairs, err = runtimeProducer.GenSymbolsWithRuntimeDeps(deps, timeMS)
		} else if stateProducer, ok := producer.(SymbolStateProducer); ok {
			pairs, err = stateProducer.GenSymbolsWithSymbolState(deps.Symbols, deps.Exchange, timeMS)
		} else {
			pairs, err = producer.GenSymbols(timeMS)
		}
		if err != nil {
			return nil, err
		}
		if runtimeShowLog(deps) {
			log.Info(fmt.Sprintf("gen symbols from %s, num: %d", producer.GetName(), len(pairs)))
		}
	}
	err = orm.EnsureCurSymbolsWithRuntimeConfig(deps.Symbols, deps.Exchange, pairs,
		deps.Config, deps.DataDir, deps.Core)
	if err != nil {
		return nil, err
	}
	if allowFilter {
		filters, filterErr := GetPairFiltersWithConfig(pairFilters(cfg), false, cfg)
		if filterErr != nil {
			return nil, filterErr
		}
		if len(filters) > 0 {
			filters = filters[1:]
		}
		pairs, err = FilterPairsWithRuntimeDeps(filters, deps, pairs, timeMS)
		if err != nil {
			return nil, err
		}
	}
	if mgr := pairMgr(cfg); mgr != nil {
		if mgr.Offset > 0 {
			if mgr.Offset < len(pairs) {
				pairs = pairs[mgr.Offset:]
			} else {
				pairs = nil
			}
		}
		if mgr.Limit > 0 && mgr.Limit < len(pairs) {
			pairs = pairs[:mgr.Limit]
		}
	}
	additional := make([]string, 0)
	if cfg != nil {
		for _, policy := range cfg.RunPolicy {
			additional = append(additional, policy.Pairs...)
		}
	}
	if deps.Core != nil {
		deps.Core.SetPairs(pairs, additional)
		for _, pair := range deps.Core.BannedPairs() {
			if !deps.Core.PairEnabled(pair) {
				deps.Core.SetPairBanUntil(pair, 0)
			}
		}
	}
	return pairs, nil
}

// FilterPairsWithRuntimeDeps applies an already-instantiated filter chain to
// one runtime's symbols and state.
func FilterPairsWithRuntimeDeps(filters []IFilter, deps *RuntimeDeps, pairs []string, timeMS int64) ([]string, *errs.Error) {
	if deps == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime dependencies are required for pair filtering")
	}
	if deps.Symbols == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required for pair filtering")
	}
	if deps.Exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required for pair filtering")
	}
	if deps.Config == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime config is required for pair filtering")
	}
	if deps.Core == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime core state is required for pair filtering")
	}
	var err *errs.Error
	for _, flt := range filters {
		if flt == nil || flt.IsDisable() {
			continue
		}
		if runtimeFilter, ok := flt.(RuntimeFilter); ok {
			pairs, err = runtimeFilter.FilterWithRuntimeDeps(deps, pairs, timeMS)
		} else if stateFilter, ok := flt.(SymbolStateFilter); ok {
			pairs, err = stateFilter.FilterWithSymbolState(deps.Symbols, deps.Exchange, pairs, timeMS)
		} else {
			pairs, err = flt.Filter(pairs, timeMS)
		}
		if err != nil {
			return nil, err
		}
	}
	return pairs, nil
}

func pairFilters(cfg *config.Config) []*config.CommonPairFilter {
	if cfg != nil {
		return cfg.PairFilters
	}
	return config.PairFilters
}

func pairMgr(cfg *config.Config) *config.PairMgrConfig {
	if cfg != nil {
		return cfg.PairMgr
	}
	return config.PairMgr
}

func pairMgrForceFilters(cfg *config.Config) bool {
	mgr := pairMgr(cfg)
	return mgr != nil && mgr.ForceFilters
}

func runtimeShowLog(deps *RuntimeDeps) bool {
	if deps == nil {
		return ShowLog
	}
	return deps.ShowLog
}

func staticPairsWithConfig(cfg *config.Config) ([]string, bool) {
	if cfg == nil {
		return config.GetStaticPairs()
	}
	pairs := slices.Clone(cfg.Pairs)
	needCalc := false
	for _, policy := range cfg.RunPolicy {
		if len(policy.Pairs) > 0 {
			pairs = append(pairs, policy.Pairs...)
		} else {
			needCalc = true
		}
	}
	if len(cfg.Pairs) > 0 {
		needCalc = false
	}
	if len(pairs) == 0 {
		return nil, false
	}
	seen := make(map[string]bool, len(pairs))
	result := pairs[:0]
	for _, pair := range pairs {
		if !seen[pair] {
			seen[pair] = true
			result = append(result, pair)
		}
	}
	return result, needCalc
}

func useFrozenStaticPairsWithRuntime(cfg *config.Config, state *core.State, pairs []string) bool {
	if cfg == nil {
		return config.IsFrozenStaticPairs(pairs)
	}
	backtest := state != nil && state.BackTestMode
	return len(pairs) > 0 && len(cfg.PairFilters) == 0 &&
		(cfg.PairMgr == nil || !cfg.PairMgr.ForceFilters) && backtest && cfg.BTStrict && cfg.BTNoKlineDownload
}

func useFrozenStaticPairs(pairs []string) bool {
	return config.IsFrozenStaticPairs(pairs)
}
