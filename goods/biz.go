package goods

import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
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
	fts := make([]IFilter, 0, len(items))
	// 未启用定期刷新，则允许成交量为空的品种
	allowEmpty := config.PairMgr.Cron == ""

	for _, cfg := range items {
		// Use the new registry system to create filters
		output, err := CreateFilter(cfg, allowEmpty)
		if err != nil {
			return nil, err
		}

		// Special handling for BlockFilter to parse pairs
		if blockFts, ok := output.(*BlockFilter); ok {
			var parseErr *errs.Error
			blockFts.Pairs, parseErr = config.ParsePairs(blockFts.Pairs...)
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

/*
RefreshPairList

刷新交易品种，如果alignStart=true，则计算当前时间前一个cron的触发时间对应的交易品种
更新core.Pairs和core.PairsMap
*/
func RefreshPairList(timeMS int64) ([]string, *errs.Error) {
	var allowFilter = false
	var err *errs.Error
	pairs, _ := config.GetStaticPairs()
	if len(pairs) > 0 {
		pairVols, err := GetSymbolVols(pairs, "1h", 1, timeMS, true)
		if err != nil {
			return nil, err
		}
		pairs, _ = filterByMinCost(pairVols)
		allowFilter = config.PairMgr.ForceFilters
	} else {
		allowFilter = true
		pairs, err = pairProducer.GenSymbols(timeMS)
		if err != nil {
			return nil, err
		}
		if ShowLog {
			log.Info(fmt.Sprintf("gen symbols from %s, num: %d", pairProducer.GetName(), len(pairs)))
		}
	}
	err = orm.EnsureCurSymbols(pairs)
	if err != nil {
		return nil, err
	}
	if allowFilter {
		for _, flt := range filters {
			if flt.IsDisable() {
				continue
			}
			oldNum := len(pairs)
			pairs, err = flt.Filter(pairs, timeMS)
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
