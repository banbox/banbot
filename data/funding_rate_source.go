package data

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"
)

const builtinFundingRateSourceVersion = "builtin-funding-rate-v1"

var fundingRateLivePollInterval = 5 * time.Minute

func init() {
	if err := RegisterDataSourceFactory(orm.SeriesSourceFundingRate, func() DataSource {
		return NewFundingRateDataSource(nil)
	}); err != nil {
		panic(err)
	}
}

// FundingRateDataSource is the built-in exchange-agnostic perpetual funding
// rate series provider ("funding_rate", default timeframe "8h").
// Each settled funding event at timestamp T is persisted with EndMS = T and
// TimeMS = T - intervalMS so historical readers only observe it at or after T.
type FundingRateDataSource struct {
	info *orm.SeriesInfo

	mu   sync.RWMutex
	deps *RuntimeDeps
}

func NewFundingRateDataSource(deps *RuntimeDeps) *FundingRateDataSource {
	return &FundingRateDataSource{
		info: orm.NewFundingRateSeriesInfo(orm.DefaultFundingRateTF),
		deps: deps,
	}
}

func (s *FundingRateDataSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *FundingRateDataSource) Version() string {
	return builtinFundingRateSourceVersion
}

func (s *FundingRateDataSource) BindRuntimeDeps(deps *RuntimeDeps) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.deps = deps
	s.mu.Unlock()
}

func (s *FundingRateDataSource) runtimeDeps() *RuntimeDeps {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deps
}

func (s *FundingRateDataSource) AllowHistoryDownload() bool {
	deps := s.runtimeDeps()
	if deps != nil {
		if deps.Core != nil && deps.Core.NetDisable {
			return false
		}
		if deps.Exchange != nil && deps.Exchange.GetNetDisable() {
			return false
		}
		if deps.Config != nil {
			if cfg := deps.Config.View(); cfg != nil && cfg.BTNoKlineDownload {
				return false
			}
		}
		return true
	}
	if core.NetDisable {
		return false
	}
	if exg.Default != nil && exg.Default.GetNetDisable() {
		return false
	}
	if config.Data.BTNoKlineDownload {
		return false
	}
	return true
}

func (s *FundingRateDataSource) resolveExchange() banexg.BanExchange {
	if deps := s.runtimeDeps(); deps != nil {
		return deps.Exchange
	}
	return exg.Default
}

func (s *FundingRateDataSource) nominalIntervalMS() int64 {
	if s != nil && s.info != nil {
		if secs, err := utils2.TFToSecSafe(s.info.TimeFrame); err == nil && secs > 0 {
			return int64(secs) * 1000
		}
	}
	return 8 * 3600 * 1000
}

func (s *FundingRateDataSource) FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error) {
	if startMS >= endMS {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.Symbol == "" {
		return nil, fmt.Errorf("funding_rate history requires subscription symbol")
	}
	if !s.AllowHistoryDownload() {
		return nil, nil
	}
	exchange := s.resolveExchange()
	if exchange == nil {
		return nil, fmt.Errorf("funding_rate source requires an initialized exchange")
	}
	symbol := sub.ExSymbol.Symbol
	var market *banexg.Market
	if m, err := exchange.GetMarket(symbol); err == nil && m != nil {
		market = m
		if !m.Swap && !m.Linear && !m.Inverse && !m.Contract {
			return nil, nil
		}
		if m.Future || m.Option {
			return nil, nil
		}
	}

	rates, err := fetchExchangeFundingRates(ctx, exchange, symbol, startMS, endMS)
	if err != nil {
		return nil, err
	}
	return fundingRatesToDataRecords(sub.ExSymbol.ID, rates, market, startMS, endMS, s.nominalIntervalMS()), nil
}

func fetchExchangeFundingRates(ctx context.Context, exchange banexg.BanExchange, symbol string, startMS, endMS int64) ([]*banexg.FundingRate, error) {
	var collected []*banexg.FundingRate
	fetchFrom := startMS
	archiveCutover := currentUTCMonthStartMS() - 1
	if fetchFrom <= archiveCutover {
		archEnd := min(endMS, archiveCutover)
		archRates, available, archErr := exg.FetchFundingRateArchive(ctx, exchange, symbol, fetchFrom, archEnd)
		if archErr == nil && available {
			collected = append(collected, archRates...)
			fetchFrom = archEnd + 1
		}
	}
	if fetchFrom <= endMS {
		apiRates, apiErr := exchange.FetchFundingRateHistory(symbol, fetchFrom, 0, map[string]interface{}{
			banexg.ParamUntil:   endMS,
			banexg.ParamContext: ctx,
		})
		if apiErr != nil {
			return nil, apiErr
		}
		collected = append(collected, apiRates...)
	}
	return dedupeFundingRates(collected), nil
}

func currentUTCMonthStartMS() int64 {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).UnixMilli()
}

func dedupeFundingRates(rates []*banexg.FundingRate) []*banexg.FundingRate {
	if len(rates) == 0 {
		return nil
	}
	valid := make([]*banexg.FundingRate, 0, len(rates))
	for _, item := range rates {
		if item == nil || item.Timestamp <= 0 || math.IsNaN(item.FundingRate) || math.IsInf(item.FundingRate, 0) {
			continue
		}
		valid = append(valid, item)
	}
	sort.SliceStable(valid, func(i, j int) bool { return valid[i].Timestamp < valid[j].Timestamp })
	out := valid[:0]
	for _, item := range valid {
		if len(out) > 0 && out[len(out)-1].Timestamp == item.Timestamp {
			out[len(out)-1] = item
			continue
		}
		out = append(out, item)
	}
	return out
}

func fundingRatesToDataRecords(sid int32, rates []*banexg.FundingRate, market *banexg.Market, startMS, endMS, nominalMS int64) []*orm.DataRecord {
	if len(rates) == 0 || startMS >= endMS {
		return nil
	}
	if nominalMS <= 0 {
		nominalMS = 8 * 3600 * 1000
	}
	var prevTS int64
	out := make([]*orm.DataRecord, 0, len(rates))
	for _, item := range rates {
		if item == nil || item.Timestamp <= 0 {
			continue
		}
		if item.Timestamp <= startMS {
			if item.Timestamp > prevTS {
				prevTS = item.Timestamp
			}
			continue
		}
		if item.Timestamp > endMS {
			continue
		}
		intervalMS := fundingSettlementIntervalMS(item, market, nominalMS)
		timeMS := item.Timestamp - intervalMS
		if prevTS > 0 && timeMS < prevTS {
			timeMS = prevTS
		}
		if timeMS < startMS {
			timeMS = startMS
		}
		if timeMS >= item.Timestamp || timeMS >= endMS {
			continue
		}
		out = append(out, &orm.DataRecord{
			Sid:    sid,
			TimeMS: timeMS,
			EndMS:  item.Timestamp,
			Closed: true,
			Values: map[string]any{
				"rate": item.FundingRate,
			},
		})
		prevTS = item.Timestamp
	}
	return out
}

func fundingSettlementIntervalMS(item *banexg.FundingRate, market *banexg.Market, nominalMS int64) int64 {
	if item != nil && item.Info != nil {
		if hours := parsePositiveInt64(item.Info["fundingIntervalHours"]); hours > 0 {
			return hours * 3600 * 1000
		}
	}
	if market != nil && market.Info != nil {
		if mins := parsePositiveInt64(market.Info["fundingInterval"]); mins > 0 {
			return mins * 60 * 1000
		}
		if hours := parsePositiveInt64(market.Info["fundingIntervalHours"]); hours > 0 {
			return hours * 3600 * 1000
		}
	}
	return nominalMS
}

func parsePositiveInt64(raw any) int64 {
	switch v := raw.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func (s *FundingRateDataSource) SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error {
	if len(subs) == 0 || sink == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cloned := make([]*orm.Subscription, 0, len(subs))
	for _, sub := range subs {
		if sub == nil || sub.ExSymbol == nil {
			continue
		}
		cp := *sub
		cloned = append(cloned, &cp)
	}
	if len(cloned) == 0 {
		return nil
	}
	nominalMS := s.nominalIntervalMS()
	lastEndBySID := make(map[int32]int64, len(cloned))
	nowMS := time.Now().UTC().UnixMilli()
	if deps := s.runtimeDeps(); deps != nil && deps.Clock != nil && deps.Clock.TimeMS() > 0 {
		nowMS = deps.Clock.TimeMS()
	}
	for _, sub := range cloned {
		lastEndBySID[sub.ExSymbol.ID] = nowMS - nominalMS
	}
	go func() {
		ticker := time.NewTicker(fundingRateLivePollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case tick := <-ticker.C:
				if !s.AllowHistoryDownload() {
					continue
				}
				curMS := tick.UTC().UnixMilli()
				if deps := s.runtimeDeps(); deps != nil && deps.Clock != nil && deps.Clock.TimeMS() > 0 {
					curMS = deps.Clock.TimeMS()
				}
				for _, sub := range cloned {
					if ctx.Err() != nil {
						return
					}
					sinceMS := lastEndBySID[sub.ExSymbol.ID]
					rows, err := s.FetchHistory(ctx, sub, sinceMS, curMS)
					if err != nil || len(rows) == 0 {
						continue
					}
					fresh := make([]*orm.DataRecord, 0, len(rows))
					for _, row := range rows {
						if row != nil && row.EndMS > sinceMS {
							fresh = append(fresh, row)
							if row.EndMS > lastEndBySID[sub.ExSymbol.ID] {
								lastEndBySID[sub.ExSymbol.ID] = row.EndMS
							}
						}
					}
					if len(fresh) > 0 {
						_ = sink.Emit(sub, fresh)
					}
				}
			}
		}
	}()
	return nil
}
