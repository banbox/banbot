package data

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

const (
	builtinMarkKlineSourceVersion    = "builtin-mark-kline-v1"
	builtinIndexKlineSourceVersion   = "builtin-index-kline-v1"
	builtinPremiumKlineSourceVersion = "builtin-premium-kline-v1"

	priceKlineRESTBatchSize = 500
)

var priceKlineLivePollInterval = time.Minute

func init() {
	for _, entry := range []struct {
		name    string
		factory DataSourceFactory
	}{
		{name: orm.SeriesSourceMarkKline, factory: func() DataSource { return NewMarkKlineDataSource(nil) }},
		{name: orm.SeriesSourceIndexKline, factory: func() DataSource { return NewIndexKlineDataSource(nil) }},
		{name: orm.SeriesSourcePremiumKline, factory: func() DataSource { return NewPremiumKlineDataSource(nil) }},
	} {
		if err := RegisterDataSourceFactory(entry.name, entry.factory); err != nil {
			panic(err)
		}
	}
}

// PriceKlineDataSource is the built-in exchange-agnostic multi-timeframe OHLC
// series provider for derivative mark price ("mark_kline"), spot index price
// ("index_kline"), and perpetual basis / premium index ("premium_kline").
// Each closed bar covering [TimeMS, TimeMS+tfMS) exposes float fields
// open/high/low/close (preserving negative basis values and resampling via
// open=first, high=max, low=min, close=last without volume>0 filtering).
type PriceKlineDataSource struct {
	info      *orm.SeriesInfo
	priceType string
	version   string

	mu   sync.RWMutex
	deps *RuntimeDeps
}

func newPriceKlineDataSource(sourceName, priceType, version string, deps *RuntimeDeps) *PriceKlineDataSource {
	return &PriceKlineDataSource{
		info:      orm.NewPriceKlineSeriesInfo(sourceName, orm.DefaultPriceKlineTF),
		priceType: priceType,
		version:   version,
		deps:      deps,
	}
}

func NewMarkKlineDataSource(deps *RuntimeDeps) *PriceKlineDataSource {
	return newPriceKlineDataSource(orm.SeriesSourceMarkKline, "mark", builtinMarkKlineSourceVersion, deps)
}

func NewIndexKlineDataSource(deps *RuntimeDeps) *PriceKlineDataSource {
	return newPriceKlineDataSource(orm.SeriesSourceIndexKline, "index", builtinIndexKlineSourceVersion, deps)
}

func NewPremiumKlineDataSource(deps *RuntimeDeps) *PriceKlineDataSource {
	return newPriceKlineDataSource(orm.SeriesSourcePremiumKline, "premiumIndex", builtinPremiumKlineSourceVersion, deps)
}

func (s *PriceKlineDataSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *PriceKlineDataSource) Version() string {
	return s.version
}

func (s *PriceKlineDataSource) SeriesInfoForTimeFrame(timeFrame string) (*orm.SeriesInfo, bool) {
	tf := strings.TrimSpace(timeFrame)
	if tf == "" {
		return s.info, true
	}
	if !orm.IsPriceKlineTimeFrame(tf) {
		return nil, false
	}
	return orm.NewPriceKlineSeriesInfo(s.info.Name, tf), true
}

func (s *PriceKlineDataSource) SubTimeFrames(targetTimeFrame string) []string {
	targetSecs, err := utils2.TFToSecSafe(strings.TrimSpace(targetTimeFrame))
	if err != nil || targetSecs <= 0 {
		return nil
	}
	candidates := []string{"1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m", "3m", "1m"}
	var out []string
	for _, cand := range candidates {
		candSecs, cErr := utils2.TFToSecSafe(cand)
		if cErr != nil || candSecs <= 0 || candSecs >= targetSecs {
			continue
		}
		if targetSecs%candSecs == 0 {
			out = append(out, cand)
		}
	}
	return out
}

func (s *PriceKlineDataSource) BindRuntimeDeps(deps *RuntimeDeps) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.deps = deps
	s.mu.Unlock()
}

func (s *PriceKlineDataSource) runtimeDeps() *RuntimeDeps {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deps
}

func (s *PriceKlineDataSource) AllowHistoryDownload() bool {
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

func (s *PriceKlineDataSource) resolveExchange() banexg.BanExchange {
	if deps := s.runtimeDeps(); deps != nil {
		return deps.Exchange
	}
	return exg.Default
}

func (s *PriceKlineDataSource) resolveTimeFrame(sub *orm.Subscription) (string, int64, int64, error) {
	tf := orm.DefaultPriceKlineTF
	if sub != nil && strings.TrimSpace(sub.TimeFrame) != "" {
		tf = strings.TrimSpace(sub.TimeFrame)
	}
	if !orm.IsPriceKlineTimeFrame(tf) {
		return "", 0, 0, fmt.Errorf("unsupported %s timeframe %q", s.info.Name, tf)
	}
	secs, err := utils2.TFToSecSafe(tf)
	if err != nil || secs <= 0 {
		return "", 0, 0, fmt.Errorf("invalid %s timeframe %q", s.info.Name, tf)
	}
	_, offSecs := utils2.GetTfAlignOrigin(secs)
	return tf, int64(secs) * 1000, int64(offSecs) * 1000, nil
}

func (s *PriceKlineDataSource) FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error) {
	return s.fetchHistoryBounded(ctx, sub, startMS, endMS, 0)
}

func (s *PriceKlineDataSource) FetchHistoryPage(ctx context.Context, sub *orm.Subscription, startMS, endMS int64, limit int) ([]*orm.DataRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%s history page limit must be positive", s.info.Name)
	}
	return s.fetchHistoryBounded(ctx, sub, startMS, endMS, limit)
}

func (s *PriceKlineDataSource) fetchHistoryBounded(ctx context.Context, sub *orm.Subscription, startMS, endMS int64, limit int) ([]*orm.DataRecord, error) {
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
		return nil, fmt.Errorf("%s history requires subscription symbol", s.info.Name)
	}
	tf, tfMS, alignOffMS, err := s.resolveTimeFrame(sub)
	if err != nil {
		return nil, err
	}
	if !s.AllowHistoryDownload() {
		return nil, nil
	}
	exchange := s.resolveExchange()
	if exchange == nil {
		return nil, fmt.Errorf("%s source requires an initialized exchange", s.info.Name)
	}
	symbol := sub.ExSymbol.Symbol
	if m, mErr := exchange.GetMarket(symbol); mErr == nil && m != nil {
		if m.Spot || (!m.Contract && !m.Swap && !m.Linear && !m.Inverse && !m.Option) {
			return nil, nil
		}
		if s.priceType == "premiumIndex" && (m.Future || m.Option) {
			return nil, nil
		}
		if m.Option && s.priceType != "mark" {
			return nil, nil
		}
	}

	firstOpen := utils2.AlignTfMSecsOffset(startMS, tfMS, alignOffMS)
	if firstOpen < startMS {
		firstOpen += tfMS
	}
	if firstOpen+tfMS > endMS {
		return nil, nil
	}
	closedEndMS := utils2.AlignTfMSecsOffset(endMS, tfMS, alignOffMS)
	if closedEndMS > endMS {
		closedEndMS -= tfMS
	}
	if closedEndMS <= firstOpen {
		return nil, nil
	}

	var records []*orm.DataRecord
	var lastTimeMS int64 = math.MinInt64
	appendBars := func(bars []*banexg.Kline) {
		for _, bar := range bars {
			if bar == nil || bar.Time < startMS || bar.Time >= closedEndMS {
				continue
			}
			endBarMS := bar.Time + tfMS
			if endBarMS > endMS || bar.Time <= 0 {
				continue
			}
			if !isFiniteOHLC(bar.Open, bar.High, bar.Low, bar.Close) {
				continue
			}
			rec := &orm.DataRecord{
				Sid:    sub.ExSymbol.ID,
				TimeMS: bar.Time,
				EndMS:  endBarMS,
				Closed: true,
				Values: map[string]any{
					"open":  bar.Open,
					"high":  bar.High,
					"low":   bar.Low,
					"close": bar.Close,
				},
			}
			if len(records) > 0 && bar.Time == lastTimeMS {
				records[len(records)-1] = rec
				continue
			}
			if bar.Time > lastTimeMS {
				records = append(records, rec)
				lastTimeMS = bar.Time
			}
		}
	}

	curSince := firstOpen
	archiveCutover := priceKlineArchiveCutoverMS(exchange)
	if curSince < archiveCutover {
		archStop := min(closedEndMS, archiveCutover)
		if limit <= 0 {
			archBars, avail, archErr := exg.FetchPriceOHLCVArchive(ctx, exchange, s.priceType, symbol, tf, curSince, archStop)
			if archErr != nil {
				if archErr.Code == errs.CodeNotSupport {
					return nil, nil
				}
				return nil, archErr
			}
			if avail {
				appendBars(archBars)
				curSince = archStop
			}
		} else {
			// Step by monthly archive windows when paged so pre-listing gaps are
			// traversed cheaply without decoding multi-year archives in one page.
			archiveSupported := true
			for curSince < archStop && len(records) < limit && archiveSupported {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				winEnd := nextUTCMonthBoundaryMS(curSince)
				if winEnd > archStop {
					winEnd = archStop
				}
				archBars, avail, archErr := exg.FetchPriceOHLCVArchive(ctx, exchange, s.priceType, symbol, tf, curSince, winEnd)
				if archErr != nil {
					if archErr.Code == errs.CodeNotSupport {
						return nil, nil
					}
					return nil, archErr
				}
				if !avail {
					// If no archive has been found yet, advance across pre-listing
					// months only when the adapter actually exposes PriceOHLCVArchiveFetcher;
					// otherwise break immediately to REST.
					if !hasPriceArchiveCapability(exchange) || len(records) > 0 {
						archiveSupported = false
						break
					}
					curSince = winEnd
					continue
				}
				appendBars(archBars)
				curSince = winEnd
			}
			// If every monthly archive probe returned unavailable on a symbol
			// whose archives are simply unpublished, fall back to REST from firstOpen.
			if len(records) == 0 && curSince > firstOpen {
				curSince = firstOpen
			}
		}
	}

	for curSince < closedEndMS && (limit <= 0 || len(records) < limit) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		leftBars := int((closedEndMS - curSince) / tfMS)
		if leftBars <= 0 {
			break
		}
		reqBatch := min(priceKlineRESTBatchSize, leftBars)
		if limit > 0 {
			if rem := limit - len(records); rem > 0 && rem < reqBatch {
				reqBatch = rem
			}
		}
		windowEnd := min(closedEndMS, curSince+int64(reqBatch)*tfMS) - 1
		bars, fetchErr := exchange.FetchOHLCV(symbol, tf, curSince, reqBatch, map[string]interface{}{
			"price":           s.priceType,
			banexg.ParamUntil: windowEnd,
		})
		if fetchErr != nil {
			if fetchErr.Code == errs.CodeNotSupport {
				return nil, nil
			}
			return nil, fetchErr
		}
		appendBars(bars)
		if len(bars) > 0 && bars[len(bars)-1].Time+tfMS > curSince {
			curSince = bars[len(bars)-1].Time + tfMS
		} else {
			curSince += int64(reqBatch) * tfMS
		}
	}

	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func hasPriceArchiveCapability(exchange banexg.BanExchange) bool {
	if exchange == nil {
		return false
	}
	if _, ok := exchange.(banexg.PriceOHLCVArchiveFetcher); ok {
		return true
	}
	if wrapper, ok := exchange.(interface{ UnderlyingExchange() banexg.BanExchange }); ok {
		return hasPriceArchiveCapability(wrapper.UnderlyingExchange())
	}
	return false
}

func priceKlineArchiveCutoverMS(exchange banexg.BanExchange) int64 {
	var nowMS int64
	if exchange != nil {
		nowMS = exchange.MilliSeconds()
	}
	if nowMS <= 0 {
		nowMS = time.Now().UTC().UnixMilli()
	}
	cutoff := time.UnixMilli(nowMS - int64(48*time.Hour/time.Millisecond)).UTC()
	return time.Date(cutoff.Year(), cutoff.Month(), cutoff.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
}

func nextUTCMonthBoundaryMS(ms int64) int64 {
	t := time.UnixMilli(ms).UTC()
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
}

func isFiniteOHLC(open, high, low, closeVal float64) bool {
	for _, v := range []float64{open, high, low, closeVal} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}

func (s *PriceKlineDataSource) SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error {
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
	nowMS := time.Now().UTC().UnixMilli()
	if deps := s.runtimeDeps(); deps != nil && deps.Clock != nil && deps.Clock.TimeMS() > 0 {
		nowMS = deps.Clock.TimeMS()
	}
	lastEndByKey := make(map[string]int64, len(cloned))
	for _, sub := range cloned {
		_, tfMS, _, err := s.resolveTimeFrame(sub)
		if err != nil || tfMS <= 0 {
			tfMS = 60 * 1000
		}
		lastEndByKey[sub.Key().String()] = nowMS - tfMS
	}
	go func() {
		ticker := time.NewTicker(priceKlineLivePollInterval)
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
					key := sub.Key().String()
					sinceMS := lastEndByKey[key]
					rows, err := s.FetchHistory(ctx, sub, sinceMS, curMS)
					if err != nil || len(rows) == 0 {
						continue
					}
					fresh := make([]*orm.DataRecord, 0, len(rows))
					for _, row := range rows {
						if row != nil && row.EndMS > sinceMS {
							fresh = append(fresh, row)
							if row.EndMS > lastEndByKey[key] {
								lastEndByKey[key] = row.EndMS
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
