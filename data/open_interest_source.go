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
	utils2 "github.com/banbox/banexg/utils"
)

const builtinOpenInterestSourceVersion = "builtin-open-interest-v1"

var openInterestLivePollInterval = 5 * time.Minute

func init() {
	if err := RegisterDataSourceFactory(orm.SeriesSourceOpenInterest, func() DataSource {
		return NewOpenInterestDataSource(nil)
	}); err != nil {
		panic(err)
	}
}

// OpenInterestDataSource is the built-in exchange-agnostic derivatives open
// interest and long/short ratio series provider ("open_interest", default
// timeframe "5m", supporting 5m/15m/30m/1h/2h/4h/6h/12h/1d).
// Each snapshot measured at timestamp T is persisted over [T-tfMS, T) with
// EndMS = T so historical readers and factor snapshots only observe it at or
// after T (zero lookahead). Missing metrics are persisted as explicit NULLs.
type OpenInterestDataSource struct {
	info *orm.SeriesInfo

	mu   sync.RWMutex
	deps *RuntimeDeps
}

func NewOpenInterestDataSource(deps *RuntimeDeps) *OpenInterestDataSource {
	return &OpenInterestDataSource{
		info: orm.NewOpenInterestSeriesInfo(orm.DefaultOpenInterestTF),
		deps: deps,
	}
}

func (s *OpenInterestDataSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *OpenInterestDataSource) Version() string {
	return builtinOpenInterestSourceVersion
}

func (s *OpenInterestDataSource) SeriesInfoForTimeFrame(timeFrame string) (*orm.SeriesInfo, bool) {
	tf := strings.TrimSpace(timeFrame)
	if tf == "" {
		return s.info, true
	}
	if !orm.IsOpenInterestTimeFrame(tf) {
		return nil, false
	}
	return orm.NewOpenInterestSeriesInfo(tf), true
}

func (s *OpenInterestDataSource) SubTimeFrames(targetTimeFrame string) []string {
	targetSecs, err := utils2.TFToSecSafe(strings.TrimSpace(targetTimeFrame))
	if err != nil || targetSecs <= 0 {
		return nil
	}
	candidates := []string{"4h", "2h", "1h", "30m", "15m", "5m"}
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

func (s *OpenInterestDataSource) BindRuntimeDeps(deps *RuntimeDeps) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.deps = deps
	s.mu.Unlock()
}

func (s *OpenInterestDataSource) runtimeDeps() *RuntimeDeps {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deps
}

func (s *OpenInterestDataSource) AllowHistoryDownload() bool {
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

func (s *OpenInterestDataSource) resolveExchange() banexg.BanExchange {
	if deps := s.runtimeDeps(); deps != nil {
		return deps.Exchange
	}
	return exg.Default
}

func (s *OpenInterestDataSource) resolveTimeFrame(sub *orm.Subscription) (string, int64, error) {
	tf := orm.DefaultOpenInterestTF
	if sub != nil && strings.TrimSpace(sub.TimeFrame) != "" {
		tf = strings.TrimSpace(sub.TimeFrame)
	}
	if !orm.IsOpenInterestTimeFrame(tf) {
		return "", 0, fmt.Errorf("unsupported open_interest timeframe %q", tf)
	}
	secs, err := utils2.TFToSecSafe(tf)
	if err != nil || secs <= 0 {
		return "", 0, fmt.Errorf("invalid open_interest timeframe %q", tf)
	}
	return tf, int64(secs) * 1000, nil
}

func (s *OpenInterestDataSource) FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error) {
	return s.fetchHistoryBounded(ctx, sub, startMS, endMS, 0)
}

func (s *OpenInterestDataSource) FetchHistoryPage(ctx context.Context, sub *orm.Subscription, startMS, endMS int64, limit int) ([]*orm.DataRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("open_interest history page limit must be positive")
	}
	return s.fetchHistoryBounded(ctx, sub, startMS, endMS, limit)
}

func (s *OpenInterestDataSource) fetchHistoryBounded(ctx context.Context, sub *orm.Subscription, startMS, endMS int64, limit int) ([]*orm.DataRecord, error) {
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
		return nil, fmt.Errorf("open_interest history requires subscription symbol")
	}
	tf, tfMS, err := s.resolveTimeFrame(sub)
	if err != nil {
		return nil, err
	}
	if !s.AllowHistoryDownload() {
		return nil, nil
	}
	exchange := s.resolveExchange()
	if exchange == nil {
		return nil, fmt.Errorf("open_interest source requires an initialized exchange")
	}
	symbol := sub.ExSymbol.Symbol
	if m, mErr := exchange.GetMarket(symbol); mErr == nil && m != nil {
		if (!m.Swap && !m.Linear && !m.Inverse && !m.Contract) || m.Future || m.Option {
			return nil, nil
		}
	}

	// Each snapshot at Timestamp T covers [T - tfMS, T) with EndMS = T.
	// TimeMS >= startMS is equivalent to Timestamp >= startMS + tfMS.
	queryStartMS := startMS + tfMS
	if queryStartMS > endMS {
		return nil, nil
	}
	queryEndMS := endMS
	if limit > 0 {
		// Align start upward so limit windows cover at least `limit` closed snapshots.
		_, offSecs := utils2.GetTfAlignOrigin(int(tfMS / 1000))
		aligned := utils2.AlignTfMSecsOffset(max(int64(0), queryStartMS-1), tfMS, int64(offSecs)*1000) + tfMS
		if spanEnd := aligned + int64(limit)*tfMS; spanEnd > aligned && spanEnd < queryEndMS {
			queryEndMS = spanEnd
		}
	}
	var params map[string]interface{}
	if len(sub.Fields) > 0 {
		params = map[string]interface{}{
			"fields": append([]string(nil), sub.Fields...),
		}
	}
	items, supported, fetchErr := exg.FetchDerivativesMetrics(ctx, exchange, symbol, tf, queryStartMS, queryEndMS, limit, params)
	if fetchErr != nil {
		return nil, fetchErr
	}
	if !supported || len(items) == 0 {
		return nil, nil
	}
	records := derivativesMetricsToDataRecords(sub.ExSymbol.ID, items, startMS, endMS, tfMS)
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func derivativesMetricsToDataRecords(sid int32, items []*banexg.DerivativesMetric, startMS, endMS, tfMS int64) []*orm.DataRecord {
	if len(items) == 0 || startMS >= endMS || tfMS <= 0 {
		return nil
	}
	out := make([]*orm.DataRecord, 0, len(items))
	var lastTimeMS int64 = math.MinInt64
	for _, item := range items {
		if item == nil || item.Timestamp <= 0 {
			continue
		}
		timeMS := item.Timestamp - tfMS
		if timeMS < startMS || timeMS >= endMS || item.Timestamp > endMS || timeMS <= 0 {
			continue
		}
		values, hasAny := derivativesMetricValuesMap(item)
		if !hasAny {
			continue
		}
		rec := &orm.DataRecord{
			Sid:    sid,
			TimeMS: timeMS,
			EndMS:  item.Timestamp,
			Closed: true,
			Values: values,
		}
		if len(out) > 0 && timeMS == lastTimeMS {
			out[len(out)-1] = rec
			continue
		}
		if timeMS > lastTimeMS {
			out = append(out, rec)
			lastTimeMS = timeMS
		}
	}
	return out
}

func derivativesMetricValuesMap(item *banexg.DerivativesMetric) (map[string]any, bool) {
	if item == nil {
		return nil, false
	}
	oi, okOI := finiteMetricOrNil(item.OpenInterest)
	oiVal, okOIVal := finiteMetricOrNil(item.OpenInterestValue)
	topPos, okTopPos := finiteMetricOrNil(item.TopTraderLongShortPosRatio)
	topAcc, okTopAcc := finiteMetricOrNil(item.TopTraderLongShortAccountRatio)
	globalLS, okGlobal := finiteMetricOrNil(item.GlobalLongShortAccountRatio)
	takerLS, okTaker := finiteMetricOrNil(item.TakerLongShortVolRatio)
	if !okOI && !okOIVal && !okTopPos && !okTopAcc && !okGlobal && !okTaker {
		return nil, false
	}
	return map[string]any{
		orm.FieldOpenInterest:      oi,
		orm.FieldOpenInterestValue: oiVal,
		orm.FieldTopLSPosRatio:     topPos,
		orm.FieldTopLSAccRatio:     topAcc,
		orm.FieldGlobalLSRatio:     globalLS,
		orm.FieldTakerLSVolRatio:   takerLS,
	}, true
}

func finiteMetricOrNil(v float64) (any, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, false
	}
	return v, true
}

func (s *OpenInterestDataSource) SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error {
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
		_, tfMS, err := s.resolveTimeFrame(sub)
		if err != nil || tfMS <= 0 {
			tfMS = 5 * 60 * 1000
		}
		lastEndByKey[sub.Key().String()] = nowMS - tfMS
	}
	go func() {
		ticker := time.NewTicker(openInterestLivePollInterval)
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
