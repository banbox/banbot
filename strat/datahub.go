package strat

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
)

const DefaultDataHubCache = 1500

var klineBarEnvFields = [...]string{
	"open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num",
}

type DataHub struct {
	mu            sync.RWMutex
	sourceMap     map[string]map[string]map[int32]*DataFields // timeframe:source:sid
	curMS         int64
	limit         int
	explicitLimit bool
}

type DataFields struct {
	mu            sync.RWMutex
	seriesMap     map[string]*ta.Series
	rawMap        map[string]any // values from the latest event; map presence preserves explicit NULL
	valMap        map[string]any
	DoneMS        int64
	TimeMS        int64
	Source        string
	Sid           int32
	Symbol        string
	TimeFrame     string
	Closed        bool
	IsWarmUp      bool
	env           *ta.BarEnv
	isKlineEnv    bool
	seriesFields  map[string]bool
	seriesNames   []string
	seriesNameSet map[string]struct{}
	autoSeries    bool
	configured    bool
	limit         int
	explicitLimit bool
}

func NewDataHub(limit ...int) *DataHub {
	curLimit := max(DefaultDataHubCache, core.NumTaCache)
	explicit := false
	if len(limit) > 0 && limit[0] > 0 {
		curLimit = limit[0]
		explicit = true
	}
	return &DataHub{
		sourceMap:     make(map[string]map[string]map[int32]*DataFields),
		limit:         curLimit,
		explicitLimit: explicit,
	}
}

func (d *DataHub) MaxCache() int {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	limit := d.limit
	d.mu.RUnlock()
	return limit
}

// SetMaxCache overrides the maximum retained history length for this DataHub
// and every non-explicitly-sized DataFields view it owns.
func (d *DataHub) SetMaxCache(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.Lock()
	d.limit = limit
	d.explicitLimit = true
	var list []*DataFields
	for _, bySource := range d.sourceMap {
		for _, bySID := range bySource {
			for _, fields := range bySID {
				if fields != nil {
					list = append(list, fields)
				}
			}
		}
	}
	d.mu.Unlock()
	for _, fields := range list {
		fields.setHubLimit(limit)
	}
}

func (d *DataHub) raiseMinCache(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.Lock()
	if d.explicitLimit || limit <= d.limit {
		d.mu.Unlock()
		return
	}
	d.limit = limit
	var list []*DataFields
	for _, bySource := range d.sourceMap {
		for _, bySID := range bySource {
			for _, fields := range bySID {
				if fields != nil {
					list = append(list, fields)
				}
			}
		}
	}
	d.mu.Unlock()
	for _, fields := range list {
		fields.raiseMinCache(limit)
	}
}

// Configure pre-registers subscriptions so AllReady also accounts for data
// streams that have not emitted their first value yet.
func (d *DataHub) Configure(subs []*DataSub) {
	if d == nil {
		return
	}
	d.mu.Lock()
	explicitHub := d.explicitLimit
	type pending struct {
		fields *DataFields
		sub    *DataSub
	}
	items := make([]pending, 0, len(subs))
	for _, sub := range subs {
		if sub == nil || sub.ExSymbol == nil || sub.TimeFrame == "" {
			continue
		}
		fields := d.ensureLocked(sub.TimeFrame, sub.Source, sub.ExSymbol.ID)
		items = append(items, pending{fields: fields, sub: sub})
	}
	d.mu.Unlock()
	for _, item := range items {
		item.fields.configureSub(item.sub, !explicitHub)
	}
}

func (d *DataHub) findKlineBySymbol(tf string, symbols ...string) *DataFields {
	if d == nil || tf == "" || len(symbols) == 0 {
		return nil
	}
	d.mu.RLock()
	bySource := d.sourceMap[tf]
	bySID := bySource[orm.SeriesSourceKline]
	var candidates []*DataFields
	for _, fields := range bySID {
		if fields != nil {
			candidates = append(candidates, fields)
		}
	}
	d.mu.RUnlock()
	for _, sym := range symbols {
		if sym == "" {
			continue
		}
		for _, fields := range candidates {
			if fields.symbolName() == sym {
				return fields
			}
		}
	}
	return nil
}

func (d *DataHub) Get(tf string, source string, sid int32) *DataFields {
	if d == nil {
		return nil
	}
	source = orm.NormalizeSeriesSource(source)
	d.mu.RLock()
	bySource := d.sourceMap[tf]
	bySID := bySource[source]
	fields := bySID[sid]
	d.mu.RUnlock()
	return fields
}

// Set updates one subscribed stream and returns the processed field view.
// seriesFields is optional; omitting it enables the default float-field policy.
func (d *DataHub) Set(evt *orm.DataSeries, seriesFields ...[]string) *DataFields {
	if d == nil || evt == nil {
		return nil
	}
	endMS := dataEndMS(evt)
	d.mu.Lock()
	fields := d.ensureLocked(evt.TimeFrame, evt.Source, evt.Sid)
	if len(seriesFields) > 0 {
		fields.configure(seriesFields[0])
	} else {
		fields.configureDefault()
	}
	if endMS > d.curMS {
		d.curMS = endMS
	}
	d.mu.Unlock()
	fields.update(evt, endMS)
	return fields
}

// AllReady reports whether every stream whose timeframe closes at the current
// event time has been updated through that time. Longer, not-yet-closing
// timeframes do not block the result.
func (d *DataHub) AllReady() bool {
	if d == nil {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.curMS <= 0 || len(d.sourceMap) == 0 {
		return false
	}
	for tf, bySource := range d.sourceMap {
		if !timeFrameClosesAt(tf, d.curMS) {
			continue
		}
		for _, bySID := range bySource {
			for _, fields := range bySID {
				if fields == nil || fields.DoneMS < d.curMS {
					return false
				}
			}
		}
	}
	return true
}

func (d *DataHub) ensureLocked(tf, source string, sid int32) *DataFields {
	source = orm.NormalizeSeriesSource(source)
	bySource := d.sourceMap[tf]
	if bySource == nil {
		bySource = make(map[string]map[int32]*DataFields)
		d.sourceMap[tf] = bySource
	}
	bySID := bySource[source]
	if bySID == nil {
		bySID = make(map[int32]*DataFields)
		bySource[source] = bySID
	}
	fields := bySID[sid]
	if fields == nil {
		fields = &DataFields{
			seriesMap:    make(map[string]*ta.Series),
			rawMap:       make(map[string]any),
			valMap:       make(map[string]any),
			seriesFields: make(map[string]bool),
			limit:        d.limit,
		}
		bySID[sid] = fields
	}
	return fields
}

func (d *DataFields) configureSub(sub *DataSub, raiseWarmup bool) {
	if sub == nil {
		return
	}
	d.mu.Lock()
	if sub.ExSymbol != nil && sub.ExSymbol.Symbol != "" && d.Symbol == "" {
		d.Symbol = sub.ExSymbol.Symbol
	}
	if orm.NormalizeSeriesSource(sub.Source) == orm.SeriesSourceKline {
		d.isKlineEnv = true
	}
	if raiseWarmup && !d.explicitLimit && sub.WarmupNum > 0 {
		if target := sub.WarmupNum + 100; target > d.limit {
			d.limit = target
			if d.env != nil {
				d.env.MaxCache = target
			}
		}
	}
	d.configureLocked(sub.SeriesFields)
	d.mu.Unlock()
}

func (d *DataFields) configure(names []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.configureLocked(names)
}

func (d *DataFields) configureLocked(names []string) {
	d.configured = true
	if len(names) == 0 {
		d.autoSeries = true
		return
	}
	changed := false
	for _, name := range names {
		if name == "" {
			continue
		}
		d.seriesFields[name] = true
		changed = d.addSeriesNameLocked(name) || changed
	}
	if changed {
		sort.Strings(d.seriesNames)
	}
}

func (d *DataFields) configureDefault() {
	d.mu.Lock()
	if !d.configured {
		d.configured = true
		d.autoSeries = true
	}
	d.mu.Unlock()
}

func (d *DataFields) MaxCache() int {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	limit := d.limit
	d.mu.RUnlock()
	return limit
}

// SetMaxCache overrides the maximum retained history length for this stream's
// Series and underlying BarEnv.
func (d *DataFields) SetMaxCache(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.limit = limit
	d.explicitLimit = true
	if d.env != nil {
		d.env.MaxCache = limit
	}
	for _, s := range d.seriesMap {
		if s != nil {
			s.Cut(limit)
		}
	}
}

func (d *DataFields) setHubLimit(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.explicitLimit {
		return
	}
	d.limit = limit
	if d.env != nil {
		d.env.MaxCache = limit
	}
	for _, s := range d.seriesMap {
		if s != nil {
			s.Cut(limit)
		}
	}
}

func (d *DataFields) raiseMinCache(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.explicitLimit || limit <= d.limit {
		return
	}
	d.limit = limit
	if d.env != nil {
		d.env.MaxCache = limit
	}
}

func (d *DataFields) symbolName() string {
	if d == nil {
		return ""
	}
	d.mu.RLock()
	sym := d.Symbol
	d.mu.RUnlock()
	return sym
}

// BarEnv returns the synchronized K-line *ta.BarEnv for K-line or OHLCV streams
// once at least one bar has arrived, or nil when not yet ready or non-OHLCV.
func (d *DataFields) BarEnv() *ta.BarEnv {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.isKlineEnv || d.env == nil || d.env.Close == nil {
		return nil
	}
	return d.env
}

func (d *DataFields) update(evt *orm.DataSeries, endMS int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.DoneMS > 0 && endMS <= d.DoneMS {
		return
	}
	d.TimeMS = evt.TimeMS
	d.DoneMS = endMS
	d.Source = orm.NormalizeSeriesSource(evt.Source)
	d.Sid = evt.Sid
	if evt.ExSymbol != nil && evt.ExSymbol.Symbol != "" {
		d.Symbol = evt.ExSymbol.Symbol
	}
	d.TimeFrame = evt.TimeFrame
	d.Closed = evt.Closed
	d.IsWarmUp = evt.IsWarmUp
	clear(d.rawMap)
	for name, value := range evt.Values {
		d.rawMap[name] = value
	}
	for name := range d.valMap {
		if _, present := evt.Values[name]; !present {
			delete(d.valMap, name)
		}
	}

	if !d.isKlineEnv && (d.Source == orm.SeriesSourceKline || evt.HasOHLCV()) {
		d.isKlineEnv = true
	}
	changed := false
	if d.isKlineEnv {
		for _, name := range klineBarEnvFields {
			changed = d.addSeriesNameLocked(name) || changed
		}
	}
	for name, value := range evt.Values {
		_, selected := d.seriesNameSet[name]
		if !selected && d.autoSeries && isFloatValue(value) {
			selected = true
			changed = d.addSeriesNameLocked(name) || changed
		}
		if !selected {
			d.valMap[name] = value
		} else {
			delete(d.valMap, name)
		}
	}
	if changed {
		sort.Strings(d.seriesNames)
	}
	if len(d.seriesNames) == 0 {
		return
	}
	if d.env == nil {
		exgName, market, symName := "", "", fmt.Sprintf("%s:%d", d.Source, evt.Sid)
		if evt.ExSymbol != nil {
			exgName = evt.ExSymbol.Exchange
			market = evt.ExSymbol.Market
			if evt.ExSymbol.Symbol != "" {
				symName = evt.ExSymbol.Symbol
			}
		} else if d.Symbol != "" {
			symName = d.Symbol
		}
		d.env, _ = ta.NewBarEnv(exgName, market, symName, evt.TimeFrame)
		if d.env == nil {
			d.env = &ta.BarEnv{
				Exchange:   exgName,
				MarketType: market,
				Symbol:     symName,
				TimeFrame:  evt.TimeFrame,
				MaxCache:   d.limit,
				Items:      make(map[int]*ta.Series),
			}
		}
		if d.limit > 0 {
			d.env.MaxCache = d.limit
		}
		if evt.Sid > 0 {
			d.env.Data.Store("sid", int64(evt.Sid))
		}
	}
	d.env.TimeStart = evt.TimeMS
	d.env.TimeStop = endMS
	d.env.BarNum++
	for _, name := range d.seriesNames {
		value := math.NaN()
		if raw, ok := evt.Values[name]; ok && raw != nil {
			if num, err := utils.ToFloat64(raw); err == nil {
				value = num
			}
		}
		series := d.seriesMap[name]
		if series == nil {
			series = d.env.NewSeries([]float64{value})
			series.Time = endMS
			d.seriesMap[name] = series
		} else {
			series.Append(value)
			if d.limit > 0 {
				series.Cut(d.limit)
			}
		}
		delete(d.valMap, name)
	}
	if d.isKlineEnv && d.env.Open == nil {
		d.env.Open = d.seriesMap["open"]
		d.env.High = d.seriesMap["high"]
		d.env.Low = d.seriesMap["low"]
		d.env.Close = d.seriesMap["close"]
		d.env.Volume = d.seriesMap["volume"]
		d.env.Quote = d.seriesMap["quote"]
		d.env.BuyVolume = d.seriesMap["buy_volume"]
		d.env.TradeNum = d.seriesMap["trade_num"]
	}
}

func (d *DataFields) addSeriesNameLocked(name string) bool {
	if _, exists := d.seriesNameSet[name]; exists {
		return false
	}
	if d.seriesNameSet == nil {
		d.seriesNameSet = make(map[string]struct{}, len(d.seriesFields)+len(d.seriesMap)+1)
	}
	d.seriesNameSet[name] = struct{}{}
	d.seriesNames = append(d.seriesNames, name)
	return true
}

func (d *DataFields) Series(name string) *ta.Series {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	series := d.seriesMap[name]
	d.mu.RUnlock()
	return series
}

func (d *DataFields) Float64(name string) float64 {
	value := d.Raw(name)
	result, _ := utils.ToFloat64(value)
	return result
}

func (d *DataFields) Int64(name string) int64 {
	value := d.Raw(name)
	switch num := value.(type) {
	case int:
		return int64(num)
	case int8:
		return int64(num)
	case int16:
		return int64(num)
	case int32:
		return int64(num)
	case int64:
		return num
	case uint:
		return int64(num)
	case uint8:
		return int64(num)
	case uint16:
		return int64(num)
	case uint32:
		return int64(num)
	case uint64:
		return int64(num)
	}
	result, _ := utils.ToFloat64(value)
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0
	}
	return int64(result)
}

func (d *DataFields) String(name string) string {
	value := d.Raw(name)
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func (d *DataFields) Raw(name string) any {
	value, _ := d.RawValue(name)
	return value
}

// RawValue returns the value from the latest event and whether that field was
// present in the event. The boolean distinguishes an explicit nil from a
// missing field; series(name) remains the derived numeric history.
func (d *DataFields) RawValue(name string) (any, bool) {
	if d == nil {
		return nil, false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	value, ok := d.rawMap[name]
	return value, ok
}

// Has reports whether the latest event contained name, including when its
// value was explicitly nil.
func (d *DataFields) Has(name string) bool {
	_, ok := d.RawValue(name)
	return ok
}

func (s *StratJob) defaultDataHubLimit() int {
	limit := DefaultDataHubCache
	if s != nil {
		if s.runtimeCore != nil && s.runtimeCore.NumTaCache > limit {
			limit = s.runtimeCore.NumTaCache
		} else if (s.strategyState == nil || s.strategyState == legacyState) && s.runtimeCore == nil && core.NumTaCache > limit {
			limit = core.NumTaCache
		}
		if s.Strat != nil && s.Strat.WarmupNum > 0 {
			limit = max(limit, s.Strat.WarmupNum+100)
		}
	}
	return limit
}

func newJobDataHub(job *StratJob) *DataHub {
	hub := NewDataHub()
	hub.raiseMinCache(job.defaultDataHubLimit())
	return hub
}

func (s *StratJob) SetData(evt *orm.DataSeries) *DataFields {
	if s == nil || evt == nil {
		return nil
	}
	minLimit := s.defaultDataHubLimit()
	if s.DataHub == nil {
		s.DataHub = NewDataHub()
	}
	s.DataHub.raiseMinCache(minLimit)
	if !s.dataHubConfigured {
		subs := CollectDataSubs(s)
		if s.Symbol != nil && s.TimeFrame != "" {
			subs = append(subs, &DataSub{
				Source: orm.SeriesSourceKline, ExSymbol: s.Symbol, TimeFrame: s.TimeFrame,
			})
		}
		s.DataHub.Configure(subs)
		s.dataHubConfigured = true
	}
	return s.DataHub.Set(evt)
}

func dataEndMS(evt *orm.DataSeries) int64 {
	if evt.EndMS > 0 {
		return evt.EndMS
	}
	secs, err := utils2.TFToSecSafe(evt.TimeFrame)
	if err != nil || secs <= 0 {
		return evt.TimeMS
	}
	return evt.TimeMS + int64(secs)*1000
}

func timeFrameClosesAt(tf string, curMS int64) bool {
	secs, err := utils2.TFToSecSafe(tf)
	if err != nil || secs <= 0 {
		return true
	}
	tfMS := int64(secs) * 1000
	_, offsetSecs := utils2.GetTfAlignOrigin(secs)
	delta := curMS - int64(offsetSecs)*1000
	return delta%tfMS == 0
}

func isFloatValue(value any) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}
