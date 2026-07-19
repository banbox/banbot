package strat

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
)

type DataHub struct {
	mu        sync.RWMutex
	sourceMap map[string]map[string]map[int32]*DataFields // timeframe:source:sid
	curMS     int64
	limit     int
}

type DataFields struct {
	mu            sync.RWMutex
	seriesMap     map[string]*ta.Series
	valMap        map[string]any
	DoneMS        int64
	TimeMS        int64
	Source        string
	Sid           int32
	TimeFrame     string
	Closed        bool
	IsWarmUp      bool
	env           *ta.BarEnv
	seriesFields  map[string]bool
	seriesNames   []string
	seriesNameSet map[string]struct{}
	autoSeries    bool
	configured    bool
	limit         int
}

func NewDataHub(limit ...int) *DataHub {
	curLimit := 512
	if len(limit) > 0 && limit[0] > 0 {
		curLimit = limit[0]
	}
	return &DataHub{
		sourceMap: make(map[string]map[string]map[int32]*DataFields),
		limit:     curLimit,
	}
}

// Configure pre-registers subscriptions so AllReady also accounts for data
// streams that have not emitted their first value yet.
func (d *DataHub) Configure(subs []*DataSub) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, sub := range subs {
		if sub == nil || sub.ExSymbol == nil || sub.TimeFrame == "" {
			continue
		}
		fields := d.ensureLocked(sub.TimeFrame, sub.Source, sub.ExSymbol.ID)
		fields.configure(sub.SeriesFields)
	}
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
			valMap:       make(map[string]any),
			seriesFields: make(map[string]bool),
			limit:        d.limit,
		}
		bySID[sid] = fields
	}
	return fields
}

func (d *DataFields) configure(names []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
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
	d.TimeFrame = evt.TimeFrame
	d.Closed = evt.Closed
	d.IsWarmUp = evt.IsWarmUp

	changed := false
	for name, value := range evt.Values {
		_, selected := d.seriesNameSet[name]
		if !selected && d.autoSeries && isFloatValue(value) {
			selected = true
			changed = d.addSeriesNameLocked(name) || changed
		}
		if !selected {
			d.valMap[name] = value
		}
	}
	if changed {
		sort.Strings(d.seriesNames)
	}
	if len(d.seriesNames) == 0 {
		return
	}
	if d.env == nil {
		d.env, _ = ta.NewBarEnv("", "", fmt.Sprintf("%s:%d", evt.Source, evt.Sid), evt.TimeFrame)
		if d.env == nil {
			return
		}
		d.env.MaxCache = d.limit
	}
	d.env.TimeStart = evt.TimeMS
	d.env.TimeStop = endMS
	d.env.BarNum++
	for _, name := range d.seriesNames {
		value := math.NaN()
		if raw, ok := evt.Values[name]; ok {
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
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if series := d.seriesMap[name]; series != nil {
		return series.Get(0)
	}
	return d.valMap[name]
}

func (s *StratJob) SetData(evt *orm.DataSeries) *DataFields {
	if s == nil || evt == nil {
		return nil
	}
	if s.DataHub == nil {
		s.DataHub = NewDataHub()
	}
	subs := CollectDataSubs(s)
	if s.Symbol != nil && s.TimeFrame != "" {
		subs = append(subs, &DataSub{
			Source: orm.SeriesSourceKline, ExSymbol: s.Symbol, TimeFrame: s.TimeFrame,
		})
	}
	s.DataHub.Configure(subs)
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
