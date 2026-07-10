package orm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"
)

type SeriesField struct {
	Name string
	Type string
	Role string
}

const SeriesSourceKline = "kline"

var klineDefaultFields = []string{
	"open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num",
}

func DefaultKlineFields() []string {
	return append([]string(nil), klineDefaultFields...)
}

func NormalizeSeriesFields(source string, fields []string) []string {
	if len(fields) == 0 && NormalizeSeriesSource(source) == SeriesSourceKline {
		return DefaultKlineFields()
	}
	seen := make(map[string]bool, len(fields))
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" || seen[field] {
			continue
		}
		seen[field] = true
		out = append(out, field)
	}
	return out
}

func MergeSeriesFields(groups ...[]string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, fields := range groups {
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" || seen[field] {
				continue
			}
			seen[field] = true
			out = append(out, field)
		}
	}
	return out
}

type SeriesBinding struct {
	Table      string
	TimeColumn string
	EndColumn  string
	SIDColumn  string
	Fields     []SeriesField
}

type SeriesInfo struct {
	Name      string
	TimeFrame string
	Binding   SeriesBinding
}

func SeriesTableName(name, timeFrame string) string {
	if timeFrame == "" {
		return name
	}
	return fmt.Sprintf("%s_%s", name, timeFrame)
}

func NewSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo {
	return &SeriesInfo{
		Name:      name,
		TimeFrame: timeFrame,
		Binding: SeriesBinding{
			Table:      SeriesTableName(name, timeFrame),
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields:     fields,
		},
	}
}

type DataRecord struct {
	Sid    int32
	TimeMS int64
	EndMS  int64
	Closed bool
	Values map[string]any
}

type DataSeries struct {
	Source    string
	Sid       int32
	TimeMS    int64
	EndMS     int64
	TimeFrame string
	Closed    bool
	IsWarmUp  bool
	Values    map[string]any
	ExSymbol  *ExSymbol
	Adj       *AdjInfo
}

func ResolveSeriesExSymbol(evt *DataSeries, extras ...*ExSymbol) *ExSymbol {
	if evt == nil {
		return nil
	}
	if evt.ExSymbol != nil {
		return evt.ExSymbol
	}
	for _, exs := range extras {
		if exs != nil {
			return exs
		}
	}
	if evt.Sid > 0 {
		return GetSymbolByID(evt.Sid)
	}
	return nil
}

func (evt *DataSeries) CloneWithExSymbol(exs *ExSymbol) *DataSeries {
	if evt == nil {
		return nil
	}
	cp := *evt
	if exs == nil {
		exs = ResolveSeriesExSymbol(evt)
	}
	cp.ExSymbol = exs
	if cp.Sid == 0 && exs != nil {
		cp.Sid = exs.ID
	}
	return &cp
}

func (evt *DataSeries) Symbol() string {
	exs := ResolveSeriesExSymbol(evt)
	if exs == nil {
		return ""
	}
	return exs.Symbol
}

func (evt *DataSeries) EnsureExSymbol(extras ...*ExSymbol) *ExSymbol {
	if evt == nil {
		return nil
	}
	exs := ResolveSeriesExSymbol(evt, extras...)
	if exs != nil {
		evt.ExSymbol = exs
		if evt.Sid == 0 {
			evt.Sid = exs.ID
		}
	}
	return exs
}

func (evt *DataSeries) FloatValue(key string) (float64, error) {
	if evt == nil {
		return 0, fmt.Errorf("series event is nil")
	}
	return seriesFloatValue(evt.Values, key)
}

func (evt *DataSeries) FloatValueDefault(key string) (float64, bool) {
	if evt == nil {
		return 0, false
	}
	return seriesFloatValueDefault(evt.Values, key)
}

func (evt *DataSeries) IntValueDefault(key string) (int64, bool) {
	if evt == nil {
		return 0, false
	}
	return seriesIntValueDefault(evt.Values, key)
}

func (evt *DataSeries) OpenValue() (float64, error) {
	return evt.FloatValue("open")
}

func (evt *DataSeries) HighValue() (float64, error) {
	return evt.FloatValue("high")
}

func (evt *DataSeries) LowValue() (float64, error) {
	return evt.FloatValue("low")
}

func (evt *DataSeries) CloseValue() (float64, error) {
	return evt.FloatValue("close")
}

func (evt *DataSeries) VolumeValue() (float64, error) {
	return evt.FloatValue("volume")
}

func (evt *DataSeries) QuoteValue() float64 {
	val, _ := evt.FloatValueDefault("quote")
	return val
}

func (evt *DataSeries) BuyVolumeValue() float64 {
	val, _ := evt.FloatValueDefault("buy_volume")
	return val
}

func (evt *DataSeries) TradeNumValue() int64 {
	val, _ := evt.IntValueDefault("trade_num")
	return val
}

func (evt *DataSeries) HasOHLCV() bool {
	if evt == nil {
		return false
	}
	for _, key := range []string{"open", "high", "low", "close", "volume"} {
		if _, ok := evt.Values[key]; !ok {
			return false
		}
	}
	return true
}

func NormalizeSeriesSource(source string) string {
	if source == "" {
		return SeriesSourceKline
	}
	return source
}

func NewDataSeriesFromKline(exs *ExSymbol, tf string, bar *banexg.Kline, adj *AdjInfo, isWarmUp, closed bool) *DataSeries {
	if bar == nil {
		return nil
	}
	sid := int32(0)
	if exs != nil {
		sid = exs.ID
	}
	return &DataSeries{
		Source:    SeriesSourceKline,
		Sid:       sid,
		TimeMS:    bar.Time,
		EndMS:     bar.Time + int64(utils2.TFToSecs(tf))*1000,
		TimeFrame: tf,
		Closed:    closed,
		IsWarmUp:  isWarmUp,
		Values: map[string]any{
			"open":       bar.Open,
			"high":       bar.High,
			"low":        bar.Low,
			"close":      bar.Close,
			"volume":     bar.Volume,
			"quote":      bar.Quote,
			"buy_volume": bar.BuyVolume,
			"trade_num":  bar.TradeNum,
		},
		ExSymbol: exs,
		Adj:      adj,
	}
}

func NewDataSeriesFromInfoKline(bar *InfoKline) *DataSeries {
	if bar == nil || bar.PairTFKline == nil {
		return nil
	}
	var exs *ExSymbol
	if bar.Adj != nil {
		exs = bar.Adj.ExSymbol
	}
	evt := NewDataSeriesFromKline(exs, bar.TimeFrame, &bar.Kline, bar.Adj, bar.IsWarmUp, true)
	if evt != nil && evt.Sid == 0 {
		evt.Sid = bar.Sid
	}
	return evt
}

func KlineToDataSeries(bar *InfoKline) *DataSeries {
	return NewDataSeriesFromInfoKline(bar)
}

type seriesOHLCVFields struct {
	open      float64
	high      float64
	low       float64
	close     float64
	volume    float64
	quote     float64
	buyVolume float64
	tradeNum  int64
}

func (evt *DataSeries) readOHLCVFields() (seriesOHLCVFields, error) {
	if evt == nil {
		return seriesOHLCVFields{}, fmt.Errorf("series event is nil")
	}
	open, err := evt.OpenValue()
	if err != nil {
		return seriesOHLCVFields{}, err
	}
	high, err := evt.HighValue()
	if err != nil {
		return seriesOHLCVFields{}, err
	}
	low, err := evt.LowValue()
	if err != nil {
		return seriesOHLCVFields{}, err
	}
	closeVal, err := evt.CloseValue()
	if err != nil {
		return seriesOHLCVFields{}, err
	}
	volume, err := evt.VolumeValue()
	if err != nil {
		return seriesOHLCVFields{}, err
	}
	return seriesOHLCVFields{
		open:      open,
		high:      high,
		low:       low,
		close:     closeVal,
		volume:    volume,
		quote:     evt.QuoteValue(),
		buyVolume: evt.BuyVolumeValue(),
		tradeNum:  evt.TradeNumValue(),
	}, nil
}

func (evt *DataSeries) resolveOHLCV(extras ...*ExSymbol) (*ExSymbol, seriesOHLCVFields, error) {
	if evt == nil {
		return nil, seriesOHLCVFields{}, fmt.Errorf("series event is nil")
	}
	exs := ResolveSeriesExSymbol(evt, extras...)
	if exs == nil {
		return nil, seriesOHLCVFields{}, fmt.Errorf("series event sid %d has no exsymbol", evt.Sid)
	}
	fields, err := evt.readOHLCVFields()
	if err != nil {
		return nil, seriesOHLCVFields{}, err
	}
	return exs, fields, nil
}

func (fields seriesOHLCVFields) kline(timeMS int64) *banexg.Kline {
	return &banexg.Kline{
		Time:      timeMS,
		Open:      fields.open,
		High:      fields.high,
		Low:       fields.low,
		Close:     fields.close,
		Volume:    fields.volume,
		Quote:     fields.quote,
		BuyVolume: fields.buyVolume,
		TradeNum:  fields.tradeNum,
	}
}

func (evt *DataSeries) OHLCV(extras ...*ExSymbol) (*SeriesOHLCV, error) {
	exs, fields, err := evt.resolveOHLCV(extras...)
	if err != nil {
		return nil, err
	}
	return &SeriesOHLCV{
		Sid:       exs.ID,
		ExSymbol:  exs,
		Source:    evt.Source,
		Time:      evt.TimeMS,
		EndMS:     evt.EndMS,
		TimeFrame: evt.TimeFrame,
		Open:      fields.open,
		High:      fields.high,
		Low:       fields.low,
		Close:     fields.close,
		Volume:    fields.volume,
		Quote:     fields.quote,
		BuyVolume: fields.buyVolume,
		TradeNum:  fields.tradeNum,
		Adj:       evt.Adj,
		IsWarmUp:  evt.IsWarmUp,
		Closed:    evt.Closed,
	}, nil
}

func AsKline(evt *DataSeries, extras ...any) (*InfoKline, error) {
	var extraExs *ExSymbol
	for _, item := range extras {
		if val, ok := item.(*ExSymbol); ok && val != nil {
			extraExs = val
			break
		}
	}
	view, err := evt.OHLCV(extraExs)
	if err != nil {
		return nil, err
	}
	return view.ToInfoKline(), nil
}

func KlineToSeries(bar *InfoKline) *DataSeries {
	return KlineToDataSeries(bar)
}

func ResampleSeriesRecords(info *SeriesInfo, exs *ExSymbol, rows []*DataRecord, toTFMS, offMS int64) ([]*DataRecord, error) {
	if info == nil {
		return nil, fmt.Errorf("series info is required")
	}
	if toTFMS <= 0 {
		return nil, fmt.Errorf("target timeframe must be greater than zero")
	}
	if len(rows) == 0 {
		return nil, nil
	}
	_, offset := utils2.GetTfAlignOrigin(int(toTFMS / 1000))
	alignOffMS := int64(offset * 1000)
	fields := normalizedSeriesBinding(info.Binding).Fields
	var out []*DataRecord
	var bucket []*DataRecord
	var bucketTime int64
	flush := func() error {
		if len(bucket) == 0 {
			return nil
		}
		values := make(map[string]any, len(fields))
		for _, field := range fields {
			fn, ok := GetAggRuleFunc(seriesAggRule(info, exs, field.Name))
			if !ok {
				fn, _ = GetAggRuleFunc("last")
			}
			val, err := fn(bucket, field)
			if err != nil {
				return err
			}
			values[field.Name] = val
		}
		out = append(out, &DataRecord{
			Sid:    bucket[0].Sid,
			TimeMS: bucketTime,
			EndMS:  bucketTime + toTFMS,
			Closed: bucket[len(bucket)-1].Closed,
			Values: values,
		})
		bucket = nil
		return nil
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		timeAlign := utils2.AlignTfMSecsOffset(row.TimeMS+offMS, toTFMS, alignOffMS)
		if len(bucket) > 0 && timeAlign != bucketTime {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if len(bucket) == 0 {
			bucketTime = timeAlign
		}
		bucket = append(bucket, row)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

func ResampleDataSeries(exs *ExSymbol, tf string, rows, prev []*DataSeries, toTFMS int64, preFire float64, fromTFMS, offMS int64, isWarmUp bool) ([]*DataSeries, bool, error) {
	if len(rows) == 0 {
		return nil, false, nil
	}
	if tf == "" {
		tf = utils2.SecsToTF(int(toTFMS / 1000))
	}
	source := NormalizeSeriesSource(rows[0].Source)
	if source == SeriesSourceKline {
		return resampleOHLCVSeries(exs, tf, rows, prev, toTFMS, preFire, fromTFMS, offMS, isWarmUp)
	}
	info := NewSeriesInfo(source, tf, inferSeriesFields(prev, rows))
	records := make([]*DataRecord, 0, len(prev)+len(rows))
	for _, row := range prev {
		if rec := SeriesToRecord(row); rec != nil {
			records = append(records, rec)
		}
	}
	for _, row := range rows {
		if rec := SeriesToRecord(row); rec != nil {
			records = append(records, rec)
		}
	}
	offsetMS := int64(float64(toTFMS)*preFire) + offMS
	aggRecords, err := ResampleSeriesRecords(info, exs, records, toTFMS, offsetMS)
	if err != nil {
		return nil, false, err
	}
	out := RecordsToSeries(info, exs, aggRecords)
	for _, row := range out {
		row.IsWarmUp = isWarmUp
	}
	lastFinished := false
	if fromTFMS > 0 && len(out) > 0 {
		_, offset := utils2.GetTfAlignOrigin(int(toTFMS / 1000))
		alignOffMS := int64(offset * 1000)
		finishMS := utils2.AlignTfMSecsOffset(rows[len(rows)-1].TimeMS+fromTFMS+offsetMS, toTFMS, alignOffMS)
		lastFinished = finishMS > out[len(out)-1].TimeMS
	}
	return out, lastFinished, nil
}

func inferSeriesFields(groups ...[]*DataSeries) []SeriesField {
	seen := make(map[string]any)
	for _, rows := range groups {
		for _, row := range rows {
			if row == nil {
				continue
			}
			for key, val := range row.Values {
				if _, ok := seen[key]; !ok {
					seen[key] = val
				}
			}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]SeriesField, 0, len(keys))
	for _, key := range keys {
		fields = append(fields, SeriesField{Name: key, Type: inferSeriesFieldType(seen[key])})
	}
	return fields
}

func inferSeriesFieldType(val any) string {
	switch val.(type) {
	case bool:
		return "bool"
	case string:
		return "string"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case float32, float64:
		return "float"
	default:
		return "json"
	}
}

func seriesAggRule(info *SeriesInfo, exs *ExSymbol, field string) string {
	if exs != nil {
		if rule, ok := exs.AggRuleMap()[field]; ok {
			return rule
		}
	}
	if info != nil && NormalizeSeriesSource(info.Name) == SeriesSourceKline {
		switch field {
		case "open":
			return "first"
		case "high":
			return "max"
		case "low":
			return "min"
		case "close":
			return "last"
		case "volume", "quote", "buy_volume", "trade_num":
			return "sum"
		}
	}
	return "last"
}

func seriesFloatValue(values map[string]any, key string) (float64, error) {
	if values == nil {
		return 0, fmt.Errorf("series event missing field %q", key)
	}
	val, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("series event missing field %q", key)
	}
	return seriesFloatAny(val)
}

func seriesFloatValueDefault(values map[string]any, key string) (float64, bool) {
	if values == nil {
		return 0, false
	}
	val, ok := values[key]
	if !ok {
		return 0, false
	}
	num, err := seriesFloatAny(val)
	if err != nil {
		return 0, false
	}
	return num, true
}

func seriesIntValueDefault(values map[string]any, key string) (int64, bool) {
	if values == nil {
		return 0, false
	}
	val, ok := values[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		num, err := strconv.ParseInt(v, 10, 64)
		return num, err == nil
	default:
		return 0, false
	}
}
