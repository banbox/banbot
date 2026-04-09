package orm

import (
	"fmt"
	"strconv"

	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"
)

type SeriesField struct {
	Name string
	Type string
	Role string
}

const SeriesSourceKline = "kline"

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

func (evt *DataSeries) OHLCV(extras ...*ExSymbol) (*SeriesOHLCV, error) {
	if evt == nil {
		return nil, fmt.Errorf("series event is nil")
	}
	exs := ResolveSeriesExSymbol(evt, extras...)
	if exs == nil {
		return nil, fmt.Errorf("series event sid %d has no exsymbol", evt.Sid)
	}
	open, err := evt.OpenValue()
	if err != nil {
		return nil, err
	}
	high, err := evt.HighValue()
	if err != nil {
		return nil, err
	}
	low, err := evt.LowValue()
	if err != nil {
		return nil, err
	}
	closeVal, err := evt.CloseValue()
	if err != nil {
		return nil, err
	}
	volume, err := evt.VolumeValue()
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
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closeVal,
		Volume:    volume,
		Quote:     evt.QuoteValue(),
		BuyVolume: evt.BuyVolumeValue(),
		TradeNum:  evt.TradeNumValue(),
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
