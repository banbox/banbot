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

func AsKline(evt *DataSeries, extras ...any) (*InfoKline, error) {
	if evt == nil {
		return nil, fmt.Errorf("series event is nil")
	}
	exs := evt.ExSymbol
	if exs == nil {
		for _, item := range extras {
			if val, ok := item.(*ExSymbol); ok && val != nil {
				exs = val
				break
			}
		}
	}
	if exs == nil && evt.Sid > 0 {
		exs = GetSymbolByID(evt.Sid)
	}
	if exs == nil {
		return nil, fmt.Errorf("series event sid %d has no exsymbol", evt.Sid)
	}
	open, err := seriesFloatValue(evt.Values, "open")
	if err != nil {
		return nil, err
	}
	high, err := seriesFloatValue(evt.Values, "high")
	if err != nil {
		return nil, err
	}
	low, err := seriesFloatValue(evt.Values, "low")
	if err != nil {
		return nil, err
	}
	closeVal, err := seriesFloatValue(evt.Values, "close")
	if err != nil {
		return nil, err
	}
	volume, err := seriesFloatValue(evt.Values, "volume")
	if err != nil {
		return nil, err
	}
	quote, _ := seriesFloatValueDefault(evt.Values, "quote")
	buyVolume, _ := seriesFloatValueDefault(evt.Values, "buy_volume")
	tradeNum, _ := seriesIntValueDefault(evt.Values, "trade_num")
	return &InfoKline{
		PairTFKline: &banexg.PairTFKline{
			Symbol:    exs.Symbol,
			TimeFrame: evt.TimeFrame,
			Kline: banexg.Kline{
				Time:      evt.TimeMS,
				Open:      open,
				High:      high,
				Low:       low,
				Close:     closeVal,
				Volume:    volume,
				Quote:     quote,
				BuyVolume: buyVolume,
				TradeNum:  tradeNum,
			},
		},
		Sid:      exs.ID,
		Adj:      evt.Adj,
		IsWarmUp: evt.IsWarmUp,
	}, nil
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
