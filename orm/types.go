package orm

import (
	"github.com/banbox/banexg"
)

type KlineAgg struct {
	TimeFrame string
	MSecs     int64
	Table     string
	AggFrom   string
	AggStart  string
	AggEnd    string
	AggEvery  string
	CpsBefore string
	Retention string
}

type AdjInfo struct {
	*ExSymbol
	Factor    float64 // Original adjacent weighting factor 原始相邻复权因子
	CumFactor float64 // Cumulative weighting factor 累计复权因子
	StartMS   int64   // start timestamp 开始时间
	StopMS    int64   // stop timestamp 结束时间
}

type InfoKline struct {
	*banexg.PairTFKline
	Sid      int32
	Adj      *AdjInfo
	IsWarmUp bool
}

type SeriesOHLCV struct {
	Sid       int32
	ExSymbol  *ExSymbol
	Source    string
	Time      int64
	EndMS     int64
	TimeFrame string
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Quote     float64
	BuyVolume float64
	TradeNum  int64
	Adj       *AdjInfo
	IsWarmUp  bool
	Closed    bool
}

func (s *SeriesOHLCV) Symbol() string {
	if s == nil || s.ExSymbol == nil {
		return ""
	}
	return s.ExSymbol.Symbol
}

func (s *SeriesOHLCV) Bar() *banexg.Kline {
	if s == nil {
		return nil
	}
	return &banexg.Kline{
		Time:      s.Time,
		Open:      s.Open,
		High:      s.High,
		Low:       s.Low,
		Close:     s.Close,
		Volume:    s.Volume,
		Quote:     s.Quote,
		BuyVolume: s.BuyVolume,
		TradeNum:  s.TradeNum,
	}
}

func (s *SeriesOHLCV) ToInfoKline() *InfoKline {
	if s == nil || s.ExSymbol == nil {
		return nil
	}
	bar := s.Bar()
	if bar == nil {
		return nil
	}
	return &InfoKline{
		PairTFKline: &banexg.PairTFKline{
			Symbol:    s.ExSymbol.Symbol,
			TimeFrame: s.TimeFrame,
			Kline:     *bar,
		},
		Sid:      s.Sid,
		Adj:      s.Adj,
		IsWarmUp: s.IsWarmUp,
	}
}
