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
