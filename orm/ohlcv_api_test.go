package orm

import (
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

var (
	_ func(*Queries, *ExSymbol, string, int64, int64, int, bool) ([]*banexg.Kline, *errs.Error)                                      = (*Queries).QueryOHLCV
	_ func(*Queries, map[int32]*ExSymbol, string, int64, int64, int, func(int32, []*banexg.Kline)) *errs.Error                       = (*Queries).QueryOHLCVBatch
	_ func(banexg.BanExchange, *ExSymbol, string, int64, int64, int, bool, *utils.PrgBar) ([]*AdjInfo, []*banexg.Kline, *errs.Error) = AutoFetchOHLCV
	_ func(*ExSymbol, string, int64, int64, int, bool) ([]*AdjInfo, []*banexg.Kline, *errs.Error)                                    = GetOHLCV
	_ func(*Queries, *ExSymbol, string, int64, int64, int, bool) ([]*AdjInfo, []*banexg.Kline, *errs.Error)                          = (*Queries).GetOHLCV
	_ func(*Queries, []*AdjInfo, string, int64, int64, int, bool) ([]*banexg.Kline, *errs.Error)                                     = (*Queries).GetAdjOHLCV

	_ func(*Queries, *ExSymbol, string, int64, int64, int, bool) ([]*DataSeries, *errs.Error)                                      = (*Queries).QuerySeries
	_ func(*Queries, map[int32]*ExSymbol, string, int64, int64, int, func(int32, []*DataSeries)) *errs.Error                       = (*Queries).QuerySeriesBatch
	_ func(banexg.BanExchange, *ExSymbol, string, int64, int64, int, bool, *utils.PrgBar) ([]*AdjInfo, []*DataSeries, *errs.Error) = AutoFetchSeries
	_ func(*ExSymbol, string, int64, int64, int, bool) ([]*AdjInfo, []*DataSeries, *errs.Error)                                    = GetSeries
	_ func(*Queries, *ExSymbol, string, int64, int64, int, bool) ([]*AdjInfo, []*DataSeries, *errs.Error)                          = (*Queries).GetSeries
	_ func(*Queries, []*AdjInfo, string, int64, int64, int, bool) ([]*DataSeries, *errs.Error)                                     = (*Queries).GetAdjSeries
)
