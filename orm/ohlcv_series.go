package orm

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func BarsToSeries(exs *ExSymbol, tf string, bars []*banexg.Kline, adj *AdjInfo, isWarmUp, closed bool) []*DataSeries {
	if len(bars) == 0 {
		return nil
	}
	rows := make([]*DataSeries, 0, len(bars))
	for _, bar := range bars {
		if bar == nil {
			continue
		}
		rows = append(rows, NewDataSeriesFromKline(exs, tf, bar, adj, isWarmUp, closed))
	}
	return rows
}

func SeriesToBars(exs *ExSymbol, rows []*DataSeries) ([]*banexg.Kline, *errs.Error) {
	if len(rows) == 0 {
		return nil, nil
	}
	items := make([]*banexg.Kline, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		view, err := row.OHLCV(exs)
		if err != nil {
			return nil, errs.New(core.ErrInvalidBars, err)
		}
		items = append(items, view.Bar())
	}
	return items, nil
}

func (evt *DataSeries) BatchTimeMS() int64 {
	if evt == nil {
		return 0
	}
	return evt.TimeMS
}

func (q *Queries) QuerySeries(exs *ExSymbol, timeframe string, startMs, endMs int64, limit int, withUnFinish bool) ([]*DataSeries, *errs.Error) {
	bars, err := q.QueryOHLCV(exs, timeframe, startMs, endMs, limit, withUnFinish)
	if err != nil {
		return nil, err
	}
	return BarsToSeries(exs, timeframe, bars, nil, false, true), nil
}

func (q *Queries) QuerySeriesBatch(exsMap map[int32]*ExSymbol, timeframe string, startMs, endMs int64, limit int, handle func(int32, []*DataSeries)) *errs.Error {
	return q.QueryOHLCVBatch(exsMap, timeframe, startMs, endMs, limit, func(sid int32, bars []*banexg.Kline) {
		handle(sid, BarsToSeries(exsMap[sid], timeframe, bars, nil, false, true))
	})
}

func (q *Queries) InsertSeries(timeFrame string, exs *ExSymbol, rows []*DataSeries, aggBig bool) (int64, *errs.Error) {
	bars, err := SeriesToBars(exs, rows)
	if err != nil {
		return 0, err
	}
	return q.InsertKLinesAuto(timeFrame, exs, bars, aggBig)
}

func (q *Queries) UpdateSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, rows []*DataSeries, aggBig bool, skipHoles ...bool) *errs.Error {
	var bars []*banexg.Kline
	if len(rows) > 0 {
		var err *errs.Error
		bars, err = SeriesToBars(exs, rows)
		if err != nil {
			return err
		}
	}
	return q.UpdateKRange(exs, timeFrame, startMS, endMS, bars, aggBig, skipHoles...)
}

func AutoFetchSeries(exchange banexg.BanExchange, exs *ExSymbol, timeFrame string, startMS, endMS int64,
	limit int, withUnFinish bool, pBar *utils.PrgBar) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	adjs, bars, err := AutoFetchOHLCV(exchange, exs, timeFrame, startMS, endMS, limit, withUnFinish, pBar)
	if err != nil {
		return nil, nil, err
	}
	return adjs, BarsToSeries(exs, timeFrame, bars, nil, false, true), nil
}

func GetSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	adjs, bars, err := GetOHLCV(exs, timeFrame, startMS, endMS, limit, withUnFinish)
	if err != nil {
		return nil, nil, err
	}
	return adjs, BarsToSeries(exs, timeFrame, bars, nil, false, true), nil
}

func (q *Queries) GetSeries(exs *ExSymbol, timeFrame string, startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	adjs, bars, err := q.GetOHLCV(exs, timeFrame, startMS, endMS, limit, withUnFinish)
	if err != nil {
		return nil, nil, err
	}
	return adjs, BarsToSeries(exs, timeFrame, bars, nil, false, true), nil
}
