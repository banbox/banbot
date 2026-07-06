package orm

import (
	"context"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func ValidateSeriesInfo(info *SeriesInfo) *errs.Error {
	return validateSeriesInfo(info)
}

func MissingSeriesRanges(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) ([]MSRange, *errs.Error) {
	rangeRepo, ok := DefaultSeriesRepo().(SeriesRangeRepo)
	if !ok {
		return nil, errs.NewMsg(core.ErrBadConfig, "series repository does not support missing range queries")
	}
	return rangeRepo.MissingSeriesRanges(ctx, info, sid, startMS, endMS)
}

func UpdateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	covered := make([]MSRange, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		if row.Sid != 0 && row.Sid != sid {
			return errs.NewMsg(core.ErrBadConfig, "series row sid %d does not match target sid %d", row.Sid, sid)
		}
		if row.EndMS <= row.TimeMS {
			return errs.NewMsg(core.ErrBadConfig, "series row end_ms must be greater than time_ms")
		}
		curStart := max(startMS, row.TimeMS)
		curEnd := min(endMS, row.EndMS)
		if curEnd > curStart {
			covered = append(covered, MSRange{Start: curStart, Stop: curEnd})
		}
	}
	holes := subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, mergeMSRanges(covered))
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if err_ := q.UpdateSRangesWithHoles(ctx, sid, info.Binding.Table, info.TimeFrame, startMS, endMS, holes); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}
