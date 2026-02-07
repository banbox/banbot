package orm

import (
	"context"
	"fmt"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func (q *PubQueries) PurgeKlineUn() *errs.Error {
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err_ := db.ExecContext(context.Background(), "delete from kline_un")
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (q *PubQueries) DelKInfo(sid int32, timeFrame string) *errs.Error {
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err_ := db.ExecContext(context.Background(), `delete from sranges where sid=? and tbl=? and timeframe=?`, sid, "kline_"+timeFrame, timeFrame)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (q *PubQueries) GetKlineRange(sid int32, timeFrame string) (int64, int64) {
	db, err := BanPubConn(false)
	if err != nil {
		return 0, 0
	}
	defer db.Close()
	row := db.QueryRowContext(context.Background(), `
select min(start_ms), max(stop_ms)
from sranges
where sid=? and tbl=? and timeframe=? and has_data=1`,
		sid, "kline_"+timeFrame, timeFrame,
	)
	var start, stop *int64
	_ = row.Scan(&start, &stop)
	if start == nil || stop == nil {
		return 0, 0
	}
	return *start, *stop
}

func (q *PubQueries) GetKlineRanges(sidList []int32, timeFrame string) map[int32][2]int64 {
	if len(sidList) == 0 {
		return map[int32][2]int64{}
	}
	var texts = make([]string, len(sidList))
	for i, sid := range sidList {
		texts[i] = fmt.Sprintf("%v", sid)
	}
	sidText := strings.Join(texts, ", ")
	sql := fmt.Sprintf(`
select sid, min(start_ms), max(stop_ms)
from sranges
where tbl=? and timeframe=? and has_data=1 and sid in (%v)
group by sid`, sidText)
	db, err := BanPubConn(false)
	if err != nil {
		return map[int32][2]int64{}
	}
	defer db.Close()
	rows, err_ := db.QueryContext(context.Background(), sql, "kline_"+timeFrame, timeFrame)
	if err_ != nil {
		return map[int32][2]int64{}
	}
	res := make(map[int32][2]int64)
	defer rows.Close()
	for rows.Next() {
		var start, stop int64
		var sid int32
		err_ = rows.Scan(&sid, &start, &stop)
		if err_ != nil {
			continue
		}
		res[sid] = [2]int64{start, stop}
	}
	return res
}

func (q *PubQueries) DelFactors(sid int32, startMS, endMS int64) *errs.Error {
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()
	sqlText := "delete from adj_factors where (sid=? or sub_id=?)"
	args := []any{sid, sid}
	if startMS > 0 {
		sqlText += " and start_ms >= ?"
		args = append(args, startMS)
	}
	if endMS > 0 {
		sqlText += " and start_ms < ?"
		args = append(args, endMS)
	}
	_, err_ := db.ExecContext(context.Background(), sqlText, args...)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (q *PubQueries) DelKLineUn(sid int32, timeFrame string) *errs.Error {
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err_ := db.ExecContext(context.Background(), "delete from kline_un where sid=? and timeframe=?", sid, timeFrame)
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}
