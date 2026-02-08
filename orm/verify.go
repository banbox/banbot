package orm

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

func ParseVerifyArgs(args *config.CmdArgs) (*VerifyArgs, *errs.Error) {
	vArgs := &VerifyArgs{
		BatchSize: args.BatchSize,
	}
	if len(args.Tables) > 0 {
		vArgs.Tables = args.Tables
	}
	for _, pair := range args.Pairs {
		if strings.HasPrefix(pair, "sid:") {
			sidStr := pair[4:]
			sid, err_ := strconv.Atoi(sidStr)
			if err_ != nil {
				return nil, errs.NewMsg(errs.CodeParamInvalid, "invalid sid: %s", sidStr)
			}
			vArgs.Sids = append(vArgs.Sids, int32(sid))
		} else {
			exs := GetExSymbol2(config.Exchange.Name, core.Market, pair)
			if exs == nil {
				return nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol not found: %s", pair)
			}
			vArgs.Sids = append(vArgs.Sids, exs.ID)
		}
	}
	return vArgs, nil
}

type VerifyResult struct {
	Sid       int32
	Symbol    string
	Table     string
	TimeFrame string
	SRangeID  int64
	StartMs   int64
	StopMs    int64
	Expected  int
	Actual    int
	Problem   string
}

type VerifyArgs struct {
	Sids      []int32
	Tables    []string // 留空默认检查kline
	BatchSize int      // 分批读取大小，默认50000
}

func VerifyDataRanges(args *VerifyArgs) ([]*VerifyResult, *errs.Error) {
	if args.BatchSize <= 0 {
		args.BatchSize = 50000
	}
	tables := args.Tables
	if len(tables) == 0 {
		tables = []string{"kline"}
	}
	sids := args.Sids
	if len(sids) == 0 {
		// 检查所有有sranges记录的sid
		allSids, err := listSRangeSids(tables)
		if err != nil {
			return nil, err
		}
		sids = allSids
	}
	if len(sids) == 0 {
		log.Info("no symbols to verify")
		return nil, nil
	}
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })

	sess, conn, err := Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	totalJobs := len(sids) * len(tables)
	pBar := utils.NewPrgBar(totalJobs, "verify")
	defer pBar.Close()

	var results []*VerifyResult
	for _, sid := range sids {
		exs := GetSymbolByID(sid)
		symbol := fmt.Sprintf("sid:%d", sid)
		if exs != nil {
			symbol = exs.Symbol
		}
		for _, tblPrefix := range tables {
			pBar.Add(1)
			sranges, err_ := PubQ().ListSRangesBySid(context.Background(), sid)
			if err_ != nil {
				return results, NewDbErr(core.ErrDbReadFail, err_)
			}
			// 按table+timeframe分组
			type tfKey struct {
				Table     string
				TimeFrame string
			}
			grouped := make(map[tfKey][]*SRange)
			for _, r := range sranges {
				if !strings.HasPrefix(r.Table, tblPrefix+"_") {
					continue
				}
				k := tfKey{Table: r.Table, TimeFrame: r.Timeframe}
				grouped[k] = append(grouped[k], r)
			}
			for key, ranges := range grouped {
				for _, r := range ranges {
					if !r.HasData {
						continue
					}
					res := verifySRange(sess, sid, symbol, key.Table, key.TimeFrame, r, args.BatchSize)
					if res != nil {
						results = append(results, res)
					}
				}
			}
		}
	}
	return results, nil
}

func verifySRange(sess *Queries, sid int32, symbol, table, timeFrame string, r *SRange, batchSize int) *VerifyResult {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	if tfMSecs <= 0 {
		return &VerifyResult{
			Sid: sid, Symbol: symbol, Table: table, TimeFrame: timeFrame,
			SRangeID: r.ID, StartMs: r.StartMs, StopMs: r.StopMs,
			Problem: "invalid timeframe",
		}
	}
	maxBars := int((r.StopMs - r.StartMs) / tfMSecs)
	if maxBars <= 0 {
		return &VerifyResult{
			Sid: sid, Symbol: symbol, Table: table, TimeFrame: timeFrame,
			SRangeID: r.ID, StartMs: r.StartMs, StopMs: r.StopMs,
			Expected: maxBars, Problem: "invalid range (stop <= start)",
		}
	}

	// 分批次统计实际k线数量
	actual := 0
	curStart := r.StartMs
	for curStart < r.StopMs {
		curEnd := curStart + int64(batchSize)*tfMSecs
		if curEnd > r.StopMs {
			curEnd = r.StopMs
		}
		num := sess.GetKlineNum(sid, timeFrame, curStart, curEnd)
		actual += num
		curStart = curEnd
	}

	if actual == maxBars {
		return nil
	}
	var problem string
	if actual == 0 {
		problem = fmt.Sprintf("no data: srange has_data=true but 0 bars (max %d)", maxBars)
	} else if actual > maxBars {
		problem = fmt.Sprintf("duplicate bars: actual %d > max %d (extra %d)", actual, maxBars, actual-maxBars)
	} else {
		problem = fmt.Sprintf("missing bars: actual %d / max %d (gap %d)", actual, maxBars, maxBars-actual)
	}
	return &VerifyResult{
		Sid: sid, Symbol: symbol, Table: table, TimeFrame: timeFrame,
		SRangeID: r.ID, StartMs: r.StartMs, StopMs: r.StopMs,
		Expected: maxBars, Actual: actual, Problem: problem,
	}
}

func listSRangeSids(tables []string) ([]int32, *errs.Error) {
	db, err := BanPubConn(false)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var whereClause string
	args := make([]any, 0, len(tables))
	if len(tables) > 0 {
		pats := make([]string, 0, len(tables))
		for _, t := range tables {
			pats = append(pats, "tbl like ?")
			args = append(args, t+"_%")
		}
		whereClause = "where " + strings.Join(pats, " or ")
	}
	sqlText := fmt.Sprintf("select distinct sid from sranges %s order by sid", whereClause)
	rows, err_ := db.QueryContext(context.Background(), sqlText, args...)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	var sids []int32
	for rows.Next() {
		var sid int32
		if err := rows.Scan(&sid); err != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err)
		}
		sids = append(sids, sid)
	}
	return sids, nil
}

func PrintVerifyResults(results []*VerifyResult) {
	if len(results) == 0 {
		log.Info("all data ranges verified OK")
		return
	}
	log.Warn("data range verification found issues", zap.Int("count", len(results)))
	for _, r := range results {
		startStr := btime.ToDateStr(r.StartMs, "")
		stopStr := btime.ToDateStr(r.StopMs, "")
		fmt.Printf("  [%s] sid=%d tf=%s range=[%s ~ %s] %s\n",
			r.Symbol, r.Sid, r.TimeFrame, startStr, stopStr, r.Problem)
	}
}
