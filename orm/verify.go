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

// VerifyIssue 单个具体问题
type VerifyIssue struct {
	Type    string // gap_no_hole, duplicate, orphan
	StartMs int64
	StopMs  int64
	Count   int // 重复次数或gap中缺失bar数
}

// VerifyTFResult 单个sid+timeframe的检查结果
type VerifyTFResult struct {
	Sid       int32
	Symbol    string
	TimeFrame string
	Issues    []*VerifyIssue
}

type VerifyArgs struct {
	Sids      []int32
	Tables    []string // 留空默认检查kline
	BatchSize int      // 分批读取时间戳数量，默认500000
}

func VerifyDataRanges(args *VerifyArgs) ([]*VerifyTFResult, *errs.Error) {
	if args.BatchSize <= 0 {
		args.BatchSize = 500000
	}
	tables := args.Tables
	if len(tables) == 0 {
		tables = []string{"kline"}
	}
	sids := args.Sids
	if len(sids) == 0 {
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

	var results []*VerifyTFResult
	for _, sid := range sids {
		exs := GetSymbolByID(sid)
		symbol := fmt.Sprintf("sid:%d", sid)
		if exs != nil {
			symbol = exs.Symbol
		}
		for _, tblPrefix := range tables {
			pBar.Add(1)
			allRanges, err_ := PubQ().ListSRangesBySid(context.Background(), sid)
			if err_ != nil {
				return results, NewDbErr(core.ErrDbReadFail, err_)
			}
			// 按timeframe分组，只处理匹配tblPrefix的
			tfRanges := make(map[string][]*SRange)
			for _, r := range allRanges {
				if !strings.HasPrefix(r.Table, tblPrefix+"_") {
					continue
				}
				tfRanges[r.Timeframe] = append(tfRanges[r.Timeframe], r)
			}
			for tf, ranges := range tfRanges {
				if allowKlineDiag(sid, symbol, tf) {
					var dataRanges, holeRanges []MSRange
					for _, r := range ranges {
						if r.HasData {
							dataRanges = append(dataRanges, MSRange{Start: r.StartMs, Stop: r.StopMs})
						} else {
							holeRanges = append(holeRanges, MSRange{Start: r.StartMs, Stop: r.StopMs})
						}
					}
					dataRanges = mergeMSRanges(dataRanges)
					holeRanges = mergeMSRanges(holeRanges)
					dNum, dStart, dStop := summarizeRangeBounds(dataRanges)
					hNum, hStart, hStop := summarizeRangeBounds(holeRanges)
					log.Warn("kline diag verify sranges",
						zap.Int32("sid", sid),
						zap.String("symbol", symbol),
						zap.String("tf", tf),
						zap.Int("data_num", dNum),
						zap.Int64("data_start", dStart),
						zap.Int64("data_stop", dStop),
						zap.Int("hole_num", hNum),
						zap.Int64("hole_start", hStart),
						zap.Int64("hole_stop", hStop),
					)
				}
				issues := verifyTFRanges(sess, sid, tf, ranges, args.BatchSize)
				if len(issues) > 0 {
					results = append(results, &VerifyTFResult{
						Sid: sid, Symbol: symbol, TimeFrame: tf, Issues: issues,
					})
				}
			}
		}
	}
	return results, nil
}

// verifyTFRanges 对单个sid+timeframe的所有sranges和实际k线做逐bar比较
func verifyTFRanges(sess *Queries, sid int32, timeFrame string, ranges []*SRange, batchSize int) []*VerifyIssue {
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	if tfMSecs <= 0 {
		return []*VerifyIssue{{Type: "error", StartMs: 0, StopMs: 0, Count: 0}}
	}
	// 按start_ms排序
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].StartMs < ranges[j].StartMs })

	// 计算整体范围
	minStart := ranges[0].StartMs
	maxStop := ranges[0].StopMs
	for _, r := range ranges[1:] {
		if r.StopMs > maxStop {
			maxStop = r.StopMs
		}
	}
	realMin, realMax, rangeErr := sess.getKLineTimeRange(sid, timeFrame)
	if rangeErr == nil && realMin > 0 {
		if realMin < minStart {
			minStart = realMin
		}
		realMaxStop := realMax + tfMSecs
		if realMaxStop > maxStop {
			maxStop = realMaxStop
		}
	}

	// 构建has_data和no_data的区间集合
	var dataRanges, holeRanges []MSRange
	for _, r := range ranges {
		if r.HasData {
			dataRanges = append(dataRanges, MSRange{Start: r.StartMs, Stop: r.StopMs})
		} else {
			holeRanges = append(holeRanges, MSRange{Start: r.StartMs, Stop: r.StopMs})
		}
	}
	dataRanges = mergeMSRanges(dataRanges)
	holeRanges = mergeMSRanges(holeRanges)

	// 分批读取实际k线时间戳，逐bar检查
	var issues []*VerifyIssue
	var lastTime int64 // 跨批次跟踪上一个时间戳
	curStart := minStart
	for curStart < maxStop {
		curEnd := curStart + int64(batchSize)*tfMSecs
		if curEnd > maxStop {
			curEnd = maxStop
		}
		times, err := sess.getKLineTimes(sid, timeFrame, curStart, curEnd)
		if err != nil {
			log.Warn("read kline times fail", zap.Int32("sid", sid), zap.String("tf", timeFrame), zap.String("err", err.Short()))
			break
		}
		batchIssues, newLast := checkTimestamps(times, lastTime, tfMSecs, dataRanges, holeRanges)
		issues = append(issues, batchIssues...)
		if newLast > 0 {
			lastTime = newLast
		}
		curStart = curEnd
	}
	return mergeIssues(issues)
}

// checkTimestamps 检查一批时间戳，发现gap/duplicate/orphan。prevTime为上一批最后的时间戳
func checkTimestamps(times []int64, prevTime, tfMSecs int64, dataRanges, holeRanges []MSRange) ([]*VerifyIssue, int64) {
	var issues []*VerifyIssue
	for _, t := range times {
		// 检查重复
		if prevTime > 0 && t == prevTime {
			issues = append(issues, &VerifyIssue{Type: "duplicate", StartMs: t, StopMs: t + tfMSecs, Count: 1})
			continue
		}
		// 检查是否在has_data范围外（orphan）
		if !msInRanges(t, dataRanges) {
			issues = append(issues, &VerifyIssue{Type: "orphan", StartMs: t, StopMs: t + tfMSecs, Count: 1})
			prevTime = t
			continue
		}
		// 检查与前一个bar之间的gap
		if prevTime > 0 {
			prevEnd := prevTime + tfMSecs
			if prevEnd < t {
				gapIssues := checkGap(prevEnd, t, tfMSecs, dataRanges, holeRanges)
				issues = append(issues, gapIssues...)
			}
		}
		prevTime = t
	}
	return issues, prevTime
}

// checkGap 检查[gapStart, gapEnd)之间缺失的bar，是否被has_data=false的hole覆盖
func checkGap(gapStart, gapEnd, tfMSecs int64, dataRanges, holeRanges []MSRange) []*VerifyIssue {
	var issues []*VerifyIssue
	var issueStart int64
	for t := gapStart; t < gapEnd; t += tfMSecs {
		inData := msInRanges(t, dataRanges)
		inHole := msInRanges(t, holeRanges)
		if inData && !inHole {
			// 在has_data范围内但没有hole记录，是真正的问题
			if issueStart == 0 {
				issueStart = t
			}
		} else {
			// 不在data范围内，或被hole覆盖，正常
			if issueStart > 0 {
				count := int((t - issueStart) / tfMSecs)
				issues = append(issues, &VerifyIssue{Type: "gap_no_hole", StartMs: issueStart, StopMs: t, Count: count})
				issueStart = 0
			}
		}
	}
	if issueStart > 0 {
		count := int((gapEnd - issueStart) / tfMSecs)
		issues = append(issues, &VerifyIssue{Type: "gap_no_hole", StartMs: issueStart, StopMs: gapEnd, Count: count})
	}
	return issues
}

// msInRanges 检查时间戳t是否在任一[Start, Stop)区间内
func msInRanges(t int64, ranges []MSRange) bool {
	// 二分查找
	lo, hi := 0, len(ranges)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		r := ranges[mid]
		if t < r.Start {
			hi = mid - 1
		} else if t >= r.Stop {
			lo = mid + 1
		} else {
			return true
		}
	}
	return false
}

// mergeIssues 合并相邻的同类型issue
func mergeIssues(issues []*VerifyIssue) []*VerifyIssue {
	if len(issues) <= 1 {
		return issues
	}
	var merged []*VerifyIssue
	cur := *issues[0]
	for _, it := range issues[1:] {
		if it.Type == cur.Type && it.StartMs == cur.StopMs {
			cur.StopMs = it.StopMs
			cur.Count += it.Count
		} else {
			c := cur
			merged = append(merged, &c)
			cur = *it
		}
	}
	merged = append(merged, &cur)
	return merged
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

func PrintVerifyResults(results []*VerifyTFResult) {
	if len(results) == 0 {
		log.Info("all data ranges verified OK")
		return
	}
	total := 0
	for _, r := range results {
		total += len(r.Issues)
	}
	log.Warn("data range verification found issues", zap.Int("symbols", len(results)), zap.Int("issues", total))
	for _, r := range results {
		fmt.Printf("\n[%s] sid=%d tf=%s  (%d issues)\n", r.Symbol, r.Sid, r.TimeFrame, len(r.Issues))
		for _, it := range r.Issues {
			startStr := btime.ToDateStr(it.StartMs, "")
			stopStr := btime.ToDateStr(it.StopMs, "")
			switch it.Type {
			case "gap_no_hole":
				fmt.Printf("  GAP  [%s ~ %s] missing %d bars, no hole in sranges\n", startStr, stopStr, it.Count)
			case "duplicate":
				fmt.Printf("  DUP  [%s] %d duplicate bar(s)\n", startStr, it.Count)
			case "orphan":
				fmt.Printf("  ORPH [%s ~ %s] %d bar(s) outside has_data sranges\n", startStr, stopStr, it.Count)
			default:
				fmt.Printf("  ERR  [%s ~ %s] %s\n", startStr, stopStr, it.Type)
			}
		}
	}
}
