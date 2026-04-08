package strat

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/banbox/banbot/orm"
)

func DataSubKey(source string, sid int32, tf string) string {
	return fmt.Sprintf("%s:%d:%s", orm.NormalizeSeriesSource(source), sid, tf)
}

func ParseDataSubKey(key string) (string, int32, string, bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 {
		return "", 0, "", false
	}
	sid64, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return "", 0, "", false
	}
	return parts[0], int32(sid64), parts[2], true
}

func CollectDataSubs(job *StratJob) []*DataSub {
	if job == nil || job.Strat == nil {
		return nil
	}
	var out []*DataSub
	if job.Strat.OnPairInfos != nil {
		for _, sub := range job.Strat.OnPairInfos(job) {
			if sub == nil {
				continue
			}
			exs := job.Symbol
			if sub.Pair != "" && sub.Pair != "_cur_" {
				if job.Symbol != nil {
					exs = orm.GetExSymbol2(job.Symbol.Exchange, job.Symbol.Market, sub.Pair)
				} else {
					exs, _ = orm.GetExSymbolCur(sub.Pair)
				}
				if exs == nil {
					continue
				}
			}
			out = append(out, &DataSub{
				Source:    orm.SeriesSourceKline,
				ExSymbol:  exs,
				TimeFrame: sub.TimeFrame,
				WarmupNum: sub.WarmupNum,
			})
		}
	}
	if job.Strat.OnDataSubs != nil {
		for _, sub := range job.Strat.OnDataSubs(job) {
			if sub == nil {
				continue
			}
			exs := sub.ExSymbol
			if exs == nil {
				exs = job.Symbol
			}
			if exs == nil {
				continue
			}
			out = append(out, &DataSub{
				Source:    orm.NormalizeSeriesSource(sub.Source),
				ExSymbol:  exs,
				TimeFrame: sub.TimeFrame,
				WarmupNum: sub.WarmupNum,
			})
		}
	}
	return out
}
