package strat

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/floats"
	"math"
	"slices"
)

type PolicyGroup struct {
	Policies []*config.RunPolicyConfig
	StartMS  int64
}

/*
RelayPolicyGroups 获取需要接力开单的策略分组

将策略按最小周期划分为多个组。组内多个策略的最小周期最大差距不能超过5倍。
用于对不同起止时间且不同周期的策略，划分不同组，提高整体效率
*/
func RelayPolicyGroups() []*PolicyGroup {
	tfScores := make(map[string]float64)
	allowTfs := allAllowTFs()
	for _, tf := range allowTfs {
		tfScores[tf] = 1
	}
	// 记录每个策略的最小使用周期
	polTFs := make(map[string]int)
	for _, pol := range config.RunPolicy {
		stgy := New(pol)
		tf := stgy.pickTimeFrame("", tfScores)
		if tf == "" {
			continue
		}
		key := fmt.Sprintf("%s:%s", pol.ID(), tf)
		if _, ok := polTFs[key]; ok {
			continue
		}
		minTfSecs := utils2.TFToSecs(tf)
		if stgy.OnPairInfos != nil || stgy.OnDataSubs != nil {
			job := &StratJob{
				Strat:     stgy,
				DataHub:   NewDataHub(),
				TimeFrame: tf,
			}
			infos := CollectDataSubs(job)
			for _, it := range infos {
				curSecs := utils2.TFToSecs(it.TimeFrame)
				if curSecs < minTfSecs {
					minTfSecs = curSecs
				}
			}
		}
		polTFs[key] = minTfSecs
	}
	if len(polTFs) == 0 {
		return nil
	}
	// 按周期从小到大排序
	items := make([]*core.StrInt64, 0, len(polTFs))
	for name, secs := range polTFs {
		items = append(items, &core.StrInt64{
			Str: name,
			Int: int64(secs),
		})
	}
	slices.SortFunc(items, func(a, b *core.StrInt64) int {
		return int(a.Int - b.Int)
	})
	// 当某个周期与组内最小周期倍率超过5倍，则归为新的组
	curGpId := 0
	gp := make(map[string]int)
	lastTfSecs := items[0].Int
	for _, it := range items {
		if it.Int > lastTfSecs*5 {
			curGpId += 1
			lastTfSecs = it.Int
		}
		gp[it.Str] = curGpId
	}
	// 计算每个分组开始时间
	curTime := btime.TimeMS()
	polGroups := make([]*PolicyGroup, 0, curGpId+1)
	for i := 0; i <= curGpId; i++ {
		polGroups = append(polGroups, &PolicyGroup{StartMS: math.MaxInt64})
	}
	for _, pol := range config.RunPolicy {
		stgy := New(pol)
		tf := stgy.pickTimeFrame("", tfScores)
		if tf == "" {
			continue
		}
		idx := gp[fmt.Sprintf("%s:%s", pol.ID(), tf)]
		g := polGroups[idx]
		tfSecs := int64(utils2.TFToSecs(tf) * 1000)
		g.StartMS = min(g.StartMS, curTime-tfSecs*int64(stgy.orderBarMax()))
		g.Policies = append(g.Policies, pol)
	}
	return polGroups
}

// CalcPairTfScores Calculate the K-line quality score of each dimension of the trading pair
// 计算交易对各维度K线质量分数
func CalcPairTfScores(exchange banexg.BanExchange, pairs []string) (map[string]map[string]float64, *errs.Error) {
	pairTfScores := make(map[string]map[string]float64)
	allowTfs := allAllowTFs()
	if len(allowTfs) == 0 {
		return pairTfScores, errs.NewMsg(core.ErrBadConfig, "run_timeframes is required in config")
	}
	wsModeTf := ""
	for _, v := range allowTfs {
		tfSecs := utils2.TFToSecs(v)
		if tfSecs < 60 {
			wsModeTf = v
			break
		}
	}
	if wsModeTf != "" {
		for _, pair := range pairs {
			pairTfScores[pair] = map[string]float64{wsModeTf: 1.0}
		}
		return pairTfScores, nil
	}
	handle := func(pair, timeFrame string, arr []*banexg.Kline, adjs []*orm.AdjInfo) {
		tfScores, ok := pairTfScores[pair]
		if !ok {
			tfScores = make(map[string]float64)
			pairTfScores[pair] = tfScores
		}
		pipChg, err := exchange.PriceOnePip(pair)
		if err != nil {
			log.Error("PriceOnePip fail", zap.String("pair", pair), zap.Float64("pip", pipChg))
			return
		}
		score := float64(1)
		if len(arr) > 0 && pipChg > 0 {
			arr = orm.ApplyAdj(adjs, arr, core.AdjFront, 0, 0)
			calcScore := calcKlineScore(arr, pipChg, 3)
			if !math.IsNaN(calcScore) {
				score = calcScore
			}
		}
		tfScores[timeFrame] = score
	}
	backNum := 600
	for _, tf := range allowTfs {
		err := orm.FastBulkOHLCV(exchange, pairs, tf, 0, 0, backNum, handle)
		if err != nil {
			return pairTfScores, err
		}
	}
	return pairTfScores, nil
}

/*
calcKlineScore
Calculate the quality of K-line. Used to eliminate trading pairs with too small changes and insufficient volatility; or calculate the best cycle of trading pairs. The threshold of 0.8 is more appropriate

Price change: four prices are the same -1 point; bar change = minimum change unit -1 point 40% weight
Average weighted entity share: 30% weight
Bar overlap ratio score, the entity part of a bar, the overlap interval of the entity range of the previous two bars, the ratio of the current bar entity, the lower the score, the higher the weight, 30% weight

计算K线质量。用于淘汰变动太小，波动不足的交易对；或计算交易对的最佳周期。阈值取0.8较合适

	价格变动：四价相同-1分；bar变动=最小变动单位-1分 40%权重
	平均加权实体占比：30%权重
	Bar重合比率分数，某个bar的实体部分，与前两个bar的实体范围重合区间，占当前bar实体的比率，越低分数越高，30%权重
*/
func calcKlineScore(arr []*banexg.Kline, pipChg float64, prevNum int) float64 {
	totalLen := len(arr)
	pipScore := float64(len(arr))
	solidRates := make([]float64, 0, len(arr))
	overlaps := make([]float64, 0, len(arr))
	olWeightSum, solWeightSum := float64(0), float64(0)
	var pRanges = make([]float64, prevNum*2)
	var nextIdx int
	var cMin, cMax float64
	for i, bar := range arr {
		chgRate := (bar.High - bar.Low) / pipChg
		if chgRate == 0 || chgRate == 1 {
			pipScore -= 1
		} else if chgRate == 2 {
			pipScore -= 0.3
		}
		cMin, cMax = bar.Low, bar.High
		barLen := cMax - cMin
		weight := barLen / cMax
		if cMin != cMax {
			rate := math.Abs(bar.Open-bar.Close) / barLen
			solidRates = append(solidRates, rate*weight)
			solWeightSum += weight
		}
		if i >= prevNum && cMin != cMax {
			pMax, pMin := slices.Max(pRanges), slices.Min(pRanges)
			// Calculate the overlap rate between the current bar and the previous n bars
			// 计算当前bar与前面n个bar的重合率
			olRate := max(min(cMax, pMax)-max(cMin, pMin), 0) / barLen
			overlaps = append(overlaps, olRate*weight)
			olWeightSum += weight
		}
		pRanges[nextIdx] = cMin
		pRanges[nextIdx+1] = cMax
		nextIdx = (nextIdx + 2) % (prevNum * 2)
	}
	// Price Change Unit Fraction
	// 价格变动单位分数
	chgScore := math.Pow(pipScore/float64(totalLen), 2)
	// Entity proportion
	// 实体部分占比分数
	jRateScore := floats.Sum(solidRates) / solWeightSum
	jRateScore = 1 - math.Pow(1-jRateScore, 2)
	// Calculate the overlap rate score with the previous n bars
	// 计算与前面n个bar重合率分数
	overlapScore := 1 - floats.Sum(overlaps)/olWeightSum
	overlapScore = 1 - math.Pow(1-overlapScore, 3)
	return chgScore*0.4 + jRateScore*0.3 + overlapScore*0.3
}

func allAllowTFs() []string {
	var groups = [][]string{config.RunTimeframes}
	for _, pol := range config.RunPolicy {
		if pol.Dirt == "any" {
			pol.Dirt = ""
		}
		stagy := New(pol)
		if stagy == nil {
			continue
		}
		if len(pol.RunTimeframes) > 0 {
			// The configured time period takes precedence over the hard-coded policy period.
			// 配置的时间周期优先级高于策略写死的
			groups = append(groups, pol.RunTimeframes)
		} else {
			groups = append(groups, stagy.RunTimeFrames)
		}
	}
	return utils.UnionArr(groups...)
}
