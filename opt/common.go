package opt

import (
	_ "embed"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/go-viper/mapstructure/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type FnCalcOptBest = func(items []*OptInfo) *OptInfo

var (
	MapCalcOptBest = map[string]FnCalcOptBest{
		"score":   getBestByScore,
		"good3":   optGood3,
		"good0t3": optGood0t3,
		"goodAvg": optGoodMa,
		"good1t4": optGood1t4,
		"good4":   optGood4,
		// below performance is poor
		// 下面的效果不好
		"good2":    optGood2,
		"good5":    optGood5,
		"good7":    optGood7,
		"good2t5":  optGood2t5,
		"good3t7":  optGood3t7,
		"good0t7":  optGood0t7,
		"good3t10": optGood3t10,
	}
	DefCalcOptBest = "good3"
	AfterBacktest  func(bt *BackTest)
)

type OptGroup struct {
	Items []*OptInfo
	Score float64
	Name  string
	Pair  string
	TFStr string
}

type OptInfo struct {
	Dirt   string
	ID     string
	Score  float64
	Params map[string]float64
	Ints   map[string]bool
	*BTResult
}

type rollBtOpt struct {
	args        *config.CmdArgs
	curMs       int64
	allEndMs    int64
	dateRange   *config.TimeTuple
	runMSecs    int64
	reviewMSecs int64
	outDir      string
	initPols    []*config.RunPolicyConfig
}

type ValItem struct {
	Tag   string
	Score float64
	Order int
	Res   int
}

func calcBestBy(items []*OptInfo, name string) *OptInfo {
	if len(items) == 0 {
		return nil
	}
	method, _ := MapCalcOptBest[name]
	defFn, _ := MapCalcOptBest[DefCalcOptBest]
	if defFn == nil {
		panic(fmt.Sprintf("`DefCalcOptBest` no associated function: %s", DefCalcOptBest))
	}
	if method == nil {
		if name != "" {
			log.Warn("picker for MapCalcOptBest not found, use default", zap.String("n", name))
		}
		method = defFn
	}
	res := method(items)
	if res == nil {
		res = defFn(items)
		if res == nil {
			res = getBestByScore(items)
		}
	}
	return res
}

func (o *OptInfo) runGetBtResult(pol *config.RunPolicyConfig) {
	for k, v := range o.Params {
		pol.Params[k] = v
	}
	bt, loss := runBTOnce()
	o.Score = -loss
	o.BTResult = bt.BTResult
}

func (o *OptInfo) ToPol(idx int, name, dirt, tfStr, pairStr string) *config.RunPolicyConfig {
	if o.Dirt == "" {
		o.Dirt = dirt
	}
	res := &config.RunPolicyConfig{
		Index:  idx,
		Name:   name,
		Dirt:   o.Dirt,
		Params: o.Params,
		Score:  o.Score,
	}
	if len(tfStr) > 0 {
		res.RunTimeframes = strings.Split(tfStr, "|")
	}
	if len(pairStr) > 0 {
		res.Pairs = strings.Split(pairStr, "|")
	}
	return res
}

func newRollBtOpt(args *config.CmdArgs) (*rollBtOpt, *errs.Error) {
	core.SetRunMode(core.RunModeBackTest)
	err := biz.SetupComsExg(args)
	if err != nil {
		return nil, err
	}
	dateRange := config.TimeRange.Clone()
	allStartMs, allEndMs := dateRange.StartMS, dateRange.EndMS
	runMSecs := int64(utils2.TFToSecs(args.RunPeriod)) * 1000
	reviewMSecs := int64(utils2.TFToSecs(args.ReviewPeriod)) * 1000
	if runMSecs < utils2.SecsHour*1000 {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "`run-period` cannot be less than 1 hour")
	}
	outDir := filepath.Join(config.GetDataDir(), "backtest", "bt_opt_"+btOptHash(args))
	err_ := utils.EnsureDir(outDir, 0755)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOWriteFail, err_)
	}
	log.Info("write bt over opt to", zap.String("dir", outDir))
	args.OutPath = filepath.Join(outDir, "opt.log")
	curMs := allStartMs + reviewMSecs
	initPols := config.RunPolicy
	return &rollBtOpt{
		args:        args,
		curMs:       curMs,
		allEndMs:    allEndMs,
		dateRange:   dateRange,
		runMSecs:    runMSecs,
		reviewMSecs: reviewMSecs,
		outDir:      outDir,
		initPols:    initPols,
	}, nil
}

func (t *rollBtOpt) next(pairPicker string) (string, *errs.Error) {
	t.dateRange.StartMS = t.curMs - t.reviewMSecs
	t.dateRange.EndMS = t.curMs
	fname := fmt.Sprintf("opt_%v.log", t.dateRange.StartMS/1000)
	t.args.OutPath = filepath.Join(t.outDir, fname)
	polStr, err := pickFromExists(t.args.OutPath, t.args.Picker, pairPicker)
	if err != nil {
		return "", err
	}
	if polStr == "" {
		config.RunPolicy = t.initPols
		polStr, err = runOptimize(t.args, 0)
		if err != nil {
			return "", err
		}
	} else {
		log.Info("use hyperopt cache", zap.String("path", fname))
	}
	return polStr, nil
}

func (t *rollBtOpt) dumpConfig() *errs.Error {
	data, err := config.DumpYaml(true)
	if err != nil {
		return err
	}
	outPath := filepath.Join(t.outDir, "config.yml")
	err_ := os.WriteFile(outPath, data, 0644)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	return nil
}

func parseRunPolicies(text string) ([]*config.RunPolicyConfig, *errs.Error) {
	var unpak = make(map[string]interface{})
	err_ := yaml.Unmarshal([]byte(text), &unpak)
	if err_ != nil {
		return nil, errs.New(errs.CodeRunTime, err_)
	}
	var cfg config.Config
	err_ = mapstructure.Decode(unpak, &cfg)
	if err_ != nil {
		return nil, errs.New(errs.CodeRunTime, err_)
	}
	return cfg.RunPolicy, nil
}

func (r *BTResult) BriefLine() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("odNum: %v, profit: %.1f%%, drawDown: %.1f%%, sharpe: %.2f",
		r.OrderNum, r.TotProfitPct, r.ShowDrawDownPct, r.SharpeRatio)
}

func (o *OptInfo) ToLine() string {
	var text string
	if len(o.Params) > 0 {
		params := make(map[string]float64)
		for k, v := range o.Params {
			if isInt, ok := o.Ints[k]; ok && isInt {
				v = math.Round(v)
			}
			params[k] = v
		}
		text = utils.MapToStr(params, true, 2)
		text += "\t"
	}
	return fmt.Sprintf("loss: %7.2f \t%s \t%s, id: %v", -o.Score, text, o.BriefLine(), o.ID)
}

/*
AvgGoodDesc
For profitable groups, cut the specified range in descending order of scores and take the average of the parameters
对盈利的组，按分数降序，截取指定范围，取参数平均值
*/
func AvgGoodDesc(items []*OptInfo, startRate float64, endRate float64) *OptInfo {
	if startRate >= endRate {
		panic("low should < upp in AvgGoodDesc")
	}
	list, bads := DescGroups(items)
	if len(list) == 0 {
		// When all are at a loss, use the loss group calculation
		// 当全部处于亏损时，使用亏损的组计算
		list = bads
	}
	lenFlt := float64(len(list))
	start := int(math.Round(lenFlt * startRate))
	stop := int(math.Round(lenFlt * endRate))
	if start+1 >= stop {
		return list[start]
	}
	var res map[string]float64
	var count = 0
	for _, it := range list[start:stop] {
		if len(res) == 0 {
			res = it.Params
		} else {
			for k, v := range res {
				val, ok := it.Params[k]
				if !ok {
					val = v
				}
				res[k] = v + val
			}
		}
		count += 1
	}
	if count == 0 {
		return nil
	}
	countFlt := float64(count)
	for k, v := range res {
		res[k] = v / countFlt
	}
	return &OptInfo{
		Params: res,
		Ints:   make(map[string]bool),
	}
}

/*
DescGroups
Divide the parameter group into profit and loss groups, both in descending order of scores; Return: Profit group, loss group
将参数组划分为盈利和亏损两组，都按分数降序；返回：盈利组，亏损组
*/
func DescGroups(items []*OptInfo) ([]*OptInfo, []*OptInfo) {
	slices.SortFunc(items, func(a, b *OptInfo) int {
		return int(b.Score - a.Score)
	})
	for i, it := range items {
		if it.Score < 0 {
			return items[:i], items[i:]
		}
	}
	return items, nil
}

func getBestByScore(items []*OptInfo) *OptInfo {
	var best *OptInfo
	for _, it := range items {
		if best == nil || it.Score > best.Score {
			best = it
		}
	}
	return best
}

func optGoodMa(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0, 1)
}

func optGood4(items []*OptInfo) *OptInfo {
	return optGoodPos(items, 0.4)
}

func optGood3(items []*OptInfo) *OptInfo {
	return optGoodPos(items, 0.3)
}

func optGood2(items []*OptInfo) *OptInfo {
	return optGoodPos(items, 0.2)
}

func optGood5(items []*OptInfo) *OptInfo {
	return optGoodPos(items, 0.5)
}

func optGood7(items []*OptInfo) *OptInfo {
	return optGoodPos(items, 0.7)
}

func optGoodPos(items []*OptInfo, rate float64) *OptInfo {
	list, bads := DescGroups(items)
	if len(list) == 0 {
		list = bads
	}
	idx := int(float64(len(list)) * rate)
	return list[idx]
}

func optGood0t3(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0, 0.3)
}

func optGood1t4(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0.1, 0.4)
}

func optGood2t5(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0.2, 0.5)
}

func optGood3t7(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0.3, 0.7)
}

func optGood0t7(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0, 0.7)
}

func optGood3t10(items []*OptInfo) *OptInfo {
	return AvgGoodDesc(items, 0.3, 1)
}

func getTestPickers(text string) ([]string, *errs.Error) {
	all := utils.KeysOfMap(MapCalcOptBest)
	if text == "" {
		return all, nil
	}
	arr := strings.Split(text, ",")
	if len(arr) <= 1 {
		return arr, nil
	}
	for _, key := range arr {
		if _, ok := MapCalcOptBest[key]; !ok {
			return nil, errs.NewMsg(errs.CodeParamInvalid, "unknown picker: %v", key)
		}
	}
	return arr, nil
}

//go:embed lines.html
var LineChartData []byte

//go:embed barStat.html
var BarStatChartData []byte

/*
DumpChart dump a chart html with datasets. draw line chart if tplData is nil
*/
func DumpChart(path, title string, label []string, prec float64, tplData []byte, items []*ChartDs) *errs.Error {
	g := Chart{
		TplData:   tplData,
		Title:     title,
		Labels:    label,
		Precision: prec,
		Datasets:  items,
	}
	return g.DumpFile(path)
}

func (g *Chart) Dump() ([]byte, *errs.Error) {
	var err_ error
	for _, it := range g.Datasets {
		col := make([]float64, 0, len(it.Data))
		prevId := 0
		for i, v := range it.Data {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			} else if prevId+1 < i {
				for j := prevId + 1; j < i; j++ {
					col = append(col, v)
				}
			}
			prevId = i
			if g.Precision > 0 {
				v, err_ = utils2.PrecFloat64(v, g.Precision, true, utils2.PrecModeSignifDigits)
				if err_ != nil {
					return nil, errs.New(errs.CodeRunTime, err_)
				}
			}
			col = append(col, v)
		}
		it.Data = col
	}
	if len(g.TplData) == 0 {
		g.TplData = LineChartData
	}
	content := string(g.TplData)
	data, err_ := utils2.Marshal(g)
	if err_ != nil {
		return nil, errs.New(errs.CodeMarshalFail, err_)
	}
	content = strings.Replace(content, "{'inject': 1}", string(data), 1)
	return []byte(content), nil
}

func (g *Chart) DumpFile(path string) *errs.Error {
	data, err := g.Dump()
	if err != nil {
		return err
	}
	err_ := os.WriteFile(path, data, 0644)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	return nil
}

// DumpBarStat generate bar chart for data Distribution Statistics 生成数据分布的条形图
func DumpBarStat(path, title string, maxNum int, data []float64) *errs.Error {
	if len(data) == 0 {
		return nil
	}

	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	for _, v := range data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	interval := (maxVal - minVal) / float64(maxNum)
	if interval == 0 {
		interval = 1
	}

	counts := make([]float64, maxNum)
	labels := make([]string, maxNum)

	// count for every range
	for _, v := range data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		idx := int((v - minVal) / interval)
		if idx >= maxNum {
			idx = maxNum - 1
		}
		counts[idx]++
	}

	// 生成标签
	for i := 0; i < maxNum; i++ {
		start := minVal + float64(i)*interval
		end := start + interval
		labels[i] = strconv.FormatFloat((start+end)/2, 'g', 3, 64)
	}

	barStat := Chart{
		TplData: BarStatChartData,
		Title:   title,
		Labels:  labels,
		Datasets: []*ChartDs{
			{
				Label:           "Count",
				Data:            counts,
				BackgroundColor: "rgba(54, 162, 235)",
			},
		},
	}

	return barStat.DumpFile(path)
}

type Chart struct {
	TplData   []byte     `json:"-"`
	Precision float64    `json:"-"`
	Title     string     `json:"title"`
	Labels    []string   `json:"labels"`
	Datasets  []*ChartDs `json:"datasets"`
}

type ChartDs struct {
	Label           string    `json:"label"`
	Data            []float64 `json:"data"`
	Color           string    `json:"color,omitempty"`
	BorderColor     string    `json:"borderColor,omitempty"`
	BackgroundColor string    `json:"backgroundColor,omitempty"`
	YAxisID         string    `json:"yAxisID,omitempty"`
	Hidden          bool      `json:"hidden"`
}
