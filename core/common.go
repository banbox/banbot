package core

import (
	"bytes"
	"fmt"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/dgraph-io/ristretto"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"math"
	"os"
	"strings"
	"unicode"
)

var (
	Cache *ristretto.Cache
)

func Setup() *errs.Error {
	var err_ error
	Cache, err_ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1 << 26,
		BufferItems: 64,
	})
	if err_ != nil {
		return errs.New(ErrRunTime, err_)
	}
	return nil
}

func GetCacheVal[T any](key interface{}, defVal T) T {
	obj, has := Cache.Get(key)
	if has {
		if val, ok := obj.(T); ok {
			return val
		}
	}
	return defVal
}

func RunExitCalls() {
	for _, method := range ExitCalls {
		method()
	}
	ExitCalls = nil
}

func KeyStratPairTf(stagy, pair, tf string) string {
	var b strings.Builder
	b.Grow(len(pair) + len(tf) + len(stagy) + 2)
	b.WriteString(stagy)
	b.WriteString("_")
	b.WriteString(pair)
	b.WriteString("_")
	b.WriteString(tf)
	return b.String()
}

func (p *JobPerf) GetAmount(amount float64) float64 {
	if p.Score == PrefMinRate {
		// Open an order based on the minimum amount 按最小金额开单
		return MinStakeAmount
	}
	return amount * p.Score
}

func GetPerfSta(stagy string) *PerfSta {
	p, ok := StratPerfSta[stagy]
	if !ok || p == nil {
		p = &PerfSta{}
		StratPerfSta[stagy] = p
	}
	return p
}

func (p *PerfSta) FindGID(val float64) int {
	if p.Splits == nil {
		panic("PerfSta.Splits is empty, FindGID fail")
	}
	for i, gp := range p.Splits {
		if val < gp {
			return i
		}
	}
	return len(p.Splits)
}

func (p *PerfSta) Log2(profit float64) float64 {
	logPft := math.Log2(math.Abs(profit)*p.Delta + 1)
	if profit < 0 {
		logPft = -logPft
	}
	return logPft
}

func DumpPerfs(outDir string) {
	perfs := make(map[string]map[string]string)
	for key, pf := range JobPerfs {
		parts := strings.Split(key, "_")
		data, ok := perfs[parts[0]]
		if !ok {
			data = make(map[string]string)
			perfs[parts[0]] = data
		}
		cacheKey := strings.Join(parts[1:], "_")
		data[cacheKey] = fmt.Sprintf("%v|%.5f|%.5f", pf.Num, pf.TotProfit, pf.Score)
	}
	res := make(map[string]interface{})
	for name, sta := range StratPerfSta {
		perf, _ := perfs[name]
		res[name] = map[string]interface{}{
			"od_num":     sta.OdNum,
			"last_gp_at": sta.LastGpAt,
			"splits":     sta.Splits,
			"delta":      sta.Delta,
			"perf":       perf,
		}
	}
	data, err_ := MarshalYaml(res)
	if err_ != nil {
		log.Error("marshal strat_perfs fail", zap.Error(err_))
		return
	}
	outName := fmt.Sprintf("%s/strat_perfs.yml", outDir)
	err_ = os.WriteFile(outName, data, 0644)
	if err_ != nil {
		log.Error("save strat_perfs fail", zap.Error(err_))
		return
	}
	log.Info("dump strat_perfs ok", zap.String("path", outName))
}

/*
IsFiat Is it legal tender? 是否是法币
*/
func IsFiat(code string) bool {
	return strings.Contains(code, "USD") || strings.Contains(code, "CNY")
}

func PNorm(min, max float64) *Param {
	return &Param{
		VType: VTypeNorm,
		Min:   min,
		Max:   max,
	}
}

func PNormF(min, max, mean, rate float64) *Param {
	return &Param{
		VType: VTypeNorm,
		Min:   min,
		Max:   max,
		Mean:  mean,
		Rate:  rate,
	}
}

func PUniform(min, max float64) *Param {
	return &Param{
		VType: VTypeUniform,
		Min:   min,
		Max:   max,
	}
}

/*
OptSpace Returns a uniformly distributed interval for use in hyperparameter searches 返回一个均匀分布的区间，用于超参数搜索
*/
func (p *Param) OptSpace() (float64, float64) {
	if p.VType == VTypeNorm {
		return p.toNormXSpace()
	} else {
		return p.Min, p.Max
	}
}

/*
ToRegular Hyperparameter values that map a uniform distribution to a normal distribution 将均匀分布映射为正态分布的超参数值
*/
func (p *Param) ToRegular(x float64) (float64, bool) {
	if p.VType == VTypeNorm {
		scale := max(p.Mean-p.Min, p.Max-p.Mean)
		normVal := p.norm(x) / p.getEdgeY()
		x = normVal*scale + p.Mean
	}
	if x < p.Min || x > p.Max {
		return min(p.Max, max(x, p.Min)), false
	}
	return x, true
}

func (p *Param) getEdgeY() float64 {
	if p.edgeY == 0 {
		p.edgeY = p.norm(0.5)
	}
	return p.edgeY
}

/*
Given the current y value range, return the x corresponding value range of the inverse normal distribution
已知当前y值域，返回反正态分布的x对应值域
*/
func (p *Param) toNormXSpace() (float64, float64) {
	// 使用pow(x, 3) + x/(20*rate) 来拟合符合正态分布的值域
	// x : [-0.5, 0.5]
	// y : [-0.15, 0.15]当rate=1
	neg, pos := p.Mean-p.Min, p.Max-p.Mean
	xMin, xMax := -0.5, 0.5
	height := p.getEdgeY()
	y := float64(0)
	if neg < pos {
		y = -neg * height / pos
	} else {
		y = pos * height / neg
	}
	x := p.calcNormX(y, 1e-6, 1000)
	if y > 0 {
		xMax = x
	} else {
		xMin = x
	}
	return xMin, xMax
}

func (p *Param) norm(x float64) float64 {
	return math.Pow(x, 3) + x/(p.Rate*20)
}

/*
Derivative of norm
*/
func (p *Param) dNorm(x float64) float64 {
	return 3*math.Pow(x, 2) + 1/(p.Rate*20)
}

/*
Calculate y=pow(x, 3) + x/20 when y is given, the value of x
计算y=pow(x, 3) + x/20当给定y时，x的值
*/
func (p *Param) calcNormX(y, tol float64, maxIter int) float64 {
	x := float64(0)
	for i := 0; i < maxIter; i++ {
		x = x - (p.norm(x)-y)/p.dNorm(x)
		if math.Abs(p.norm(x)-y) < tol {
			return x
		}
	}
	return x
}

func IsLimitOrder(t int) bool {
	return t == OrderTypeLimit || t == OrderTypeLimitMaker
}

func MarshalYaml(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	err := enc.Encode(v)
	_ = enc.Close()
	return buf.Bytes(), err
}

func CountDigit(text string) int {
	count := 0
	for _, c := range text {
		if unicode.IsDigit(c) {
			count += 1
		}
	}
	return count
}

func SplitDigits(s string) []string {
	var result []string
	var currentDigits strings.Builder

	for _, char := range s {
		if unicode.IsDigit(char) {
			currentDigits.WriteRune(char)
		} else {
			if currentDigits.Len() > 0 {
				result = append(result, currentDigits.String())
				currentDigits.Reset()
			}
		}
	}

	if currentDigits.Len() > 0 {
		result = append(result, currentDigits.String())
	}

	return result
}

func SetLogCap(path string) {
	if LogFile == path {
		return
	}
	LogFile = ""
	if CapOut != nil {
		CapOut.Stop()
		CapOut = nil
	}
	var flags = os.O_CREATE | os.O_WRONLY
	if LiveMode {
		// 实时模式日志追加保存
		flags = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		log.Error("open file to write log fail", zap.Error(err))
		return
	}
	CapOut, err = log.NewOutCapture(file, file)
	if err != nil {
		log.Error("new out capture fail", zap.Error(err))
	} else {
		CapOut.Start()
		LogFile = path
	}
	ExitCalls = append(ExitCalls, func() {
		if CapOut != nil {
			CapOut.Stop()
		}
	})
}

func GetOdBook(pair string) (*banexg.OrderBook, bool) {
	lockOdBook.Lock()
	data, ok := OdBooks[pair]
	lockOdBook.Unlock()
	return data, ok
}

func SetOdBook(pair string, book *banexg.OrderBook) {
	lockOdBook.Lock()
	OdBooks[pair] = book
	lockOdBook.Unlock()
}

func (t *TimeValueRange) SetEnd(timeMS int64, val float64) {
	t.StopMS = timeMS
	t.StopValue = val
	t.ValueChg = val - t.StartValue
}
