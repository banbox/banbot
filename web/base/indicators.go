package base

import (
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"github.com/gofiber/fiber/v2"
	"math"
	"sort"
	"strconv"
	"strings"
)

type Figure struct {
	Key       string  `json:"key"`   // tr
	Title     string  `json:"title"` // TR:
	Type      string  `json:"type"`  // tag/line
	BaseValue float64 `json:"baseValue"`
}

type DrawInd struct {
	Name       string
	Title      string
	IsMain     bool
	CalcParams []float64 // 参数
	Figures    []*Figure
	FigureTpl  string // 客户端会使用此模板动态生成Figures
	FigureType string // 默认空，客户端默认line
	doCalc     func(e *ta.BarEnv, params []float64) []float64
}

type AdvInd struct {
	*DrawInd
	Calc func(kline [][]float64, params []float64) (interface{}, error)
}

var (
	baseInds = map[string]*DrawInd{
		"RMA": {
			Title:      "RMA",
			IsMain:     true,
			CalcParams: []float64{5, 10, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.RMA(e.Close, int(p)).Get(0)
				}
				return res
			},
		},
		"WMA": {
			Title:      "WMA",
			IsMain:     true,
			CalcParams: []float64{10, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.WMA(e.Close, int(p)).Get(0)
				}
				return res
			},
		},
		"VWMA": {
			Title:      "VWMA",
			IsMain:     true,
			CalcParams: []float64{10, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.VWMA(e.Close, e.Volume, int(p)).Get(0)
				}
				return res
			},
		},
		"HMA": {
			Title:      "HMA",
			IsMain:     true,
			CalcParams: []float64{10, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.HMA(e.Close, int(p)).Get(0)
				}
				return res
			},
		},
		"KAMA": {
			Title:      "KAMA",
			IsMain:     true,
			CalcParams: []float64{10, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.KAMA(e.Close, int(p)).Get(0)
				}
				return res
			},
		},
		"ALMA": {
			Title:      "ALMA",
			IsMain:     true,
			CalcParams: []float64{10, 6, 0.85},
			Figures: []*Figure{
				{"alma", "ALMA: ", "line", 0},
			},
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := []float64{ta.ALMA(e.Close, int(params[0]), params[1], params[2]).Get(0)}
				return res
			},
		},
		"TR": {
			Title: "TR 真实振幅",
			Figures: []*Figure{
				{"tr", "TR: ", "line", 0},
			},
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				val := ta.TR(e.High, e.Low, e.Close).Get(0)
				return []float64{val}
			},
		},
		"ATR": {
			Title:      "ATR 平均真实振幅",
			CalcParams: []float64{14, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.ATR(e.High, e.Low, e.Close, int(p)).Get(0)
				}
				return res
			},
		},
		"StdDev": {
			Title:      "StdDev 标准差",
			CalcParams: []float64{7},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					dev := ta.StdDev(e.Close, int(p))
					res[i] = dev.Get(0)
				}
				return res
			},
		},
		"TD": {
			Title: "TD 狄马克序列(神奇九转)",
			Figures: []*Figure{
				{"td", "TD: ", "line", 0},
			},
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				val := ta.TD(e.Close).Get(0)
				return []float64{val}
			},
		},
		"ADX": {
			Title:      "ADX",
			CalcParams: []float64{14, 30},
			FigureTpl:  "{i}",
			doCalc: func(e *ta.BarEnv, params []float64) []float64 {
				res := make([]float64, len(params))
				for i, p := range params {
					res[i] = ta.ADX(e.High, e.Low, e.Close, int(p)).Get(0)
				}
				return res
			},
		},
	}
	advInds = map[string]*AdvInd{
		"ChanLun": {
			DrawInd: &DrawInd{
				Title:  "Chan 缠论",
				IsMain: true,
			},
			Calc: func(kline [][]float64, params []float64) (interface{}, error) {
				cg := &ta.CGraph{}
				for i, k := range kline {
					b := &ta.Kline{
						Time:   int64(k[0]),
						Open:   k[1],
						High:   k[2],
						Low:    k[3],
						Close:  k[4],
						Volume: k[5],
					}
					if len(k) > 6 {
						b.BuyVolume = k[6]
					}
					if len(k) > 7 {
						b.Quote = k[7]
					}
					if len(k) > 8 {
						b.TradeNum = int64(k[8])
					}
					cg.AddBars(i+1, b)
				}
				cg.Parse()
				lines := cg.Dump()
				res := make([][]float64, 0, len(lines))
				for _, l := range lines {
					res = append(res, []float64{float64(l.StartPos), l.StartPrice, float64(l.StopPos), l.StopPrice})
				}
				return res, nil
			},
		},
	}
	IndsCache = make([]map[string]interface{}, 0)
)

func init() {
	for name, ind := range baseInds {
		ind.Name = name
		IndsCache = append(IndsCache, ind.ToMap())
	}
	for name, ind := range advInds {
		ind.Name = name
		IndsCache = append(IndsCache, ind.ToMap())
	}

	sort.Slice(IndsCache, func(i, j int) bool {
		a := IndsCache[i]["name"].(string)
		b := IndsCache[j]["name"].(string)
		return a < b
	})
}

func CalcInd(name string, kline [][]float64, params []float64) (interface{}, error) {
	indi, _ := advInds[name]
	if indi != nil {
		return indi.Calc(kline, params)
	}
	ind, ok := baseInds[name]
	if !ok {
		return nil, &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "unsupported indicator: " + name,
		}
	}
	return ind.Calc(kline, params)
}

func (d *DrawInd) Calc(kline [][]float64, params []float64) ([]map[string]float64, error) {
	if len(kline) < 2 {
		return nil, nil
	}
	tfMSecs := int64(kline[1][0] - kline[0][0])
	timeFrame := utils2.SecsToTF(int(tfMSecs / 1000))

	var env = &ta.BarEnv{
		TimeFrame:  timeFrame,
		TFMSecs:    tfMSecs,
		Exchange:   "binance",
		MarketType: "linear",
	}
	figures := d.Figures
	if d.FigureTpl != "" {
		if strings.Contains(d.FigureTpl, "{i}") {
			for i := range params {
				key := strings.Replace(d.FigureTpl, "{i}", strconv.Itoa(i+1), 1)
				figures = append(figures, &Figure{Key: key})
			}
		} else {
			figures = append(figures, &Figure{Key: d.FigureTpl})
		}
	}
	res := make([]map[string]float64, 0, len(kline))
	for _, k := range kline {
		var buyVolume = float64(0)
		var quote = float64(0)
		var tradeNum int64
		if len(k) > 6 {
			buyVolume = k[6]
		}
		if len(k) > 7 {
			quote = k[7]
		}
		if len(k) > 8 {
			tradeNum = int64(k[8])
		}
		err := env.OnBar(int64(k[0]), k[1], k[2], k[3], k[4], k[5], quote, buyVolume, tradeNum)
		if err != nil {
			return nil, err
		}
		arr := d.doCalc(env, params)
		data := make(map[string]float64)
		for i, v := range arr {
			if i >= len(figures) {
				break
			}
			if math.IsInf(v, 0) || math.IsNaN(v) {
				continue
			}
			data[figures[i].Key] = v
		}
		res = append(res, data)
	}
	return res, nil
}

func (d *DrawInd) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"name":        d.Name,
		"title":       d.Title,
		"is_main":     d.IsMain,
		"calcParams":  d.CalcParams,
		"figures":     d.Figures,
		"figure_tpl":  d.FigureTpl,
		"figure_type": d.FigureType,
	}
}
