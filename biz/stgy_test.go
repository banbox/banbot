package biz

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strategy"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	ta "github.com/banbox/banta"
	"strings"
	"testing"
)

func initApp() *errs.Error {
	var args config.CmdArgs
	args.Init()
	err := config.LoadConfig(&args)
	if err != nil {
		return err
	}
	log.Setup(config.Args.Debug, config.Args.Logfile)
	err = exg.Setup()
	if err != nil {
		return err
	}
	return orm.Setup()
}

func TestStagyRun(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	core.ExgName = "binance"
	core.Market = "linear"
	exchange, err := exg.Get()
	if err != nil {
		panic(err)
	}
	_, err = exchange.LoadMarkets(false, nil)
	if err != nil {
		panic(err)
	}
	err = orm.EnsureExgSymbols(exchange)
	if err != nil {
		panic(err)
	}
	barNum := 300
	tf := "1h"
	tfMSecs := int64(utils.TFToSecs(tf) * 1000)
	pairs := []string{"ETC/USDT:USDT", "AVAX/USDT:USDT", "DOT/USDT:USDT", "LTC/USDT:USDT", "ETH/USDT:USDT",
		"ARPA/USDT:USDT", "SOL/USDT:USDT", "1000XEC/USDT:USDT", "DOGE/USDT:USDT", "MANA/USDT:USDT",
		"SAND/USDT:USDT", "BLUR/USDT:USDT", "1000LUNC/USDT:USDT", "BCH/USDT:USDT", "ID/USDT:USDT",
		"SFP/USDT:USDT", "WAVES/USDT:USDT", "CHZ/USDT:USDT", "MASK/USDT:USDT", "BNB/USDT:USDT"}
	stagy := strategy.Get("hammer")
	if stagy == nil {
		panic("load strategy fail")
	}
	for _, symbol := range pairs {
		exs, err := orm.GetExSymbolCur(symbol)
		if err != nil {
			panic(err)
		}
		envKey := strings.Join([]string{symbol, tf}, "_")
		env := &ta.BarEnv{
			Exchange:   core.ExgName,
			MarketType: core.Market,
			Symbol:     symbol,
			TimeFrame:  tf,
			TFMSecs:    tfMSecs,
			MaxCache:   core.NumTaCache,
			Data:       map[string]interface{}{"sid": exs.ID},
		}
		strategy.Envs[envKey] = env
		job := &strategy.StagyJob{
			Stagy:         stagy,
			Env:           env,
			Symbol:        exs,
			TimeFrame:     tf,
			TPMaxs:        make(map[int64]float64),
			OpenLong:      true,
			OpenShort:     true,
			CloseLong:     true,
			CloseShort:    true,
			ExgStopLoss:   true,
			ExgTakeProfit: true,
		}
		if jobs, ok := strategy.Jobs[envKey]; ok {
			strategy.Jobs[envKey] = append(jobs, job)
		} else {
			strategy.Jobs[envKey] = []*strategy.StagyJob{job}
		}
	}
	curTime := utils.AlignTfMSecs(btime.TimeMS(), tfMSecs) - tfMSecs*int64(barNum)
	norBar := banexg.Kline{Time: curTime, Open: 0.1, High: 0.1, Low: 0.1, Close: 0.1, Volume: 0.1}
	for i := 0; i < barNum; i++ {
		curTime += tfMSecs
		bar := &banexg.PairTFKline{
			Kline:     norBar,
			TimeFrame: tf,
		}
		bar.Time = curTime
		if i%5 == 0 {
			// 构造一个锤子
			bar.Kline = banexg.Kline{Time: curTime, Open: 0.1, High: 0.1, Low: 0.08, Close: 0.097, Volume: 0.1}
		}
		for _, pair := range pairs {
			bar.Symbol = pair
			envKey := strings.Join([]string{pair, tf}, "_")
			env, _ := strategy.Envs[envKey]
			env.OnBar(bar.Time, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
			core.SetBarPrice(pair, bar.Close)
			jobs, _ := strategy.Jobs[envKey]
			for _, job := range jobs {
				job.InitBar(nil)
				job.Stagy.OnBar(job)
			}
		}
	}
}