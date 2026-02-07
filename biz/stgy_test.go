package biz

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
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
	config.Args.SetLog(true)
	err = exg.Setup()
	if err != nil {
		return err
	}
	return orm.Setup()
}

func TestStratRun(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	core.ExgName = "binance"
	core.Market = "linear"
	exchange := exg.Default
	_, err = orm.LoadMarkets(exchange, false)
	if err != nil {
		panic(err)
	}
	err = orm.EnsureExgSymbols(exchange)
	if err != nil {
		panic(err)
	}
	barNum := 300
	tf := "1h"
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	pairs := []string{"ETC/USDT:USDT", "AVAX/USDT:USDT", "DOT/USDT:USDT", "LTC/USDT:USDT", "ETH/USDT:USDT",
		"ARPA/USDT:USDT", "SOL/USDT:USDT", "1000XEC/USDT:USDT", "DOGE/USDT:USDT", "MANA/USDT:USDT",
		"SAND/USDT:USDT", "BLUR/USDT:USDT", "1000LUNC/USDT:USDT", "BCH/USDT:USDT", "ID/USDT:USDT",
		"SFP/USDT:USDT", "WAVES/USDT:USDT", "CHZ/USDT:USDT", "MASK/USDT:USDT", "BNB/USDT:USDT"}
	stagy := strat.New(&config.RunPolicyConfig{Name: "hammer"})
	if stagy == nil {
		panic("load strategy fail")
	}
	accJobs := strat.GetJobs(config.DefAcc)
	for _, symbol := range pairs {
		exs, err := orm.GetExSymbolCur(symbol)
		if err != nil {
			panic(err)
		}
		envKey := strings.Join([]string{symbol, tf}, "_")
		env, _ := ta.NewBarEnv(core.ExgName, core.Market, symbol, tf)
		env.MaxCache = core.NumTaCache
		strat.Envs[envKey] = env
		job := &strat.StratJob{
			Strat:         stagy,
			Env:           env,
			Symbol:        exs,
			TimeFrame:     tf,
			TPMaxs:        make(map[int64]float64),
			CloseLong:     true,
			CloseShort:    true,
			ExgStopLoss:   true,
			ExgTakeProfit: true,
		}
		jobs, ok := accJobs[envKey]
		if !ok {
			jobs = make(map[string]*strat.StratJob)
			accJobs[envKey] = jobs
		}
		jobs[job.Strat.Name] = job
	}
	curTime := utils2.AlignTfMSecs(btime.TimeMS(), tfMSecs) - tfMSecs*int64(barNum)
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
			env, _ := strat.Envs[envKey]
			env.OnBar(bar.Time, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, 0, 0, 0)
			com.SetBarPrice(pair, bar.Close)
			jobs, _ := accJobs[envKey]
			for _, job := range jobs {
				job.InitBar(nil)
				job.Strat.OnBar(job)
			}
		}
	}
}
