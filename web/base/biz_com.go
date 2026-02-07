package base

import (
	"fmt"
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/gofiber/contrib/websocket"
	"go.uber.org/zap"
)

var (
	exgInits    = map[banexg.BanExchange]bool{}
	exgInitLock deadlock.Mutex
	receiver    *data.KLineWatcher
	wsSubs      = map[string]map[*WsClient]bool{}
	wsSubLock   deadlock.Mutex
)

func InitExg(exchange banexg.BanExchange) *errs.Error {
	exgInitLock.Lock()
	defer exgInitLock.Unlock()
	if _, ok := exgInits[exchange]; ok {
		return nil
	}
	err := orm.InitExg(exchange)
	if err != nil {
		return err
	}
	exgInits[exchange] = true
	return nil
}

func GetExg(name, market, ctType string, load bool) (banexg.BanExchange, *errs.Error) {
	exchange, err := exg.GetWith(name, market, ctType)
	if err != nil {
		return nil, err
	}
	if !load {
		return exchange, nil
	}
	return exchange, InitExg(exchange)
}

func ArrKLines(klines []*banexg.Kline) [][]float64 {
	res := make([][]float64, 0, len(klines))
	for _, k := range klines {
		res = append(res, []float64{float64(k.Time), k.Open, k.High, k.Low, k.Close, k.Volume, k.BuyVolume})
	}
	return res
}

func SetKlineSub(client *WsClient, isSub, lock bool, keys ...string) {
	if lock {
		wsSubLock.Lock()
		defer wsSubLock.Unlock()
	}
	for _, key := range keys {
		clients, ok := wsSubs[key]
		if !ok {
			clients = make(map[*WsClient]bool)
			wsSubs[key] = clients
		}
		if isSub {
			clients[client] = true
		} else {
			delete(clients, client)
		}
	}
}

func RunReceiver() {
	var err *errs.Error
	receiver, err = data.NewKlineWatcher(config.SpiderAddr)
	if err != nil {
		log.Warn("connect spider fail", zap.String("addr", config.SpiderAddr), zap.String("err", err.Short()))
		return
	}
	receiver.OnKLineMsg = klineHandler
	// 暂时只监听默认交易所的默认市场
	exsList := orm.GetExSymbols(core.ExgName, core.Market)
	if len(exsList) == 0 {
		return
	}
	jobs := make([]data.WatchJob, 0, len(exsList))
	timeFrame := "1m"
	for _, exs := range exsList {
		jobs = append(jobs, data.WatchJob{Symbol: exs.Symbol, TimeFrame: timeFrame})
	}
	err = receiver.WatchJobs(core.ExgName, core.Market, core.WsSubKLine, jobs...)
	if err != nil {
		log.Error("subscribe spider fail", zap.Int("num", len(jobs)), zap.Error(err))
	}
	log.Info("subscribe kline from spider success")
	err = receiver.RunForever()
	if err != nil {
		log.Error("receive spider fail", zap.Error(err))
	}
}

func klineHandler(msg *data.KLineMsg) {
	if len(msg.Arr) == 0 {
		return
	}
	wsSubLock.Lock()
	defer wsSubLock.Unlock()
	key := fmt.Sprintf("%s_%s_%s", msg.ExgName, msg.Market, msg.Pair)
	clients, _ := wsSubs[key]
	if len(clients) == 0 {
		return
	}
	wsMsg := map[string]interface{}{
		"a":    "subscribe",
		"bars": ArrKLines(msg.Arr),
		"secs": msg.TFSecs,
		"upd":  msg.Interval,
	}
	raw, err := utils2.Marshal(wsMsg)
	if err != nil {
		log.Warn("marshal ws kline fail", zap.Error(err))
		return
	}

	for c := range clients {
		err_ := c.Conn.WriteMessage(websocket.TextMessage, raw)
		if err_ != nil {
			log.Debug("write to ws fail", zap.Error(err_))
			c.Close(false)
			delete(clients, c)
		}
	}
}
