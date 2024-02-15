package data

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"testing"
)

func TestWatchOhlcv(t *testing.T) {
	client, err := NewKlineWatcher("127.0.0.1:6789")
	if err != nil {
		panic(err)
	}
	err = client.WatchJobs("binance", banexg.MarketLinear, "ohlcv", WatchJob{
		Symbol:    "BTC/USDT:USDT",
		TimeFrame: "1m",
	})
	if err != nil {
		panic(err)
	}
	client.RunForever()
}

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

func TestSaveKlines(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	var arr []*banexg.Kline
	err_ := utils2.ReadJsonFile("testdata/btc_1m.json", &arr)
	if err_ != nil {
		panic(err_)
	}
	sid := int32(-1)
	timeFrame := "1m"
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		panic(err)
	}
	err = sess.Exec(fmt.Sprintf(`
delete from kline_1m where sid=%v;
delete from kline_5m where sid=%v;
delete from kline_15m where sid=%v;
delete from kline_1h where sid=%v;
delete from kline_1d where sid=%v;
delete from kline_un where sid=%v;
delete from kinfo where sid=%v;
delete from khole where sid=%v;`, sid, sid, sid, sid, sid, sid, sid, sid))
	if err != nil {
		panic(err)
	}
	conn.Release()
	core.RunMode = core.RunModeBackTest
	tfMSecs := int64(utils.TFToSecs(timeFrame) * 1000)
	for i, bar := range arr {
		btime.CurTimeMS = bar.Time + tfMSecs
		sess, conn, err = orm.Conn(nil)
		if err != nil {
			panic(err)
		}
		_, err = sess.InsertKLinesAuto(timeFrame, sid, []*banexg.Kline{bar})
		conn.Release()
		if i == 8 {
			break
		}
	}
}