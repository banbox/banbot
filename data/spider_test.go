package data

import (
	"context"
	"fmt"
	"testing"

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
	"go.uber.org/zap"
)

func TestWatchOhlcv(t *testing.T) {
	core.SetRunMode(core.RunModeLive)
	err := initApp()
	if err != nil {
		panic(err)
	}
	client, err := NewKlineWatcher("127.0.0.1:6789")
	if err != nil {
		panic(err)
	}
	client.OnKLineMsg = func(msg *KLineMsg) {
		if len(msg.Arr) == 0 {
			return
		}
		code := fmt.Sprintf("%s.%s.%s", msg.ExgName, msg.Market, msg.Pair)
		k := msg.Arr[0]
		dateStr := btime.ToDateStr(k.Time, core.DefaultDateFmt)
		barStr := fmt.Sprintf("%f %f %f %f %f", k.Open, k.High, k.Low, k.Close, k.Volume)
		log.Info("receive", zap.String("code", code), zap.Int("num", len(msg.Arr)),
			zap.Int("tfSecs", msg.TFSecs), zap.Int("intv", msg.Interval),
			zap.String("date", dateStr), zap.String("bar", barStr))
	}
	market, quote := banexg.MarketLinear, "USDT"
	codes := []string{"BTC", "ETH", "SOL"}
	jobs := make([]WatchJob, 0, len(codes))
	for _, code := range codes {
		var symbol string
		if market == banexg.MarketSpot {
			symbol = fmt.Sprintf("%s/%s", code, quote)
		} else if market == banexg.MarketLinear {
			symbol = fmt.Sprintf("%s/%s:%s", code, quote, quote)
		} else if market == banexg.MarketInverse {
			symbol = fmt.Sprintf("%s/%s:%s", quote, code, quote)
		} else {
			panic("unsupported market")
		}
		jobs = append(jobs, WatchJob{
			Symbol:    symbol,
			TimeFrame: "1m",
		})
	}
	err = client.WatchJobs("binance", market, "ohlcv", jobs...)
	if err != nil {
		panic(err)
	}
	err = client.RunForever()
	if err != nil {
		panic(err)
	}
}

func initApp() *errs.Error {
	var args config.CmdArgs
	args.Init()
	errs.PrintErr = utils.PrintErr
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	core.StopAll = cancel
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

func TestSaveKlines(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	var arr []*banexg.Kline
	err_ := utils2.ReadJsonFile("testdata/btc_1m.json", &arr, utils2.JsonNumDefault)
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
`, sid, sid, sid, sid, sid))
	if err != nil {
		panic(err)
	}
	conn.Release()
	{
		db, err := orm.BanPubConn(true)
		if err != nil {
			panic(err)
		}
		_, _ = db.ExecContext(context.Background(), "delete from kline_un where sid=?", sid)
		_, _ = db.ExecContext(context.Background(), "delete from sranges where sid=?", sid)
		_ = db.Close()
	}
	core.SetRunMode(core.RunModeBackTest)
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	exs := orm.GetSymbolByID(sid)
	for i, bar := range arr {
		btime.CurTimeMS = bar.Time + tfMSecs
		sess, conn, err = orm.Conn(nil)
		if err != nil {
			panic(err)
		}
		_, err = sess.InsertKLinesAuto(timeFrame, exs, []*banexg.Kline{bar}, true)
		conn.Release()
		if i == 8 {
			break
		}
	}
}
