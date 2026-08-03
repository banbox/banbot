package orm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg/errs"
)

func TestContextualKlineOperationErrorMakesEmptyFailureAuditable(t *testing.T) {
	err := contextualKlineOperationError("download", "BTC/USDT:USDT", "15m", 1_785_024_000_000, 1_785_628_800_000, 672,
		errs.NewMsg(errs.CodeRunTime, ""))
	if err == nil || !strings.Contains(err.Message(), "BTC/USDT:USDT") ||
		!strings.Contains(err.Message(), "empty error code") {
		t.Fatalf("fetch error = %#v", err)
	}
}

func TestAutoFetchOhlcv(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	exchange := exg.Default
	// 交易对初始化
	err = EnsureExgSymbols(exchange)
	if err != nil {
		panic(err)
	}
	err = InitListDates()
	if err != nil {
		panic(err)
	}
	exs, err := GetExSymbol(exchange, "BTC/USDT:USDT")
	if err != nil {
		panic(err)
	}
	stop := btime.TimeMS()
	start := int64(0)
	_, klines, err := AutoFetchOHLCV(exchange, exs, "1m", start, stop, 1000, false, nil)
	if err != nil {
		panic(err)
	}
	for _, k := range klines {
		fmt.Printf("%v %f %f %f %f %f\n", k.Time, k.Open, k.High, k.Low, k.Close, k.Volume)
	}
}

func TestFetchOhlcvs(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	exchange := exg.Default
	pair := "BTC/USDT:USDT"
	curMS := btime.TimeMS()
	limit := 127
	start := curMS - int64(limit)*60000
	arr, err := exchange.FetchOHLCV(pair, "1m", start, limit, nil)
	if err != nil {
		panic(err)
	}
	for _, k := range arr {
		fmt.Printf("{%v,%f,%f,%f,%f,%f},\n", k.Time, k.Open, k.High, k.Low, k.Close, k.Volume)
	}
}

func TestBulkDownOHLCV(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	exchange := exg.Default
	_, err = LoadMarkets(exchange, false)
	if err != nil {
		panic(err)
	}
	ldSess, ldConn, err := Conn(nil)
	if err != nil {
		panic(err)
	}
	err = ldSess.LoadExgSymbols(core.ExgName)
	ldConn.Release()
	if err != nil {
		panic(err)
	}
	exsMap := GetExSymbols(core.ExgName, core.Market)
	startMS := int64(1696982400000)
	err = BulkDownOHLCV(exchange, exsMap, "1d", startMS, 0, 1, nil)
	if err != nil {
		panic(err)
	}
}
