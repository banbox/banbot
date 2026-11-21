package strat

import (
	"math"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
)

type refTestExchange struct {
	banexg.BanExchange
	markets map[string]*banexg.Market
}

func newRefTestExchange(marketType string, symbols ...string) *refTestExchange {
	markets := make(map[string]*banexg.Market, len(symbols))
	for _, sym := range symbols {
		base, quote, settle, _ := core.SplitSymbol(sym)
		markets[sym] = &banexg.Market{
			ID:           sym,
			Symbol:       sym,
			Base:         base,
			Quote:        quote,
			Settle:       settle,
			Type:         marketType,
			Contract:     banexg.IsContract(marketType),
			Linear:       marketType == banexg.MarketLinear,
			ContractSize: 1,
		}
	}
	ex := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: marketType, Markets: markets}}
	return &refTestExchange{BanExchange: ex, markets: markets}
}

func (e *refTestExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if m, ok := e.markets[symbol]; ok {
		return m, nil
	}
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "market not found: %s", symbol)
}

func TestDataHubLongHistorySMABeyond512AndWarmupScaling(t *testing.T) {
	hub := NewDataHub()
	if hub.MaxCache() < 1500 {
		t.Fatalf("NewDataHub default MaxCache = %d, want >= 1500", hub.MaxCache())
	}

	exs := &orm.ExSymbol{ID: 7, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT"}
	const tf = "1h"
	const stepMS = int64(3600_000)

	var df *DataFields
	sma1000 := math.NaN()
	for i := 1; i <= 1200; i++ {
		ts := int64(i) * stepMS
		price := float64(i)
		df = hub.Set(&orm.DataSeries{
			Source:    orm.SeriesSourceKline,
			Sid:       exs.ID,
			ExSymbol:  exs,
			TimeFrame: tf,
			TimeMS:    ts,
			EndMS:     ts + stepMS,
			Closed:    true,
			Values: map[string]any{
				"open":       price - 0.5,
				"high":       price + 1.0,
				"low":        price - 1.0,
				"close":      price,
				"volume":     100.0 + float64(i%10),
				"quote":      price * 100.0,
				"buy_volume": 50.0,
				"trade_num":  int64(42),
			},
		})
		sma1000 = ta.SMA(df.Series("close"), 1000).Get(0)
	}
	if df == nil || df.Series("close") == nil {
		t.Fatal("expected close series after feeding 1200 bars")
	}
	if df.Series("close").Len() != 1200 {
		t.Fatalf("close.Len() = %d, want 1200", df.Series("close").Len())
	}
	// Average of 201..1200 = (201 + 1200) / 2 = 700.5
	if math.IsNaN(sma1000) || math.Abs(sma1000-700.5) > 1e-9 {
		t.Fatalf("SMA(close, 1000) = %v, want 700.5", sma1000)
	}

	// Verify WarmupNum > 1500 automatically raises job DataHub capacity to WarmupNum + 100.
	job := &StratJob{
		Strat:     &TradeStrat{WarmupNum: 1800},
		Symbol:    exs,
		TimeFrame: "15m",
	}
	sma1800 := math.NaN()
	for i := 1; i <= 1850; i++ {
		ts := int64(i) * stepMS
		updated := job.SetData(&orm.DataSeries{
			Source:    orm.SeriesSourceKline,
			Sid:       exs.ID,
			ExSymbol:  exs,
			TimeFrame: tf,
			TimeMS:    ts,
			EndMS:     ts + stepMS,
			Closed:    true,
			Values:    map[string]any{"open": float64(i), "high": float64(i), "low": float64(i), "close": float64(i), "volume": 10.0},
		})
		sma1800 = ta.SMA(updated.Series("close"), 1800).Get(0)
	}
	jobDF := job.DataHub.Get(tf, orm.SeriesSourceKline, exs.ID)
	if jobDF == nil || jobDF.MaxCache() != 1900 || jobDF.Series("close").Len() != 1850 {
		t.Fatalf("WarmupNum-scaled DataFields: maxCache=%d len=%d, want 1900 / 1850",
			jobDF.MaxCache(), jobDF.Series("close").Len())
	}
	// Average of 51..1850 = (51 + 1850) / 2 = 950.5
	if math.IsNaN(sma1800) || math.Abs(sma1800-950.5) > 1e-9 {
		t.Fatalf("SMA(close, 1800) = %v, want 950.5", sma1800)
	}

	// Verify DataFields.SetMaxCache truncates explicitly.
	jobDF.SetMaxCache(300)
	if jobDF.MaxCache() != 300 || jobDF.Series("close").Len() != 300 || jobDF.BarEnv().MaxCache != 300 {
		t.Fatalf("SetMaxCache(300) failed: maxCache=%d len=%d envMax=%d",
			jobDF.MaxCache(), jobDF.Series("close").Len(), jobDF.BarEnv().MaxCache)
	}
}

func TestDataFieldsFullBarEnvIndicatorsAndCustomNullSemantics(t *testing.T) {
	exs := &orm.ExSymbol{ID: 19, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "ETH/USDT:USDT"}
	sub := &DataSub{Source: orm.SeriesSourceKline, ExSymbol: exs, TimeFrame: "4h"}
	hub := NewDataHub()
	hub.Configure([]*DataSub{sub})

	// Before any bar arrives, BarEnv() must be nil so callers can safely nil-check.
	pre := hub.Get("4h", orm.SeriesSourceKline, exs.ID)
	if pre == nil || pre.BarEnv() != nil {
		t.Fatalf("expected pre-bar DataFields.BarEnv() to be nil, got %+v", pre)
	}

	refEnv, err := ta.NewBarEnv(exs.Exchange, exs.Market, exs.Symbol, "4h")
	if err != nil {
		t.Fatal(err)
	}

	const stepMS = int64(4 * 3600_000)
	var df *DataFields
	for i := 1; i <= 40; i++ {
		ts := int64(i) * stepMS
		wave := math.Sin(float64(i) * 0.4)
		closeP := 2000.0 + float64(i)*5.0 + wave*30.0
		openP := closeP - wave*10.0
		highP := max(openP, closeP) + 15.0 + math.Abs(wave)*5.0
		lowP := min(openP, closeP) - 12.0 - math.Abs(wave)*4.0
		vol := 500.0 + float64((i*37)%200)
		quote := closeP * vol
		buyVol := vol * 0.55
		tradeNum := int64(1000 + i)

		if err := refEnv.OnBar(ts, openP, highP, lowP, closeP, vol, quote, buyVol, tradeNum); err != nil {
			t.Fatal(err)
		}

		values := map[string]any{
			"open":         openP,
			"high":         highP,
			"low":          lowP,
			"close":        closeP,
			"volume":       vol,
			"quote":        quote,
			"buy_volume":   buyVol,
			"trade_num":    tradeNum,
			"funding_ext":  0.0001 * float64(i),
			"regime_label": "trend",
			"explicit_nil": nil,
		}
		if i == 40 {
			delete(values, "stale_col")
		} else {
			values["stale_col"] = int64(i)
		}
		df = hub.Set(&orm.DataSeries{
			Source:    orm.SeriesSourceKline,
			Sid:       exs.ID,
			ExSymbol:  exs,
			TimeFrame: "4h",
			TimeMS:    ts,
			EndMS:     ts + stepMS,
			Closed:    true,
			Values:    values,
		})

		env := df.BarEnv()
		if env == nil {
			t.Fatalf("bar %d: df.BarEnv() is nil", i)
		}
		if env.Open == nil || env.High == nil || env.Low == nil || env.Close == nil ||
			env.Volume == nil || env.Quote == nil || env.BuyVolume == nil || env.TradeNum == nil {
			t.Fatalf("bar %d: BarEnv standard OHLCV series has nil pointer: %+v", i, env)
		}
		closeSer := df.Series("close")
		if closeSer != env.Close || closeSer.Env != env || closeSer.Env.High != env.High {
			t.Fatalf("bar %d: close series is not bound to full BarEnv", i)
		}

		// Compute BarEnv-dependent indicators on every bar (just as in real strategies).
		gotATR := ta.ATR(closeSer.Env.High, closeSer.Env.Low, closeSer, 14).Get(0)
		wantATR := ta.ATR(refEnv.High, refEnv.Low, refEnv.Close, 14).Get(0)
		gotMFI := ta.MFI(env, 14).Get(0)
		wantMFI := ta.MFI(refEnv, 14).Get(0)
		gotWillR := ta.WillR(env, 14).Get(0)
		wantWillR := ta.WillR(refEnv, 14).Get(0)
		ho, hh, hl, hc := ta.HeikinAshi(env)
		wHo, wHh, wHl, wHc := ta.HeikinAshi(refEnv)

		if i >= 20 {
			for name, pair := range map[string][2]float64{
				"ATR":   {gotATR, wantATR},
				"MFI":   {gotMFI, wantMFI},
				"WillR": {gotWillR, wantWillR},
				"HA_O":  {ho.Get(0), wHo.Get(0)},
				"HA_H":  {hh.Get(0), wHh.Get(0)},
				"HA_L":  {hl.Get(0), wHl.Get(0)},
				"HA_C":  {hc.Get(0), wHc.Get(0)},
			} {
				if math.IsNaN(pair[0]) || math.Abs(pair[0]-pair[1]) > 1e-9 {
					t.Fatalf("bar %d %s = %v, want %v", i, name, pair[0], pair[1])
				}
			}
		}
	}

	// Verify custom extension columns and NULL/missing semantics are 100% preserved.
	if extSer := df.Series("funding_ext"); extSer == nil || extSer.Len() != 40 || math.Abs(extSer.Get(0)-0.004) > 1e-12 {
		t.Fatalf("custom float extension series lost or misaligned: %+v", df.Series("funding_ext"))
	}
	if df.String("regime_label") != "trend" {
		t.Fatalf("custom string extension lost: %q", df.String("regime_label"))
	}
	if raw, ok := df.RawValue("explicit_nil"); !ok || raw != nil || !df.Has("explicit_nil") {
		t.Fatalf("explicit NULL semantics lost: raw=%v ok=%v has=%v", raw, ok, df.Has("explicit_nil"))
	}
	if raw, ok := df.RawValue("stale_col"); ok || raw != nil || df.Has("stale_col") {
		t.Fatalf("missing field inherited stale value: raw=%v ok=%v", raw, ok)
	}
	if raw, ok := df.RawValue("trade_num"); !ok || raw != int64(1040) {
		t.Fatalf("trade_num raw int64 type lost: %T(%v) ok=%v", raw, raw, ok)
	}
}

func TestRefKlineSubNormalizesAndEnsuresReadOnlySymbol(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	eth := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "ETH/USDT:USDT"}
	if err := symbols.SetExSymbols([]*orm.ExSymbol{eth}); err != nil {
		t.Fatal(err)
	}
	ex := newRefTestExchange(banexg.MarketLinear, "ETH/USDT:USDT", "BTC/USDT:USDT")
	state := NewState()
	state.Symbols = symbols
	state.Exchange = ex

	job := &StratJob{
		Strat:           &TradeStrat{WarmupNum: 120},
		Symbol:          eth,
		TimeFrame:       "15m",
		symbols:         symbols,
		strategyState:   state,
		runtimeExchange: ex,
	}
	job.DataHub = newJobDataHub(job)

	// 1. Short base symbol "BTC" normalizes to "BTC/USDT:USDT" and is ensured in SymbolState.
	btcSub := job.RefKlineSub("BTC", "1d")
	if btcSub == nil || btcSub.ExSymbol == nil || btcSub.ExSymbol.ID <= 0 || btcSub.ExSymbol.Symbol != "BTC/USDT:USDT" {
		t.Fatalf("RefKlineSub(\"BTC\", \"1d\") = %+v, exs=%+v", btcSub, btcSub.ExSymbol)
	}
	if btcSub.WarmupNum != 120 {
		t.Fatalf("RefKlineSub default WarmupNum = %d, want 120", btcSub.WarmupNum)
	}

	// 2. Empty symbol targets current job symbol's auxiliary timeframe.
	auxSub := NewRefKlineSub(job, "", "4h", 50)
	if auxSub == nil || auxSub.ExSymbol == nil || auxSub.ExSymbol.ID != eth.ID || auxSub.WarmupNum != 50 {
		t.Fatalf("NewRefKlineSub(job, \"\", \"4h\", 50) = %+v", auxSub)
	}

	// 3. Before bars arrive, RefEnv returns nil safely.
	if got := job.RefEnv("BTC", "1d"); got != nil {
		t.Fatalf("expected nil RefEnv before bars arrive, got %+v", got)
	}

	// 4. After feeding BTC 1d kline into job, RefEnv works with "BTC", "BTC/USDT", and "BTC/USDT:USDT".
	btcExs := btcSub.ExSymbol
	job.SetData(&orm.DataSeries{
		Source:    orm.SeriesSourceKline,
		Sid:       btcExs.ID,
		ExSymbol:  btcExs,
		TimeFrame: "1d",
		TimeMS:    86400_000,
		EndMS:     2 * 86400_000,
		Closed:    true,
		Values: map[string]any{
			"open": 60000.0, "high": 62000.0, "low": 59500.0, "close": 61500.0, "volume": 1200.0,
		},
	})
	for _, query := range []string{"BTC", "BTC/USDT", "BTC/USDT:USDT"} {
		env := job.RefEnv(query, "1d")
		if env == nil || env.Close == nil || env.Close.Get(0) != 61500.0 || env.High.Get(0) != 62000.0 {
			t.Fatalf("RefEnv(%q, \"1d\") = %+v", query, env)
		}
	}
}
