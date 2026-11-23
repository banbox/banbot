package biz

// Backtest engine baseline.
//
// One hermetic, deterministic time-series backtest driven through the full
// Trader -> strategy -> order manager -> wallet path on synthetic bars. It is
// the regression reference for any change to backtest matching or accounting:
//
//   - TestBacktestBaselineGolden compares every order and the equity curve
//     with testdata/bt_baseline (BANBOT_UPDATE_BASELINE=1 rewrites; the order
//     dump is gzipped text).
//   - BenchmarkBacktestBaseline measures the same workload.
//
// The file uses only exported biz/strat API so that it keeps compiling while
// internals move.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
	"github.com/sasha-s/go-deadlock"
)

const (
	blTF       = "15m"
	blTFMS     = int64(15 * 60 * 1000)
	blStartMS  = int64(1_704_067_200_000) // 2024-01-01 00:00:00 UTC
	blWarmBars = 40
	blBars     = 4000
)

var blSymbols = []string{
	"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
	"XRP/USDT:USDT", "DOGE/USDT:USDT", "LINK/USDT:USDT",
}

var blStrategies = []string{"bl:ma_cross", "bl:pullback", "bl:breakout", "bl:scalp"}

type blBar struct {
	sym        int
	ms         int64
	o, h, l, c float64
	warm       bool
}

// blRand is a fixed splitmix64 generator: the workload must not depend on the
// Go release's math/rand stream.
type blRand struct{ s uint64 }

func (r *blRand) next() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// unit returns a value in [-1, 1).
func (r *blRand) unit() float64 { return float64(r.next()>>11)/float64(1<<52) - 1 }

func blRound(v float64) float64 { return math.Round(v*10000) / 10000 }

// blGenBars builds the time-ordered bar stream. Symbol 4 stops trading at 60%
// of the range (delist) and symbol 5 has a three-day hole (data gap).
func blGenBars() []blBar {
	bases := []float64{42000, 2300, 100, 0.6, 0.08, 15}
	perSym := make([][]blBar, len(blSymbols))
	for si := range blSymbols {
		rng := &blRand{s: uint64(1000 + si*7919)}
		price := bases[si]
		drift := 0.0
		total := blWarmBars + blBars
		bars := make([]blBar, 0, total)
		for i := 0; i < total; i++ {
			idx := i - blWarmBars
			if si == 4 && idx >= blBars*6/10 {
				break
			}
			if si == 5 && idx >= blBars/2 && idx < blBars/2+288 {
				continue
			}
			if i%240 == 0 {
				drift = rng.unit() * 0.0012
			}
			vol := 0.004 + 0.003*math.Abs(rng.unit())
			o := price
			p1 := o * (1 + drift + vol*rng.unit())
			p2 := p1 * (1 + drift + vol*rng.unit())
			c := p2 * (1 + vol*0.5*rng.unit())
			h := math.Max(math.Max(o, p1), math.Max(p2, c)) * (1 + 0.0005*math.Abs(rng.unit()))
			l := math.Min(math.Min(o, p1), math.Min(p2, c)) * (1 - 0.0005*math.Abs(rng.unit()))
			o, h, l, c = blRound(o), blRound(h), blRound(l), blRound(c)
			if l <= 0 {
				l = 0.0001
			}
			bars = append(bars, blBar{sym: si, ms: blStartMS + int64(idx)*blTFMS, o: o, h: h, l: l, c: c, warm: idx < 0})
			price = c
		}
		perSym[si] = bars
	}
	var all []blBar
	for _, bars := range perSym {
		all = append(all, bars...)
	}
	sort.SliceStable(all, func(a, b int) bool {
		if all[a].ms != all[b].ms {
			return all[a].ms < all[b].ms
		}
		return all[a].sym < all[b].sym
	})
	return all
}

type blExchange struct {
	banexg.BanExchange
	markets map[string]*banexg.Market
}

func newBLExchange() *blExchange {
	markets := make(map[string]*banexg.Market, len(blSymbols))
	for _, sym := range blSymbols {
		base, quote, settle, _ := core.SplitSymbol(sym)
		markets[sym] = &banexg.Market{
			ID: sym, Symbol: sym, Base: base, Quote: quote, Settle: settle,
			Type: banexg.MarketLinear, Contract: true, Linear: true, Swap: true,
			Active: true, ContractSize: 1,
			Precision: &banexg.Precision{
				Amount: 0.0001, Price: 0.0001,
				ModeAmount: banexg.PrecModeTickSize, ModePrice: banexg.PrecModeTickSize,
			},
			Limits: &banexg.MarketLimits{
				Amount: &banexg.LimitRange{Min: 0.0001, Max: 100_000_000},
				Cost:   &banexg.LimitRange{Min: 1, Max: 100_000_000},
			},
		}
	}
	base := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear, Markets: markets}}
	return &blExchange{BanExchange: base, markets: markets}
}

func (e *blExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear, Markets: e.markets}
}

func (e *blExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if m := e.markets[symbol]; m != nil {
		return m, nil
	}
	return nil, errs.NewMsg(core.ErrInvalidSymbol, "unknown symbol %s", symbol)
}

func (e *blExchange) PrecPrice(_ *banexg.Market, p float64) (float64, *errs.Error) {
	return blRound(p), nil
}

func (e *blExchange) PrecAmount(_ *banexg.Market, a float64) (float64, *errs.Error) {
	return math.Floor(a*10000+1e-9) / 10000, nil
}

func (e *blExchange) PrecCost(_ *banexg.Market, c float64) (float64, *errs.Error) {
	return blRound(c), nil
}

func (e *blExchange) CalcMaintMargin(_ string, cost float64) (float64, *errs.Error) {
	return cost * 0.005, nil
}

func (e *blExchange) GetLeverage(string, float64, string) (float64, float64) { return 5, 20 }

func (e *blExchange) PriceSymbolParts(symbol string) ([4]string, *errs.Error) {
	base, quote, settle, id := core.SplitSymbol(symbol)
	if settle == "" {
		settle = quote
	}
	return [4]string{base, quote, settle, id}, nil
}

// CalculateFee charges 5 bp on takers and 2 bp on makers in the quote currency.
func (e *blExchange) CalculateFee(_ string, _ string, _ string, amount, price float64, isMaker bool, _ map[string]interface{}) (*banexg.Fee, *errs.Error) {
	rate := 0.0005
	if isMaker {
		rate = 0.0002
	}
	cost := amount * price * rate
	return &banexg.Fee{Currency: "USDT", Cost: cost, QuoteCost: cost, Rate: rate, IsMaker: isMaker}, nil
}

var blRegisterOnce sync.Once

func blRegisterStrategies() {
	blRegisterOnce.Do(func() {
		// Market entries with fixed stop-loss/take-profit, closed on the reverse cross.
		strat.RegisterStrategy("bl:ma_cross", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
			return &strat.TradeStrat{
				WarmupNum:     blWarmBars,
				RunTimeFrames: []string{blTF},
				OnBar: func(s *strat.StratJob) {
					e := s.Env
					fast, slow := ta.SMA(e.Close, 7), ta.SMA(e.Close, 25)
					c := e.Close.Get(0)
					up := fast.Get(0) > slow.Get(0) && fast.Get(1) <= slow.Get(1)
					down := fast.Get(0) < slow.Get(0) && fast.Get(1) >= slow.Get(1)
					if up {
						_ = s.CloseOrders(&strat.ExitReq{Tag: "cross_dn", Dirt: core.OdDirtShort})
						_ = s.OpenOrder(&strat.EnterReq{Tag: "cross_up", StopLossVal: c * 0.02, TakeProfitVal: c * 0.035})
					} else if down {
						_ = s.CloseOrders(&strat.ExitReq{Tag: "cross_up", Dirt: core.OdDirtLong})
						_ = s.OpenOrder(&strat.EnterReq{Tag: "cross_dn", Short: true, StopLossVal: c * 0.02, TakeProfitVal: c * 0.035})
					}
				},
			}
		})
		// Limit entries that expire, with a two-rung take-profit ladder and breakeven stop.
		strat.RegisterStrategy("bl:pullback", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
			return &strat.TradeStrat{
				WarmupNum:     blWarmBars,
				RunTimeFrames: []string{blTF},
				EachMaxLong:   2,
				EachMaxShort:  2,
				OnBar: func(s *strat.StratJob) {
					e := s.Env
					if e.BarNum%6 != 0 {
						return
					}
					c := e.Close.Get(0)
					ma := ta.SMA(e.Close, 20).Get(0)
					short := c < ma
					limit, sl := c*0.994, c*0.975
					if short {
						limit, sl = c*1.006, c*1.025
					}
					_ = s.OpenOrder(&strat.EnterReq{
						Tag: "pull", Short: short, Limit: limit, StopBars: 4, StopLoss: sl,
						TakeProfits: []strat.TakeProfitTarget{
							{Rate: 0.012, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
							{Rate: 0.03, ExitRate: 0.5, Tag: "tp2"},
						},
					})
				},
			}
		})
		// OCO stop entries on both sides of the recent range with a trailing stop.
		strat.RegisterStrategy("bl:breakout", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
			return &strat.TradeStrat{
				WarmupNum:     blWarmBars,
				RunTimeFrames: []string{blTF},
				EachMaxLong:   1,
				EachMaxShort:  1,
				OnBar: func(s *strat.StratJob) {
					e := s.Env
					if e.BarNum%10 != 0 || s.GetOrderNum(0) > 0 {
						return
					}
					hi := ta.Highest(e.High, 20).Get(0)
					lo := ta.Lowest(e.Low, 20).Get(0)
					group := "brk" + strconv.Itoa(e.BarNum)
					_ = s.OpenOrder(&strat.EnterReq{
						Tag: "brk_up", Stop: hi * 1.001, OCOGroup: group, StopBars: 8,
						StopLoss: hi * 0.98, ActivationPrice: hi * 1.012, CallbackPct: 0.8,
					})
					_ = s.OpenOrder(&strat.EnterReq{
						Tag: "brk_dn", Short: true, Stop: lo * 0.999, OCOGroup: group, StopBars: 8,
						StopLoss: lo * 1.02, ActivationPrice: lo * 0.988, CallbackPct: 0.8,
					})
				},
			}
		})
		// Order-change driven limit exits plus timed partial and custom exits.
		strat.RegisterStrategy("bl:scalp", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
			return &strat.TradeStrat{
				WarmupNum:     blWarmBars,
				RunTimeFrames: []string{blTF},
				EachMaxLong:   3,
				OnBar: func(s *strat.StratJob) {
					e := s.Env
					if e.BarNum%4 == 0 && e.Close.Get(0) < e.Close.Get(3) {
						_ = s.OpenOrder(&strat.EnterReq{Tag: "dip", LegalCost: 150})
					}
					if e.BarNum%16 == 8 {
						_ = s.CloseOrders(&strat.ExitReq{Tag: "half", Dirt: core.OdDirtLong, ExitRate: 0.5, FilledOnly: true})
					}
				},
				OnOrderChange: func(s *strat.StratJob, od *ormo.InOutOrder, chgType int) {
					if chgType == strat.OdChgEnterFill && od.EnterTag == "dip" {
						_ = s.CloseOrders(&strat.ExitReq{
							Tag: "scalp_tp", OrderID: od.ID, Dirt: core.OdDirtLong, Limit: od.Enter.Average * 1.008,
						})
					}
				},
				OnCheckExit: func(s *strat.StratJob, od *ormo.InOutOrder) *strat.ExitReq {
					if od.Status == ormo.InOutStatusFullEnter && s.Env.TimeStop-od.EnterAt > 40*blTFMS {
						return &strat.ExitReq{Tag: "timeout", OrderID: od.ID}
					}
					return nil
				},
			}
		})
	})
}

type blResult struct {
	orders string
	equity string
}

// blRun executes the workload. dump=false skips building the text result so the
// benchmark measures the engine only.
func blRun(tb testing.TB, bars []blBar, dump bool) blResult {
	tb.Helper()
	blRegisterStrategies()
	coreState, err := core.NewState(nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer coreState.Close()
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.BackTestMode = true
	coreState.EnvReal = false
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true
	coreState.ExgName = "binance"
	coreState.SetPairs(blSymbols, nil)

	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(bars[0].ms)

	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
	exsList := make([]*orm.ExSymbol, len(blSymbols))
	for i, sym := range blSymbols {
		exsList[i] = &orm.ExSymbol{ID: int32(i + 1), Exchange: "binance", Market: banexg.MarketLinear, Symbol: sym}
		if setErr := symbols.CacheExSymbolChecked(exsList[i]); setErr != nil {
			tb.Fatal(setErr)
		}
	}

	policies := make([]*config.RunPolicyConfig, len(blStrategies))
	for i, name := range blStrategies {
		policies[i] = &config.RunPolicyConfig{Name: name}
	}
	account := &config.AccountConfig{Leverage: 5, MaxOpenOrders: 60}
	cfgData := &config.Config{
		Name:               "bt-baseline",
		MarketType:         banexg.MarketLinear,
		StakeAmount:        200,
		Leverage:           5,
		OpenVolRate:        1,
		BTNetCost:          15,
		BTSlippageBps:      2,
		BTFundingApr:       10,
		BTGapBars:          20,
		BTGapMinHours:      48,
		BTDelistPenaltyBps: 30,
		MaxOpenOrders:      60,
		MaxSimulOpen:       60,
		WalletAmounts:      map[string]float64{"USDT": 20_000},
		Accounts:           map[string]*config.AccountConfig{"default": account},
		RunTimeframes:      []string{blTF},
		Pairs:              blSymbols,
		RunPolicy:          policies,
	}
	cfg := config.NewSnapshotWithDirs(cfgData, tb.TempDir(), "")
	exchange := newBLExchange()
	deps := RuntimeDeps{
		Core:           coreState,
		Clock:          clock,
		Market:         com.NewMarketStateWithExchange("binance", exchange),
		Batch:          strat.NewBatchState(),
		Strategies:     strat.NewState(),
		Orders:         ormo.NewOrderState(),
		Trading:        NewTradingState(),
		Config:         cfg,
		Accounts:       map[string]*config.AccountConfig{"default": account},
		AccountsMu:     &sync.RWMutex{},
		Symbols:        symbols,
		Exchange:       exchange,
		DefaultAccount: "default",
	}
	if bindErr := BindRuntimeDeps(deps); bindErr != nil {
		tb.Fatal(bindErr)
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	wallets := InitFakeWalletsWithRuntimeDeps(deps)
	InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)

	tfScores := make(map[string]map[string]float64, len(blSymbols))
	for _, sym := range blSymbols {
		tfScores[sym] = map[string]float64{blTF: 1}
	}
	if _, _, loadErr := strat.LoadStratJobsWithState(deps.Strategies, coreState, symbols, blSymbols, tfScores, deps.Orders); loadErr != nil {
		tb.Fatalf("load strategy jobs: %v", loadErr)
	}
	InitOdSubsWithRuntimeDeps(&deps)
	trader, trErr := NewTraderWithRuntimeDeps(deps)
	if trErr != nil {
		tb.Fatal(trErr)
	}

	openList := func() []*ormo.InOutOrder {
		openMap, lock := deps.Orders.GetOpenODs("default")
		lock.Lock()
		defer lock.Unlock()
		list := make([]*ormo.InOutOrder, 0, len(openMap))
		for _, od := range openMap {
			list = append(list, od)
		}
		return list
	}

	var equity strings.Builder
	lastMS := int64(0)
	steps := 0
	for i := range bars {
		bar := &bars[i]
		if dump && bar.ms != lastMS && lastMS != 0 && !bar.warm {
			steps++
			if steps%200 == 0 {
				fmt.Fprintf(&equity, "%d equity=%s open=%d\n", lastMS+blTFMS, blNum(wallets.EquityWithOrders(openList())), len(openList()))
			}
		}
		lastMS = bar.ms
		endMS := bar.ms + blTFMS
		trader.SetTimeMS(endMS)
		evt := orm.NewDataSeriesFromKline(exsList[bar.sym], blTF, &banexg.Kline{
			Time: bar.ms, Open: bar.o, High: bar.h, Low: bar.l, Close: bar.c, Volume: 100_000, Quote: bar.c * 100_000,
		}, nil, true, true)
		evt.EndMS = endMS
		evt.IsWarmUp = bar.warm
		if feedErr := trader.FeedDataSeries(evt); feedErr != nil {
			tb.Fatalf("feed %s@%d: %v", blSymbols[bar.sym], bar.ms, feedErr)
		}
	}
	if cleanErr := CleanUpOdMgrWithState(deps.Trading); cleanErr != nil {
		tb.Fatalf("cleanup: %v", cleanErr)
	}
	if !dump {
		return blResult{}
	}
	fmt.Fprintf(&equity, "final equity=%s total=%s available=%s open=%d\n",
		blNum(wallets.EquityWithOrders(openList())), blNum(wallets.TotalLegal(nil, true)), blNum(wallets.AvaLegal(nil)), len(openList()))

	orders := append(deps.Orders.HistoricalOrders(), openList()...)
	sort.SliceStable(orders, func(a, b int) bool { return orders[a].ID < orders[b].ID })
	var out strings.Builder
	for _, od := range orders {
		blDumpOrder(&out, od)
	}
	return blResult{orders: out.String(), equity: equity.String()}
}

// blNum prints 10 significant digits; blSameLine applies the tolerance.
func blNum(v float64) string { return strconv.FormatFloat(v, 'g', 10, 64) }

func blDumpEx(out *strings.Builder, name string, ex *ormo.ExOrder) {
	if ex == nil {
		fmt.Fprintf(out, " %s=nil", name)
		return
	}
	fmt.Fprintf(out, " %s={%s %s create=%d update=%d price=%s avg=%s amt=%s filled=%s status=%d fee=%s feeq=%s}",
		name, ex.OrderType, ex.Side, ex.CreateAt, ex.UpdateAt, blNum(ex.Price), blNum(ex.Average),
		blNum(ex.Amount), blNum(ex.Filled), ex.Status, blNum(ex.Fee), blNum(ex.FeeQuote))
}

func blDumpOrder(out *strings.Builder, od *ormo.InOutOrder) {
	fmt.Fprintf(out, "#%d %s %s %s short=%t status=%d tag=%s exit=%s lev=%s enterAt=%d exitAt=%d init=%s cost=%s profit=%s rate=%s",
		od.ID, od.Symbol, od.Timeframe, od.Strategy, od.Short, od.Status, od.EnterTag, od.ExitTag, blNum(od.Leverage),
		od.EnterAt, od.ExitAt, blNum(od.InitPrice), blNum(od.QuoteCost), blNum(od.Profit), blNum(od.ProfitRate))
	blDumpEx(out, "enter", od.Enter)
	blDumpEx(out, "exit", od.Exit)
	keys := make([]string, 0, len(od.Info))
	for k := range od.Info {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out.WriteString(" info={")
	for i, k := range keys {
		if i > 0 {
			out.WriteByte(' ')
		}
		val := od.Info[k]
		if f, ok := val.(float64); ok {
			fmt.Fprintf(out, "%s=%s", k, blNum(f))
			continue
		}
		raw, _ := json.Marshal(val)
		fmt.Fprintf(out, "%s=%s", k, raw)
	}
	out.WriteString("}\n")
}

// blSameLine reports whether two dump lines agree. Numeric fields may differ by
// a relative 1e-7: the engine sums floats in map order, so the last bits are
// not reproducible between runs, while any behavioural change moves a price,
// amount, fee or timestamp by far more.
func blSameLine(got, want string) bool {
	if got == want {
		return true
	}
	gf, wf := blFields(got), blFields(want)
	if len(gf) != len(wf) {
		return false
	}
	for i := range gf {
		if gf[i] == wf[i] {
			continue
		}
		g, gErr := strconv.ParseFloat(gf[i], 64)
		w, wErr := strconv.ParseFloat(wf[i], 64)
		if gErr != nil || wErr != nil {
			return false
		}
		if math.Abs(g-w) > 1e-7*math.Max(math.Abs(g), math.Abs(w))+1e-9 {
			return false
		}
	}
	return true
}

func blFields(line string) []string {
	return strings.FieldsFunc(line, func(r rune) bool {
		return r == ' ' || r == '=' || r == '{' || r == '}' || r == ',' || r == ':' || r == '"'
	})
}

// blDiff returns the number of differing lines and reports the first few.
func blDiff(t *testing.T, name, got, want string) int {
	t.Helper()
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(want, "\n")
	diffs := 0
	for i := 0; i < len(gotLines) || i < len(wantLines); i++ {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if !blSameLine(g, w) {
			diffs++
			if diffs <= 5 {
				t.Errorf("%s line %d differs\n want: %s\n  got: %s", name, i+1, w, g)
			}
		}
	}
	return diffs
}

func blCompareGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "bt_baseline", name)
	if os.Getenv("BANBOT_UPDATE_BASELINE") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		data := []byte(got)
		if strings.HasSuffix(name, ".gz") {
			var buf bytes.Buffer
			zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			_, _ = zw.Write(data)
			_ = zw.Close()
			data = buf.Bytes()
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (set BANBOT_UPDATE_BASELINE=1 to create): %v", err)
	}
	if strings.HasSuffix(name, ".gz") {
		zr, zErr := gzip.NewReader(bytes.NewReader(want))
		if zErr != nil {
			t.Fatal(zErr)
		}
		if want, err = io.ReadAll(zr); err != nil {
			t.Fatal(err)
		}
	}
	if diffs := blDiff(t, name, got, string(want)); diffs > 0 {
		t.Fatalf("%s: %d lines differ from the baseline", name, diffs)
	}
}

func TestBacktestBaselineGolden(t *testing.T) {
	bars := blGenBars()
	first := blRun(t, bars, true)
	second := blRun(t, bars, true)
	if blDiff(t, "rerun orders", second.orders, first.orders)+blDiff(t, "rerun equity", second.equity, first.equity) > 0 {
		t.Fatal("baseline workload is not deterministic across two runs in one process")
	}
	orders := strings.Count(first.orders, "\n")
	if orders < 500 {
		t.Fatalf("baseline produced only %d orders; the workload no longer exercises the engine", orders)
	}
	blCompareGolden(t, "summary.golden", fmt.Sprintf("orders=%d\n%s", orders, first.equity))
	blCompareGolden(t, "orders.golden.gz", first.orders)
}

func BenchmarkBacktestBaseline(b *testing.B) {
	// Production entry points disable lock-order tracking (entry/main.go).
	previous := deadlock.Opts.Disable
	deadlock.Opts.Disable = true
	defer func() { deadlock.Opts.Disable = previous }()
	bars := blGenBars()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blRun(b, bars, false)
	}
	b.ReportMetric(float64(len(bars)), "bars/op")
}
