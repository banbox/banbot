package opt

import (
	"bytes"
	"cmp"
	"context"
	_ "embed"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/com"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"gonum.org/v1/gonum/floats"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/olekukonko/tablewriter"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type BTResult struct {
	MaxOpenOrders   int                    `json:"maxOpenOrders"`
	MinReal         float64                `json:"minReal"`
	MaxReal         float64                `json:"maxReal"`         // Maximum Assets 最大资产
	MaxDrawDownPct  float64                `json:"maxDrawDownPct"`  // Maximum drawdown percentage 最大回撤百分比
	ShowDrawDownPct float64                `json:"showDrawDownPct"` // Displays the maximum drawdown percentage 显示最大回撤百分比
	MaxDrawDownVal  float64                `json:"maxDrawDownVal"`  // Maximum drawdown percentage 最大回撤金额
	ShowDrawDownVal float64                `json:"showDrawDownVal"` // Displays the maximum drawdown percentage 显示最大回撤金额
	MaxFundOccup    float64                `json:"maxFundOccup"`
	MaxOccupForPair float64                `json:"maxOccupForPair"`
	BarNum          int                    `json:"barNum"`
	TimeNum         int                    `json:"timeNum"`
	OrderNum        int                    `json:"orderNum"`
	lastTime        int64                  // 上次bar的时间戳
	lastPlotMS      int64                  // 上次资金曲线采样时间
	histOdOff       int                    // 计算已完成订单利润的偏移
	donePftLegal    float64                // 已完成订单利润
	Plots           *PlotData              `json:"plots"`
	EntLabels       []string               `json:"entLabels"`
	EntDatasets     []*ChartDs             `json:"entDatasets"`
	CreateMS        int64                  `json:"createMS"`
	StartMS         int64                  `json:"startMS"`
	EndMS           int64                  `json:"endMS"`
	PlotEvery       int                    `json:"plotEvery"`
	TotalInvest     float64                `json:"totalInvest"`
	OutDir          string                 `json:"outDir"`
	PairGrps        []*RowItem             `json:"pairGrps"`
	DateGrps        []*RowItem             `json:"dateGrps"`
	EnterGrps       []*RowItem             `json:"enterGrps"`
	ExitGrps        []*RowItem             `json:"exitGrps"`
	ProfitGrps      []*RowItem             `json:"profitGrps"`
	DrawDowns       []*core.TimeValueRange `json:"drawDowns"`
	TmpDrawDown     *core.TimeValueRange   `json:"-"`
	TotProfit       float64                `json:"totProfit"`
	TotCost         float64                `json:"totCost"`
	TotFee          float64                `json:"totFee"`
	TotProfitPct    float64                `json:"totProfitPct"`
	TfHits          map[string]int         `json:"tfHits"`
	WinRatePct      float64                `json:"winRatePct"`
	FinBalance      float64                `json:"finBalance"`
	FinWithdraw     float64                `json:"finWithdraw"`
	SharpeRatio     float64                `json:"sharpeRatio"`
	SortinoRatio    float64                `json:"sortinoRatio"`
	CalcDiff        float64                `json:"calcDiff"`
	Stability       float64                `json:"stability"`
	HitSlTp         int                    `json:"hitSlTp"`
	runtimeDeps     *biz.RuntimeDeps
	reportDeps      *ReportDeps
}

// ReportDeps owns the state needed by report/replay helpers. It is deliberately
// narrower than Runtime: report code binds only the config, market, wallet,
// order, symbol, and storage owners it actually consumes.
type ReportDeps struct {
	Config         *config.Snapshot
	Core           *core.State
	Clock          *btime.ClockState
	Market         *com.MarketState
	Symbols        *orm.SymbolState
	Storage        *orm.Storage
	Strategies     *strat.State
	Orders         *ormo.OrderState
	Trading        *biz.TradingState
	Exchange       banexg.BanExchange
	DefaultAccount string
}

func NewReportDeps(deps biz.RuntimeDeps) *ReportDeps {
	return &ReportDeps{
		Config:         deps.Config,
		Core:           deps.Core,
		Clock:          deps.Clock,
		Market:         deps.Market,
		Symbols:        deps.Symbols,
		Storage:        deps.Storage,
		Strategies:     deps.Strategies,
		Orders:         deps.Orders,
		Trading:        deps.Trading,
		Exchange:       deps.Exchange,
		DefaultAccount: deps.DefaultAccount,
	}
}

func reportDepsFromRuntime(deps *biz.RuntimeDeps) *ReportDeps {
	if deps == nil {
		return nil
	}
	return NewReportDeps(*deps)
}

func (d *ReportDeps) validateSeries() *errs.Error {
	if d == nil {
		return errs.NewMsg(core.ErrBadConfig, "report dependencies are required")
	}
	missing := make([]string, 0, 2)
	if d.Symbols == nil {
		missing = append(missing, "symbols")
	}
	if d.Storage == nil {
		missing = append(missing, "storage")
	}
	if len(missing) > 0 {
		return errs.NewMsg(core.ErrBadConfig, "runtime report requires %s state", strings.Join(missing, " and "))
	}
	// A report's symbol catalog and query owner must be the same concrete
	// binding. An unbound symbol state is not safe merely because a storage
	// owner was supplied: resolving from one catalog and querying another can
	// silently produce a mixed report.
	if d.Symbols.Storage() != d.Storage {
		return errs.NewMsg(core.ErrBadConfig, "runtime report symbols and storage must share an owner")
	}
	return nil
}

func (d *ReportDeps) validateResult() *errs.Error {
	if d == nil {
		return errs.NewMsg(core.ErrBadConfig, "report dependencies are required")
	}
	missing := make([]string, 0, 6)
	if d.Config == nil || d.Config.View() == nil {
		missing = append(missing, "config")
	}
	if d.Trading == nil {
		missing = append(missing, "trading")
	}
	if d.Orders == nil {
		missing = append(missing, "orders")
	}
	if d.Market == nil || d.Market.Prices == nil {
		missing = append(missing, "market prices")
	}
	if d.Symbols == nil {
		missing = append(missing, "symbols")
	}
	if d.Storage == nil {
		missing = append(missing, "storage")
	}
	if len(missing) > 0 {
		return errs.NewMsg(core.ErrBadConfig, "runtime report requires %s state", strings.Join(missing, ", "))
	}
	if d.Symbols.Storage() != d.Storage {
		return errs.NewMsg(core.ErrBadConfig, "runtime report symbols and storage must share an owner")
	}
	return nil
}

func (d *ReportDeps) dateStr(timestamp int64, format string) string {
	if d == nil {
		return btime.ToDateStr(timestamp, format)
	}
	if format == "" {
		format = core.DefaultDateFmt
	}
	location := time.UTC
	if d.Config != nil {
		location = d.Config.Location()
	}
	return btime.MSToTime(timestamp).In(location).Format(format)
}

func (d *ReportDeps) dateStrLoc(timestamp int64, format string) string {
	if d == nil {
		return btime.ToDateStrLoc(timestamp, format)
	}
	return d.dateStr(timestamp, format)
}

func (d *ReportDeps) configView() *config.Config {
	if d == nil || d.Config == nil {
		return nil
	}
	return d.Config.View()
}

func (d *ReportDeps) account() string {
	if d == nil {
		return ""
	}
	if d.DefaultAccount != "" {
		return d.DefaultAccount
	}
	if d.Config != nil {
		if account := d.Config.DefaultAccount(); account != "" {
			return account
		}
	}
	return "default"
}

func (d *ReportDeps) strictBacktest() bool {
	if d == nil || d.Core == nil {
		return false
	}
	cfg := d.configView()
	return d.Core.BackTestMode && cfg != nil && cfg.BTStrict
}

func (d *ReportDeps) wallet() *biz.BanWallets {
	if d == nil || d.Trading == nil {
		return nil
	}
	return d.Trading.Wallet(d.account())
}

func (d *ReportDeps) context() context.Context {
	if d != nil && d.Core != nil {
		if ctx := d.Core.Context(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

func (d *ReportDeps) exchangeMarket() (string, string) {
	if d == nil {
		return "", ""
	}
	if d.Core != nil && d.Core.ExgName != "" && d.Core.Market != "" {
		return d.Core.ExgName, d.Core.Market
	}
	if d.Exchange != nil {
		if info := d.Exchange.Info(); info != nil {
			return info.ID, info.MarketType
		}
	}
	return "", ""
}

func (d *ReportDeps) symbol(pair string) (*orm.ExSymbol, *errs.Error) {
	if d == nil || d.Symbols == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "report symbol state is required")
	}
	exchange, market := d.exchangeMarket()
	if exchange == "" || market == "" {
		return nil, errs.NewMsg(core.ErrBadConfig, "report exchange and market are required")
	}
	exs := d.Symbols.GetExSymbol2(exchange, market, pair)
	if exs == nil {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not found in report symbol state", pair)
	}
	return exs, nil
}

func (d *ReportDeps) queries() (*orm.Queries, func(), *errs.Error) {
	if d == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "report dependencies are required")
	}
	if d.Storage != nil {
		queries, conn, err := d.Storage.Conn(d.context())
		if err != nil {
			return nil, nil, err
		}
		if d.Symbols != nil {
			queries = queries.WithSeriesSymbolState(d.Symbols)
		}
		return queries, func() {
			if conn != nil {
				conn.Release()
			}
		}, nil
	}
	return nil, nil, errs.NewMsg(core.ErrBadConfig, "runtime report storage is required")
}

func (d *ReportDeps) bizRuntimeDeps() biz.RuntimeDeps {
	if d == nil {
		return biz.RuntimeDeps{}
	}
	return biz.RuntimeDeps{
		Core:           d.Core,
		Clock:          d.Clock,
		Market:         d.Market,
		Config:         d.Config,
		Symbols:        d.Symbols,
		Storage:        d.Storage,
		Strategies:     d.Strategies,
		Orders:         d.Orders,
		Trading:        d.Trading,
		Exchange:       d.Exchange,
		DefaultAccount: d.DefaultAccount,
	}
}

type PlotData struct {
	Labels        []string   `json:"labels"`
	OdNum         []int      `json:"odNum"`
	JobNum        []int      `json:"jobNum"`
	Real          []float64  `json:"real"`
	Available     []float64  `json:"available"`
	Profit        []float64  `json:"profit"`
	UnrealizedPOL []float64  `json:"unrealizedPOL"`
	WithDraw      []float64  `json:"withDraw"`
	More          []*ChartDs `json:"more"`
	tmpOdNum      int
}

type RowPart struct {
	WinCount     int                `json:"winCount"`
	OrderNum     int                `json:"orderNum"`
	ProfitSum    float64            `json:"profitSum"`
	ProfitPctSum float64            `json:"profitPctSum"`
	CostSum      float64            `json:"costSum"`
	Durations    []int              `json:"-"`
	Orders       []*ormo.InOutOrder `json:"-"`
	Sharpe       float64            `json:"sharpe"` // 夏普比率
	Sortino      float64            `json:"sortino"`
}

type RowItem struct {
	Title string `json:"title"`
	RowPart
}

var (
	PairPickers = make(map[string]func(r *BTResult) []string)
)

func NewBTResult() *BTResult {
	res := &BTResult{
		Plots:     &PlotData{},
		PlotEvery: 1,
		CreateMS:  btime.UTCStamp(),
	}
	return res
}

func (r *BTResult) reportRuntimeDeps() *ReportDeps {
	if r == nil {
		return nil
	}
	if r.reportDeps != nil {
		return r.reportDeps
	}
	return reportDepsFromRuntime(r.runtimeDeps)
}

func (r *BTResult) dateStr(timestamp int64, format string) string {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.dateStr(timestamp, format)
	}
	return btime.ToDateStr(timestamp, format)
}

func (r *BTResult) dateStrLoc(timestamp int64, format string) string {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.dateStrLoc(timestamp, format)
	}
	return btime.ToDateStrLoc(timestamp, format)
}

func (r *BTResult) runtimeConfig() *config.Config {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.configView()
	}
	return &config.Data
}

func (r *BTResult) runtimeCore() *core.State {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.Core
	}
	return nil
}

func (r *BTResult) dataDir() string {
	if deps := r.reportRuntimeDeps(); deps != nil {
		if deps.Config != nil {
			return deps.Config.DataDir
		}
		return ""
	}
	return config.GetDataDir()
}

func (r *BTResult) strategyDir() string {
	if deps := r.reportRuntimeDeps(); deps != nil {
		if deps.Config != nil {
			return deps.Config.StrategyDir
		}
		return ""
	}
	return config.GetStratDir()
}

func (r *BTResult) strategyState() *strat.State {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.Strategies
	}
	return nil
}

func (r *BTResult) orderState() *ormo.OrderState {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.Orders
	}
	return nil
}

func (r *BTResult) historyOrders() []*ormo.InOutOrder {
	if deps := r.reportRuntimeDeps(); deps != nil {
		if deps.Orders == nil {
			return nil
		}
		return deps.Orders.HistoricalOrders()
	}
	return ormo.HistODs
}

func (r *BTResult) reportAccount() string {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.account()
	}
	return config.DefAcc
}

func (r *BTResult) reportWallet() *biz.BanWallets {
	if deps := r.reportRuntimeDeps(); deps != nil {
		return deps.wallet()
	}
	return biz.GetWallets(r.reportAccount())
}

func (r *BTResult) strategyJobs(account string) map[string]map[string]*strat.StratJob {
	if deps := r.reportRuntimeDeps(); deps != nil {
		if deps.Strategies == nil {
			return nil
		}
		return deps.Strategies.Jobs(account)
	}
	return strat.GetJobs(account)
}

func (r *BTResult) orderMatchTfs() map[string]bool {
	if deps := r.reportRuntimeDeps(); deps != nil {
		if deps.Core == nil {
			return nil
		}
		return deps.Core.OrderMatchTfs
	}
	return core.OrderMatchTfs
}

func (r *BTResult) doneProfits(off int) float64 {
	deps := r.reportRuntimeDeps()
	if deps == nil {
		return ormo.LegalDoneProfits(off)
	}
	if deps.Market == nil || deps.Market.Prices == nil {
		return 0
	}
	orders := r.historyOrders()
	if off < 0 {
		off = 0
	}
	if off >= len(orders) {
		return 0
	}
	var total float64
	for _, order := range orders[off:] {
		_, quote, _, _ := core.SplitSymbol(order.Symbol)
		nowMS := int64(0)
		if deps.Clock != nil {
			nowMS = deps.Clock.TimeMS()
		}
		price := deps.Market.Prices.GetPriceSafeExpAt(nowMS, quote, "", com.PriceExpireMS)
		if price >= 0 {
			total += price * order.Profit
		}
	}
	return total
}

func (r *BTResult) printBtResult(reset bool) {
	if cfg := r.runtimeConfig(); cfg != nil && cfg.StratPerf != nil && cfg.StratPerf.Enable {
		if state := r.runtimeCore(); state != nil {
			state.DumpPerfs(r.OutDir)
		} else if r.reportRuntimeDeps() == nil {
			core.DumpPerfs(r.OutDir)
		}
	}
	orders := r.historyOrders()
	log.Info("BackTest Reports:\n" + r.cmdReports(orders))
	if r.HitSlTp > 0 {
		log.Warn("Stop-loss & take-profit triggered in one K-line — set proper `run_policy[i].refine_tf`",
			zap.Int("bad", r.HitSlTp), zap.Int("total", r.OrderNum))
	}
	log.Info("Saved", zap.String("at", r.OutDir))
	if r.CalcDiff > 0.01 {
		log.Error("TotInvestment + TotProfit != FinalBalance, may be bug, please report on github",
			zap.Float64("total_invest", r.TotalInvest), zap.Float64("total_profit", r.TotProfit),
			zap.Float64("final_balance", r.FinBalance), zap.Float64("final_withdraw", r.FinWithdraw),
			zap.Float64("calc_diff", r.CalcDiff))
	}
	r.dumpBtFiles(reset)
}

func (r *BTResult) cmdReports(orders []*ormo.InOutOrder) string {
	var b strings.Builder
	var tblText string
	if len(orders) > 0 {
		items := []struct {
			Title  string
			Handle func(*BTResult) string
		}{
			{Title: " Pair Profits ", Handle: textGroupPairs},
			{Title: " Date Profits ", Handle: textGroupDays},
			{Title: " Profit Ranges ", Handle: textGroupProfitRanges},
			{Title: " Enter Tag ", Handle: textGroupEntTags},
			{Title: " Exit Tag ", Handle: textGroupExitTags},
			{Title: " Drawdowns Top 10 ", Handle: textDrawdowns},
		}
		for _, item := range items {
			tblText = item.Handle(r)
			if tblText != "" {
				width := strings.Index(tblText, "\n")
				head := utils.PadCenter(item.Title, width, "=")
				b.WriteString(head)
				b.WriteString("\n")
				b.WriteString(tblText)
				b.WriteString("\n")
			}
		}
	} else {
		b.WriteString("No Orders Found\n")
	}
	b.WriteString(r.textMetrics(orders))
	return b.String()
}

func (r *BTResult) dumpBtFiles(reset bool) {
	csvPath := fmt.Sprintf("%s/orders.csv", r.OutDir)
	orders := r.historyOrders()
	var err_ error
	if deps := r.reportRuntimeDeps(); deps != nil {
		err_ = dumpOrdersCSV(orders, csvPath, deps)
	} else {
		err_ = DumpOrdersCSV(orders, csvPath)
	}
	if err_ != nil {
		log.Error("dump orders.csv fail", zap.Error(err_))
	}

	err := ormo.DumpOrdersGobItems(filepath.Join(r.OutDir, "orders.gob"), orders)
	if err != nil {
		log.Warn("dump orders.gob fail", zap.Error(err))
	}

	r.dumpConfig()

	if reset {
		r.dumpStrategy()
	}

	r.dumpStratOutputs(reset)

	r.DumpCharts()

	r.dumpDetail("")
}

func (r *BTResult) Collect() {
	orders := r.historyOrders()
	r.OrderNum = len(orders)
	sumProfit := float64(0)
	sumFee := float64(0)
	sumCost := float64(0)
	winCount := float64(0)
	tfHits := make(map[string]int)
	hitSlTp := 0
	for _, od := range orders {
		sumProfit += od.Profit
		sumFee += od.Enter.FeeQuote
		if od.Exit != nil {
			sumFee += od.Exit.FeeQuote
		}
		sumCost += od.EnterCost() / od.Leverage
		if od.Profit > 0 {
			winCount += 1
		}
		oldNum, _ := tfHits[od.Timeframe]
		tfHits[od.Timeframe] = oldNum + 1
		if od.GetInfoString(ormo.OdInfoSLTP) == "yes" {
			hitSlTp += 1
		}
	}
	for tf := range r.orderMatchTfs() {
		if _, ok := tfHits[tf]; !ok {
			tfHits[tf] = 0
		}
	}
	r.HitSlTp = hitSlTp
	r.TfHits = tfHits
	r.TotProfit = sumProfit
	r.TotCost = utils.NanInfTo(sumCost, 0)
	r.TotFee = sumFee
	r.TotProfitPct = r.TotProfit * 100 / r.TotalInvest
	if r.MinReal > r.MaxReal {
		r.MinReal = r.MaxReal
	}
	sort.Slice(r.DrawDowns, func(i, j int) bool {
		return r.DrawDowns[i].ValueChg < r.DrawDowns[j].ValueChg
	})
	if len(r.DrawDowns) > 10 {
		r.DrawDowns = r.DrawDowns[:10]
	}
	// Calculate the maximum drawdown on the chart
	// 计算图表上的最大回撤
	ddRate, ddVal := utils.CalcDrawDown(r.Plots.Real, 0)
	r.ShowDrawDownPct = ddRate * 100
	r.ShowDrawDownVal = ddVal
	if len(orders) > 0 {
		r.WinRatePct = winCount * 100 / float64(len(orders))
		r.groupByPairs(orders)
		r.groupByDates(orders)
		r.groupByProfits(orders)
		r.groupByEnters(orders)
		r.groupByExits(orders)
		labels, dsList := calcGroupEndProfitsWithDeps(orders, func(o *ormo.InOutOrder) string {
			return fmt.Sprintf("%v:%v", o.Strategy, o.EnterTag)
		}, ShowNum, r.reportRuntimeDeps())
		r.EntLabels = labels
		r.EntDatasets = dsList
	}
	wallets := r.reportWallet()
	if wallets != nil {
		r.FinWithdraw = wallets.GetWithdrawLegal(nil)
		r.FinBalance = wallets.AvaLegal(nil) + r.FinWithdraw
	}
	rangeSecs := (r.EndMS - r.StartMS) / 1000
	sharpe, sortino, err := CalcMeasuresByReal(r.Plots.Real, rangeSecs, "", 0, 0)
	if err != nil {
		log.Warn("calc sharpe/sortino fail", zap.Error(err))
	} else {
		r.SharpeRatio, r.SortinoRatio = sharpe, sortino
	}
	r.CalcDiff = math.Abs((r.FinBalance-r.TotProfit)/r.TotalInvest - 1)
	r.Stability = utils.CalcAssetStabilityScore(r.Plots.Real, 0)
}

func (r *BTResult) textMetrics(orders []*ormo.InOutOrder) string {

	totProfitPct := strconv.FormatFloat(r.TotProfitPct, 'f', 1, 64)
	avfProfit := strconv.FormatFloat(r.TotProfitPct*100/float64(len(orders)), 'f', 2, 64)
	avgCost := r.TotCost / float64(len(orders))
	slices.SortFunc(orders, func(a, b *ormo.InOutOrder) int {
		if order := cmp.Compare(a.Profit, b.Profit); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	drawDownRate := strconv.FormatFloat(r.ShowDrawDownPct, 'f', 2, 64) + "%"
	realDrawDown := strconv.FormatFloat(r.MaxDrawDownPct, 'f', 2, 64) + "%"
	drawDownVal := strconv.FormatFloat(r.ShowDrawDownVal, 'f', 0, 64)
	realDrawVal := strconv.FormatFloat(r.MaxDrawDownVal, 'f', 0, 64)
	sharpeStr := strconv.FormatFloat(r.SharpeRatio, 'f', 2, 64)
	sortinoStr := strconv.FormatFloat(r.SortinoRatio, 'f', 2, 64)
	rows := [][]string{
		{"Backtest From", r.dateStr(r.StartMS, "")},
		{"Backtest To", r.dateStr(r.EndMS, "")},
		{"Max Open Orders", strconv.Itoa(r.MaxOpenOrders)},
		{"Total Orders/BarNum", fmt.Sprintf("%v/%v", len(orders), r.BarNum)},
		{"Total Investment", strconv.FormatFloat(r.TotalInvest, 'f', 0, 64)},
		{"Final Balance", strconv.FormatFloat(r.FinBalance, 'f', 2, 64)},
		{"Final WithDraw", strconv.FormatFloat(r.FinWithdraw, 'f', 2, 64)},
		{"Absolute Profit", strconv.FormatFloat(r.TotProfit, 'f', 2, 64)},
		{"Total Profit %", totProfitPct + "%"},
		{"Total Fee", strconv.FormatFloat(r.TotFee, 'f', 2, 64)},
		{"Avg Profit %%", avfProfit + "%%"},
		{"Total Cost", strconv.FormatFloat(r.TotCost, 'f', 2, 64)},
		{"Avg Cost", strconv.FormatFloat(avgCost, 'f', 2, 64)},
	}
	rows2 := [][]string{
		{"Max Assets", strconv.FormatFloat(r.MaxReal, 'f', 1, 64)},
		{"Min Assets", strconv.FormatFloat(r.MinReal, 'f', 1, 64)},
		{"Max DrawDown", fmt.Sprintf("%v / %v", drawDownRate, realDrawDown)},
		{"Max DrawDown", fmt.Sprintf("%v / %v", drawDownVal, realDrawVal)},
		{"Max Fund Occupy", strconv.FormatFloat(r.MaxFundOccup, 'f', 0, 64)},
		{"Max Occupy by Pair", strconv.FormatFloat(r.MaxOccupForPair, 'f', 0, 64)},
		{"TimeFrames", sortTfMap(r.TfHits)},
		{"Win Rate", strconv.FormatFloat(r.WinRatePct, 'f', 1, 64) + "%"},
		{"Sharpe/Sortino", sharpeStr + " / " + sortinoStr},
	}
	if len(orders) > 0 {
		worstVal := strconv.FormatFloat(orders[0].Profit, 'f', 1, 64)
		worstPct := strconv.FormatFloat(orders[0].ProfitRate*100, 'f', 1, 64)
		bestVal := strconv.FormatFloat(orders[len(orders)-1].Profit, 'f', 1, 64)
		bestPct := strconv.FormatFloat(orders[len(orders)-1].ProfitRate*100, 'f', 1, 64)
		rows = append(rows, []string{"Best Order", bestVal + "  " + bestPct + "%"})
		rows = append(rows, []string{"Worst Order", worstVal + "  " + worstPct + "%"})
	}
	rows = append(rows, rows2...)
	return renderTable([]string{"Metric", "Value"}, rows, tw.AlignRight)
}

func renderTable(heads []string, rows [][]string, align tw.Align) string {
	var b bytes.Buffer
	cfg := tw.Rendition{
		Borders: tw.Border{Left: tw.On, Top: tw.Off, Right: tw.On, Bottom: tw.Off},
	}
	table := tablewriter.NewTable(&b,
		tablewriter.WithRenderer(renderer.NewMarkdown(cfg)),
		tablewriter.WithConfig(tablewriter.Config{
			Header: tw.CellConfig{
				Alignment: tw.CellAlignment{Global: align},
			},
			Row: tw.CellConfig{
				Alignment: tw.CellAlignment{Global: align},
			},
			Footer: tw.CellConfig{
				Alignment: tw.CellAlignment{Global: align},
			},
		}),
	)
	table.Header(heads)
	table.Bulk(rows)
	table.Render()
	return b.String()
}

func (r *BTResult) groupByPairs(orders []*ormo.InOutOrder) {
	groups := groupItems(orders, true, func(od *ormo.InOutOrder, i int) string {
		return od.Symbol
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Sharpe > groups[j].Sharpe
	})
	r.PairGrps = groups
}

func textGroupPairs(r *BTResult) string {
	return printGroups(r.PairGrps, "Pair", true, nil, nil)
}

func (r *BTResult) groupByEnters(orders []*ormo.InOutOrder) {
	groups := groupItems(orders, true, func(od *ormo.InOutOrder, i int) string {
		return fmt.Sprintf("%s:%s", od.Strategy, od.EnterTag)
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Title < groups[j].Title
	})
	r.EnterGrps = groups
}

func textGroupEntTags(r *BTResult) string {
	return printGroups(r.EnterGrps, "Enter Tag", true, nil, nil)
}

func (r *BTResult) groupByExits(orders []*ormo.InOutOrder) {
	groups := groupItems(orders, true, func(od *ormo.InOutOrder, i int) string {
		return fmt.Sprintf("%s:%s", od.Strategy, od.ExitTag)
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Title < groups[j].Title
	})
	r.ExitGrps = groups
}

func textGroupExitTags(r *BTResult) string {
	return printGroups(r.ExitGrps, "Exit Tag", true, nil, nil)
}

func textDrawdowns(r *BTResult) string {
	if len(r.DrawDowns) == 0 {
		return ""
	}
	heads := []string{"Start Time", "Start Value", "Lowest Time", "Lowest Value", "Drawdown Value"}
	var rows [][]string
	for _, dd := range r.DrawDowns {
		if dd.ValueChg >= 0 {
			continue
		}
		startTime := r.dateStr(dd.StartMS, core.DefaultDateFmt)
		startVal := strconv.FormatFloat(dd.StartValue, 'f', 2, 64)
		stopTime := r.dateStr(dd.StopMS, core.DefaultDateFmt)
		stopVal := strconv.FormatFloat(dd.StopValue, 'f', 2, 64)
		ddVal := strconv.FormatFloat(dd.ValueChg, 'f', 2, 64)
		rows = append(rows, []string{startTime, startVal, stopTime, stopVal, ddVal})
	}
	if len(rows) == 0 {
		return ""
	}
	return renderTable(heads, rows, tw.AlignCenter)
}

func (r *BTResult) groupByProfits(orders []*ormo.InOutOrder) {
	odNum := len(orders)
	if odNum == 0 {
		return
	}
	rates := make([]float64, 0, len(orders))
	for _, od := range orders {
		rates = append(rates, od.ProfitRate)
	}
	var clsNum int
	if odNum > 150 {
		clsNum = min(19, int(math.Round(math.Pow(float64(odNum), 0.5))))
	} else {
		clsNum = int(math.Round(math.Pow(float64(odNum), 0.6)))
	}
	res := utils.KMeansVals(rates, clsNum)
	var grpTitles = make([]string, 0, len(res.Clusters))
	for _, gp := range res.Clusters {
		if len(gp.Items) == 0 {
			// KMeansVals can emit empty clusters for degenerate (many-duplicate) inputs.
			// No order maps to an empty cluster (res.RowGIds never references it), but a
			// title slot must still exist so grpTitles stays index-aligned with the cluster
			// ids used below via grpTitles[res.RowGIds[i]].
			grpTitles = append(grpTitles, "")
			continue
		}
		minPct := strconv.FormatFloat(slices.Min(gp.Items)*100, 'f', 2, 64)
		maxPct := strconv.FormatFloat(slices.Max(gp.Items)*100, 'f', 2, 64)
		grpTitles = append(grpTitles, fmt.Sprintf("%s ~ %s%%", minPct, maxPct))
	}
	groups := groupItems(orders, false, func(od *ormo.InOutOrder, i int) string {
		return grpTitles[res.RowGIds[i]]
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Orders[0].ProfitRate < groups[j].Orders[0].ProfitRate
	})
	r.ProfitGrps = groups
}

func textGroupProfitRanges(r *BTResult) string {
	return printGroups(r.ProfitGrps, "Profit Range", false, []string{"Enter Tags", "Exit Tags"}, makeEnterExits)
}

func (r *BTResult) groupByDates(orders []*ormo.InOutOrder) {
	units := []string{"1Y", "1Q", "1M", "1w", "1d", "6h", "1h"}
	startMS, endMS := orders[0].RealEnterMS(), orders[len(orders)-1].RealEnterMS()
	var bestTF string
	var bestTFSecs int
	var bestScore float64
	// Find the optimal granularity for grouping
	// 查找分组的最佳粒度
	for _, tf := range units {
		tfSecs := utils2.TFToSecs(tf)
		grpNum := float64(endMS-startMS) / 1000 / float64(tfSecs)
		numPerGp := float64(len(orders)) / grpNum
		score1 := utils.NearScore(grpNum, 18, 2)
		score2 := utils.NearScore(min(numPerGp, 60), 40, 1)
		curScore := score2 * score1
		if curScore > bestScore {
			bestTF = tf
			bestTFSecs = tfSecs
			bestScore = curScore
		}
	}
	if bestTF == "" {
		bestTF = "1d"
		bestTFSecs = utils2.TFToSecs(bestTF)
	}
	tfUnit := bestTF[1]
	groups := groupItems(orders, false, func(od *ormo.InOutOrder, i int) string {
		entMS := od.RealEnterMS()
		if tfUnit == 'Y' {
			return r.dateStrLoc(entMS, "2006")
		} else if tfUnit == 'Q' {
			enterMS := utils2.AlignTfMSecs(entMS, int64(bestTFSecs*1000))
			return r.dateStrLoc(enterMS, "2006-01")
		} else if tfUnit == 'M' {
			return r.dateStrLoc(entMS, "2006-01")
		} else if tfUnit == 'd' || tfUnit == 'w' {
			enterMS := utils2.AlignTfMSecs(entMS, int64(bestTFSecs*1000))
			return r.dateStrLoc(enterMS, "2006-01-02")
		} else {
			return r.dateStrLoc(entMS, "2006-01-02 15")
		}
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Title < groups[j].Title
	})
	r.DateGrps = groups
}

func textGroupDays(r *BTResult) string {
	return printGroups(r.DateGrps, "Date", false, []string{"Enter Tags", "Exit Tags"}, makeEnterExits)
}

func makeEnterExits(orders []*ormo.InOutOrder) []string {
	enters := make(map[string]int)
	exits := make(map[string]int)
	for _, od := range orders {
		if num, ok := enters[od.EnterTag]; ok {
			enters[od.EnterTag] = num + 1
		} else {
			enters[od.EnterTag] = 1
		}
		if num, ok := exits[od.ExitTag]; ok {
			exits[od.ExitTag] = num + 1
		} else {
			exits[od.ExitTag] = 1
		}
	}
	entList := make([]string, 0, len(enters))
	exitList := make([]string, 0, len(enters))
	for k, v := range enters {
		entList = append(entList, fmt.Sprintf("%s/%v", k, v))
	}
	for k, v := range exits {
		exitList = append(exitList, fmt.Sprintf("%s/%v", k, v))
	}
	return []string{
		strings.Join(entList, " "),
		strings.Join(exitList, " "),
	}
}

func groupItems(orders []*ormo.InOutOrder, measure bool, getTag func(od *ormo.InOutOrder, i int) string) []*RowItem {
	if len(orders) == 0 {
		return nil
	}
	groups := make(map[string]*RowItem)
	for i, od := range orders {
		tag := getTag(od, i)
		sta, ok := groups[tag]
		duration := max(0, int((od.RealExitMS()-od.RealEnterMS())/1000))
		isWin := od.Profit >= 0
		if !ok {
			sta = &RowItem{
				Title: tag,
				RowPart: RowPart{
					OrderNum:     1,
					ProfitSum:    od.Profit,
					ProfitPctSum: od.ProfitRate,
					CostSum:      od.EnterCost() / od.Leverage,
					Durations:    []int{duration},
					Orders:       make([]*ormo.InOutOrder, 0, 8),
				},
			}
			sta.Orders = append(sta.Orders, od)
			if isWin {
				sta.WinCount = 1
			}
			groups[tag] = sta
		} else {
			if isWin {
				sta.WinCount += 1
			}
			sta.OrderNum += 1
			sta.ProfitSum += od.Profit
			sta.ProfitPctSum += od.ProfitRate
			sta.CostSum += od.EnterCost() / od.Leverage
			sta.Durations = append(sta.Durations, duration)
			sta.Orders = append(sta.Orders, od)
		}
	}
	if measure {
		// 分30份采样计算指标，太大的话会导致指标偏小
		for _, gp := range groups {
			sharpe, sortino, err := CalcMeasureByOrders(gp.Orders)
			if err != nil {
				log.Warn("calc measure fail", zap.Error(err))
			} else {
				if !math.IsNaN(sharpe) && !math.IsInf(sharpe, 0) {
					gp.Sharpe = sharpe
				}
				if !math.IsNaN(sortino) && !math.IsInf(sortino, 0) {
					gp.Sortino = sortino
				}
			}
		}
	}
	return utils.ValsOfMap(groups)
}

func printGroups(groups []*RowItem, title string, measure bool, extHeads []string, prcGrp func([]*ormo.InOutOrder) []string) string {
	heads := []string{title, "Count", "Avg Profit %", "Tot Profit %", "Sum Profit", "Duration(h'm)", "Win Rate"}
	if measure {
		heads = append(heads, "Sharpe/Sortino")
	}
	if len(extHeads) > 0 {
		heads = append(heads, extHeads...)
	}
	var rows [][]string
	for _, sta := range groups {
		grpCount := len(sta.Orders)
		numText := strconv.Itoa(grpCount)
		avgProfit := strconv.FormatFloat(sta.ProfitPctSum*100/float64(grpCount), 'f', 2, 64)
		totProfit := strconv.FormatFloat(sta.ProfitSum*100/sta.CostSum, 'f', 2, 64)
		sumProfit := strconv.FormatFloat(sta.ProfitSum, 'f', 2, 64)
		duraText := kMeansDurations(sta.Durations, 3)
		winRate := strconv.FormatFloat(float64(sta.WinCount)*100/float64(grpCount), 'f', 1, 64) + "%"
		row := []string{sta.Title, numText, avgProfit, totProfit, sumProfit, duraText, winRate}
		if measure {
			sharpeStr := strconv.FormatFloat(sta.Sharpe, 'f', 2, 64)
			sortinoStr := strconv.FormatFloat(sta.Sortino, 'f', 2, 64)
			row = append(row, sharpeStr+" / "+sortinoStr)
		}
		if prcGrp != nil {
			cols := prcGrp(sta.Orders)
			row = append(row, cols...)
		}
		rows = append(rows, row)
	}
	return renderTable(heads, rows, tw.AlignCenter)
}

func CalcMeasureByOrders(ods []*ormo.InOutOrder) (float64, float64, *errs.Error) {
	return calcMeasureByOrders(ods, nil)
}

// CalcMeasureByOrdersWithRuntimeDeps calculates order metrics using only the
// supplied runtime's wallet prices, symbol catalog, and storage owner.
func CalcMeasureByOrdersWithRuntimeDeps(ods []*ormo.InOutOrder, deps biz.RuntimeDeps) (float64, float64, *errs.Error) {
	return calcMeasureByOrders(ods, NewReportDeps(deps))
}

func calcMeasureByOrders(ods []*ormo.InOutOrder, deps *ReportDeps) (float64, float64, *errs.Error) {
	if len(ods) == 0 {
		return 0, 0, nil
	}
	initStake := float64(0)
	if deps == nil {
		for key, val := range config.WalletAmounts {
			initStake += val * com.GetPriceSafe(key, "")
		}
	} else {
		cfg := deps.configView()
		if cfg == nil || deps.Market == nil || deps.Market.Prices == nil {
			return 0, 0, errs.NewMsg(core.ErrBadConfig, "runtime report config and market prices are required")
		}
		nowMS := int64(0)
		if deps.Clock != nil {
			nowMS = deps.Clock.TimeMS()
		}
		for key, val := range cfg.WalletAmounts {
			price := deps.Market.Prices.GetPriceSafeExpAt(nowMS, key, "", com.PriceExpireMS)
			if price < 0 {
				return 0, 0, errs.NewMsg(core.ErrRunTime, "no valid runtime price for wallet currency: %s", key)
			}
			initStake += val * price
		}
	}
	tf := "1d"
	tfSecs := utils2.TFToSecs(tf)
	tfMsecs := int64(tfSecs * 1000)
	startMS, endMS := ormo.CalcTimeRange(ods)
	startMS = utils2.AlignTfMSecs(startMS, tfMsecs)
	endMS = utils2.AlignTfMSecs(endMS, tfMsecs) + tfMsecs
	pairOrders := make(map[string][]*ormo.InOutOrder)
	for _, od := range ods {
		items, _ := pairOrders[od.Symbol]
		pairOrders[od.Symbol] = append(items, od)
	}
	cumRets, err := calcCumCurveWithDeps(pairOrders, startMS, endMS, tf, initStake, deps)
	if err != nil {
		return 0, 0, err
	}
	if len(cumRets) == 0 {
		return 0, 0, nil
	}
	lastRet := cumRets[0]
	retRates := make([]float64, len(cumRets))
	for i, ret := range cumRets {
		retRates[i] = (ret - lastRet) / lastRet
		lastRet = ret
	}
	periods := utils2.TFToSecs("1y") / tfSecs
	return calcMeasures(retRates, periods, 0)
}

func calcMeasures(returns []float64, periods int, riskFree float64) (float64, float64, *errs.Error) {
	sharpeFlt, err := utils.SharpeRatioBy(returns, riskFree, periods, true)
	if err != nil {
		return 0, 0, errs.New(errs.CodeRunTime, err)
	}
	sortineFlt, err := utils.SortinoRatioBy(returns, riskFree, periods, true)
	if err != nil {
		if !errors.Is(err, utils.ErrNoNegativeResults) {
			return sharpeFlt, 0, errs.New(errs.CodeRunTime, err)
		}
		sortineFlt = math.Inf(1)
	}
	return sharpeFlt, sortineFlt, nil
}

func kMeansDurations(durations []int, num int) string {
	slices.Sort(durations)
	diffNum := 1
	for i, val := range durations[1:] {
		if val != durations[i] {
			diffNum += 1
		}
	}
	if diffNum < num {
		if len(durations) == 0 {
			return ""
		}
		num = diffNum
	}
	var d = make([]float64, 0, len(durations))
	for _, dura := range durations {
		d = append(d, float64(dura))
	}
	var res = utils.KMeansVals(d, num)
	if res == nil {
		return ""
	}
	var b strings.Builder
	for _, grp := range res.Clusters {
		grpNum := len(grp.Items)
		var coord int
		if grpNum == 1 {
			coord = int(math.Round(grp.Items[0]))
		} else {
			coord = int(math.Round(grp.Center))
		}
		if coord < 60 {
			b.WriteString(strconv.Itoa(coord))
			b.WriteString("s")
		} else {
			mins := coord / 60
			hours := mins / 60
			lmins := mins % 60
			b.WriteString(strconv.Itoa(hours))
			if hours <= 99 {
				b.WriteString("'")
				b.WriteString(strconv.Itoa(lmins))
			}
		}
		b.WriteString("/")
		b.WriteString(strconv.Itoa(grpNum))
		b.WriteString("  ")
	}
	return b.String()
}

func DumpOrdersCSV(orders []*ormo.InOutOrder, outPath string) error {
	return dumpOrdersCSV(orders, outPath, nil)
}

func dumpOrdersCSV(orders []*ormo.InOutOrder, outPath string, deps *ReportDeps) error {
	sort.Slice(orders, func(i, j int) bool {
		var a, b = orders[i], orders[j]
		var ta, tb = a.RealEnterMS(), b.RealEnterMS()
		if ta != tb {
			return ta < tb
		}
		if a.Symbol != b.Symbol {
			return a.Symbol < b.Symbol
		}
		if a.Strategy != b.Strategy {
			return a.Strategy < b.Strategy
		}
		if a.EnterTag != b.EnterTag {
			return a.EnterTag < b.EnterTag
		}
		if a.Enter.Amount != b.Enter.Amount {
			return a.Enter.Amount < b.Enter.Amount
		}
		return a.ID < b.ID
	})
	dateStr := func(timestamp int64) string {
		if deps != nil {
			return deps.dateStrLoc(timestamp, "")
		}
		return btime.ToDateStrLoc(timestamp, "")
	}
	file, err_ := os.Create(outPath)
	if err_ != nil {
		return err_
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	heads := []string{"sid", "symbol", "timeframe", "direction", "leverage", "entAt", "entTag", "entPrice",
		"entAmount", "entCost", "entFee", "exitAt", "exitTag", "exitPrice", "exitAmount", "exitGot",
		"exitFee", "maxPftRate", "maxDrawDown", "profitRate", "profit", "strategy"}
	if err_ = writer.Write(heads); err_ != nil {
		return err_
	}
	colNum := len(heads)
	for _, od := range orders {
		row := make([]string, colNum)
		row[0] = fmt.Sprintf("%v", od.Sid)
		row[1] = od.Symbol
		row[2] = od.Timeframe
		row[3] = "long"
		if od.Short {
			row[3] = "short"
		}
		row[4] = fmt.Sprintf("%v", od.Leverage)
		row[5] = dateStr(od.RealEnterMS())
		row[6] = od.EnterTag
		if od.Enter != nil {
			row[7], row[8], row[9], row[10] = calcExOrder(od.Enter)
		}
		row[11] = dateStr(od.RealExitMS())
		row[12] = od.ExitTag
		if od.Exit != nil {
			row[13], row[14], row[15], row[16] = calcExOrder(od.Exit)
		}
		row[17] = strconv.FormatFloat(od.MaxPftRate, 'f', 4, 64)
		row[18] = strconv.FormatFloat(od.MaxDrawDown, 'f', 4, 64)
		row[19] = strconv.FormatFloat(od.ProfitRate, 'f', 4, 64)
		row[20] = strconv.FormatFloat(od.Profit, 'f', 8, 64)
		row[21] = od.Strategy
		if err_ = writer.Write(row); err_ != nil {
			return err_
		}
	}
	return nil
}

func calcExOrder(od *ormo.ExOrder) (string, string, string, string) {
	price := od.Average
	if price == 0 {
		price = od.Price
	}
	exitGot := price * od.Filled
	priceStr := strconv.FormatFloat(price, 'f', 8, 64)
	amtStr := strconv.FormatFloat(od.Filled, 'f', 8, 64)
	valStr := strconv.FormatFloat(exitGot, 'f', 4, 64)
	feeStr := strconv.FormatFloat(od.FeeQuote, 'f', 8, 64)
	return priceStr, amtStr, valStr, feeStr
}

func (r *BTResult) dumpConfig() {
	cfg := r.runtimeConfig()
	if cfg == nil {
		log.Error("runtime config is unavailable while dumping backtest config")
		return
	}
	data, err := cfg.Desensitize().DumpYaml()
	if err != nil {
		log.Error("marshal config as yaml fail", zap.Error(err))
		return
	}
	outName := fmt.Sprintf("%s/config.yml", r.OutDir)
	// 这里不检查是否存在，直接覆盖，因webUI创建的带有敏感信息
	err_ := os.WriteFile(outName, data, 0644)
	if err_ != nil {
		log.Error("save yaml to file fail", zap.Error(err_))
	}
}

func (r *BTResult) dumpStrategy() {
	stratDir := r.strategyDir()
	if stratDir == "" {
		log.Info("env `BanStratDir` not configured, skip backup strategy")
		return
	}
	pairTfs := map[string]map[string]string(nil)
	if state := r.runtimeCore(); state != nil {
		pairTfs = state.StgPairTfs
	} else if r.reportRuntimeDeps() == nil {
		pairTfs = core.StgPairTfs
	}
	for name := range pairTfs {
		dname := strings.Split(name, ":")[0]
		curDir, err_ := utils.FindSubPath(stratDir, dname, 3)
		if err_ != nil {
			log.Warn("skip backup strat", zap.String("name", name), zap.Error(err_))
			continue
		}
		tgtDir := fmt.Sprintf("%s/strat_%s", r.OutDir, dname)
		err_ = utils.CopyDirExcluding(curDir, tgtDir, r.OutDir)
		if err_ != nil {
			log.Warn("backup strat fail", zap.String("name", name), zap.Error(err_))
		}
	}
}

func (r *BTResult) dumpStratOutputs(reset bool) {
	groups := make(map[string][]string)
	pairStrats := map[string]map[string]*strat.TradeStrat(nil)
	if state := r.strategyState(); state != nil {
		pairStrats = state.PairStrats
	} else if r.reportRuntimeDeps() == nil {
		pairStrats = strat.PairStrats
	}
	for _, items := range pairStrats {
		for _, stgy := range items {
			if len(stgy.Outputs) == 0 {
				continue
			}
			rows, _ := groups[stgy.Name]
			groups[stgy.Name] = append(rows, stgy.Outputs...)
			if reset {
				stgy.Outputs = nil
			}
		}
	}
	for name, rows := range groups {
		name = strings.ReplaceAll(name, ":", "_")
		outPath := fmt.Sprintf("%s/%s.log", r.OutDir, name)
		file, err := os.Create(outPath)
		if err != nil {
			log.Error("create strategy output file fail", zap.String("name", name), zap.Error(err))
			continue
		}
		_, err = file.WriteString(strings.Join(rows, "\n"))
		if err != nil {
			log.Error("write strategy output fail", zap.String("name", name), zap.Error(err))
		}
		err = file.Close()
		if err != nil {
			log.Error("close strategy output fail", zap.String("name", name), zap.Error(err))
		}
	}
}

func (r *BTResult) DumpCharts() {
	odNum := make([]float64, 0, len(r.Plots.OdNum))
	for _, v := range r.Plots.OdNum {
		odNum = append(odNum, float64(v))
	}
	jobNum := make([]float64, 0, len(r.Plots.JobNum))
	for _, v := range r.Plots.JobNum {
		jobNum = append(jobNum, float64(v))
	}
	outPath := fmt.Sprintf("%s/assets.html", r.OutDir)
	title := "Real-time Assets/Balances/Unrealized P&L/Withdrawals/Concurrent Orders"
	tplPath := filepath.Join(r.dataDir(), "lines.html")
	tplData, _ := os.ReadFile(tplPath)
	dataList := []*ChartDs{
		{Label: "Real", Data: r.Plots.Real},
		{Label: "Available", Data: r.Plots.Available},
		{Label: "Profit", Data: r.Plots.Profit, Hidden: true},
		{Label: "UnPOL", Data: r.Plots.UnrealizedPOL, Hidden: true},
		{Label: "Withdraw", Data: r.Plots.WithDraw, Hidden: true},
		{Label: "OrderNum", Data: odNum, YAxisID: "yRight", Hidden: true},
		{Label: "JobNum", Data: jobNum, YAxisID: "yRight", Hidden: true},
	}
	if len(r.Plots.More) > 0 {
		dataList = append(dataList, r.Plots.More...)
	}
	err := DumpChart(outPath, title, r.Plots.Labels, 5, tplData, dataList)
	if err != nil {
		log.Error("save assets.html fail", zap.Error(err))
	}
	// Draw cumulative profit curves for each entry tag separately
	// 为入场标签分别绘制累计利润曲线
	if len(r.EntLabels) > 0 {
		outPath = fmt.Sprintf("%s/enters.html", r.OutDir)
		title = "Strategy Enter Tag Cum Profits"
		err = DumpChart(outPath, title, r.EntLabels, 5, nil, r.EntDatasets)
		if err != nil {
			log.Error("Dump EnterTag CumProfits fail", zap.Error(err))
		}
	}
}

func (r *BTResult) Score() float64 {
	return CalcBtScore(r.TotProfitPct, r.ShowDrawDownPct)
}

func (r *BTResult) dumpDetail(outPath string) {
	if outPath == "" {
		outPath = fmt.Sprintf("%s/detail.json", r.OutDir)
	}
	if r.CreateMS == 0 {
		r.CreateMS = btime.UTCStamp()
	}
	data, err_ := utils2.Marshal(r)
	if err_ != nil {
		log.Error("marshal backtest detail fail", zap.Error(err_))
		return
	}
	err_ = os.WriteFile(outPath, data, 0644)
	if err_ != nil {
		log.Error("write backtest detail fail", zap.Error(err_))
	}
}

/*
DelBigObjects 删除大对象引用，避免内存泄露
*/
func (r *BTResult) DelBigObjects() {
	grpList := [][]*RowItem{r.PairGrps, r.DateGrps, r.EnterGrps, r.ExitGrps, r.ProfitGrps}
	for _, gp := range grpList {
		for _, p := range gp {
			p.Orders = nil
			p.Durations = nil
		}
	}
}

func (r *BTResult) logState(startMS, timeMS int64, odNum int) {
	if r.StartMS == 0 {
		r.StartMS = startMS
	}
	r.EndMS = timeMS
	wallets := r.reportWallet()
	if wallets == nil {
		return
	}
	totalLegal := wallets.TotalLegal(nil, true)
	r.MinReal = min(r.MinReal, totalLegal)
	if totalLegal > r.MaxReal {
		r.MaxReal = totalLegal
		r.TmpDrawDown = &core.TimeValueRange{
			StartMS:    timeMS,
			StartValue: totalLegal,
			StopMS:     timeMS,
			StopValue:  totalLegal,
		}
		r.DrawDowns = append(r.DrawDowns, r.TmpDrawDown)
	} else if totalLegal < r.MaxReal {
		drawDownPct := (r.MaxReal - totalLegal) * 100 / r.MaxReal
		r.MaxDrawDownPct = max(r.MaxDrawDownPct, drawDownPct)
		if r.TmpDrawDown != nil && totalLegal < r.TmpDrawDown.StopValue {
			r.TmpDrawDown.SetEnd(timeMS, totalLegal)
		}
		r.MaxDrawDownVal = max(r.MaxDrawDownVal, r.MaxReal-totalLegal)
		maxOccupy := r.MaxReal - wallets.AvaLegal(nil)
		r.MaxFundOccup = max(r.MaxFundOccup, maxOccupy)
		pairNum := 0
		if state := r.runtimeCore(); state != nil {
			pairNum = len(state.Pairs)
		} else if r.reportRuntimeDeps() == nil {
			pairNum = len(core.Pairs)
		}
		if pairNum > 0 {
			r.MaxOccupForPair = max(r.MaxOccupForPair, maxOccupy/float64(pairNum))
		}
	}
	if r.TimeNum%r.PlotEvery != 0 {
		if odNum > r.Plots.tmpOdNum {
			r.Plots.tmpOdNum = odNum
		}
		return
	}
	if r.Plots.tmpOdNum > odNum {
		odNum = r.Plots.tmpOdNum
	}
	r.Plots.tmpOdNum = 0
	splStep := 5
	if len(r.Plots.Real) >= ShowNum*splStep {
		// Check whether there is too much data and resample if the total number of samples exceeds 5 times
		// 检查数据是否太多，超过采样总数5倍时，进行重采样
		r.PlotEvery *= splStep
		oldNum := len(r.Plots.Real)
		newNum := oldNum / splStep
		plots := &PlotData{
			Labels:        make([]string, 0, newNum),
			OdNum:         make([]int, 0, newNum),
			JobNum:        make([]int, 0, newNum),
			Real:          make([]float64, 0, newNum),
			Available:     make([]float64, 0, newNum),
			UnrealizedPOL: make([]float64, 0, newNum),
			WithDraw:      make([]float64, 0, newNum),
		}
		for i := 0; i < oldNum; i += splStep {
			plots.Labels = append(plots.Labels, r.Plots.Labels[i])
			plots.OdNum = append(plots.OdNum, slices.Max(r.Plots.OdNum[i:i+splStep]))
			plots.JobNum = append(plots.JobNum, slices.Max(r.Plots.JobNum[i:i+splStep]))
			plots.Real = append(plots.Real, r.Plots.Real[i])
			plots.Available = append(plots.Available, r.Plots.Available[i])
			plots.Profit = append(plots.Profit, r.Plots.Profit[i])
			plots.UnrealizedPOL = append(plots.UnrealizedPOL, r.Plots.UnrealizedPOL[i])
			plots.WithDraw = append(plots.WithDraw, r.Plots.WithDraw[i])
		}
		r.Plots = plots
		return
	}
	r.logPlot(wallets, timeMS, odNum, totalLegal)
}

func (r *BTResult) logPlot(wallets *biz.BanWallets, timeMS int64, odNum int, totalLegal float64) {
	if wallets == nil {
		return
	}
	if timeMS < r.lastPlotMS {
		log.Error("backtest plot time moved backwards",
			zap.Int64("last_ms", r.lastPlotMS), zap.Int64("current_ms", timeMS))
		return
	}
	if odNum < 0 {
		odNum = 0
		account := r.reportAccount()
		if state := r.orderState(); state != nil {
			odNum = state.OpenNum(account, ormo.InOutStatusPartEnter)
		} else if r.reportRuntimeDeps() == nil {
			odNum = ormo.OpenNum(account, ormo.InOutStatusPartEnter)
		}
	}
	jobNum := 0
	jobMap := r.strategyJobs(wallets.Account)
	for _, jobs := range jobMap {
		for _, j := range jobs {
			if j.CheckMS+j.Env.TFMSecs >= timeMS {
				jobNum += 1
			}
		}
	}
	if totalLegal < 0 {
		totalLegal = wallets.TotalLegal(nil, true)
	}
	avaLegal := wallets.AvaLegal(nil)
	profitLegal := wallets.UnrealizedPOLLegal(nil)
	drawLegal := wallets.GetWithdrawLegal(nil)
	curDate := r.dateStr(timeMS, "")
	r.donePftLegal += r.doneProfits(r.histOdOff)
	r.histOdOff = len(r.historyOrders())
	if timeMS == r.lastPlotMS && len(r.Plots.Labels) > 0 {
		last := len(r.Plots.Labels) - 1
		r.Plots.Labels[last] = curDate
		r.Plots.OdNum[last] = odNum
		r.Plots.JobNum[last] = jobNum
		r.Plots.Real[last] = totalLegal
		r.Plots.Available[last] = avaLegal
		r.Plots.Profit[last] = r.donePftLegal
		r.Plots.UnrealizedPOL[last] = profitLegal
		r.Plots.WithDraw[last] = drawLegal
		return
	}
	r.lastPlotMS = timeMS
	r.Plots.Labels = append(r.Plots.Labels, curDate)
	r.Plots.OdNum = append(r.Plots.OdNum, odNum)
	r.Plots.JobNum = append(r.Plots.JobNum, jobNum)
	r.Plots.Real = append(r.Plots.Real, totalLegal)
	r.Plots.Available = append(r.Plots.Available, avaLegal)
	r.Plots.Profit = append(r.Plots.Profit, r.donePftLegal)
	r.Plots.UnrealizedPOL = append(r.Plots.UnrealizedPOL, profitLegal)
	r.Plots.WithDraw = append(r.Plots.WithDraw, drawLegal)
}

// CalcMeasuresByReal real & rangeSecs are required, tf=1d, factor=365, riskFree=0
func CalcMeasuresByReal(real []float64, rangeSecs int64, tf string, factor int, riskFree float64) (float64, float64, *errs.Error) {
	inLen := len(real)
	if inLen <= 1 {
		return 0, 0, nil
	}
	if tf == "" {
		tf, factor = "1d", 365
	}
	tfSecs := utils2.TFToSecs(tf)
	yearSecs := utils2.TFToSecs("1y")
	// 期望数量=时间范围/年*年内周期数
	expNum := int(float64(rangeSecs) / float64(yearSecs) * float64(factor))
	rate := float64(inLen) / float64(expNum)
	step, smpNum := 1, 0
	if rate < 0.7 {
		// 输入数量远小于给定时间周期，使用输入数量对应周期
		smpNum = inLen
		tfSecs = int(float64(tfSecs) / rate)
	} else {
		// 输入数量远多于给定周期，计算采样步长
		step = max(1, int(math.Round(rate)))
		smpNum = inLen / step
		tfSecs = int(rangeSecs) / smpNum
	}
	factor = yearSecs / tfSecs
	prevVal := real[0]
	inReturns := make([]float64, 0, smpNum+1)
	cumRets := make([]float64, 0, smpNum+1)
	for i := step; i < inLen; i += step {
		curVal := real[i]
		retVal := (curVal - prevVal) / prevVal
		if math.IsInf(retVal, 0) || math.IsNaN(retVal) {
			retVal = 0
		}
		inReturns = append(inReturns, retVal)
		prevVal = curVal
		cumRets = append(cumRets, curVal)
	}
	if len(inReturns) <= 1 {
		return 0, 0, nil
	}
	sharpe, sortino, err := calcMeasures(inReturns, factor, riskFree)
	if err != nil {
		return 0, 0, err
	}
	sharpe = utils.NanInfTo(sharpe, 0)
	sortino = utils.NanInfTo(sortino, 0)
	return sharpe, sortino, nil
}

func selectPairs(r *BTResult, name string) []string {
	fn, ok := PairPickers[name]
	if !ok {
		log.Warn("PairPickers not found", zap.String("name", name))
		return nil
	}
	return fn(r)
}

func ParseBtResult(path string) (*BTResult, *errs.Error) {
	data, err_ := os.ReadFile(path)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	var res = BTResult{}
	err_ = utils2.Unmarshal(data, &res, utils2.JsonNumDefault)
	if err_ != nil {
		return nil, errs.New(errs.CodeUnmarshalFail, err_)
	}
	return &res, nil
}

func CalcBtScore(profitPct, drawDownPct float64) float64 {
	var score float64
	if profitPct <= 0 {
		score = profitPct
	} else {
		// 盈利时返回无回撤收益率
		score = profitPct * math.Pow(1-drawDownPct/100, 1.5)
	}
	return utils.NanInfTo(score, 0)
}

/*
DumpEnterTagCumProfits

export line chart of cumulative profit based on entry tag statistics

按入场信号统计累计利润导出折线图
*/
func DumpEnterTagCumProfits(path string, odList []*ormo.InOutOrder, xNum int) *errs.Error {
	labels, dsList := CalcGroupEndProfits(odList, func(o *ormo.InOutOrder) string {
		return fmt.Sprintf("%v:%v", o.Strategy, o.EnterTag)
	}, xNum)
	if len(dsList) == 0 {
		return nil
	}
	title := "Strategy Enter Tag Cum Profits"
	err := DumpChart(path, title, labels, 5, nil, dsList)
	return err
}

/*
CalcGroupEndProfits

calculate cumulative profit curve data for orders (directly using profit accumulation when closing positions)

生成订单累计利润曲线数据（直接使用平仓时利润累加）
*/
func CalcGroupEndProfits(odList []*ormo.InOutOrder, genKey func(o *ormo.InOutOrder) string, xNum int) ([]string, []*ChartDs) {
	return calcGroupEndProfitsWithDeps(odList, genKey, xNum, nil)
}

func calcGroupEndProfitsWithDeps(odList []*ormo.InOutOrder, genKey func(o *ormo.InOutOrder) string, xNum int, deps *ReportDeps) ([]string, []*ChartDs) {
	if len(odList) == 0 {
		return nil, nil
	}
	if xNum <= 0 {
		xNum = 1
	}
	orders := slices.Clone(odList)
	sort.SliceStable(orders, func(i, j int) bool {
		a, b := orders[i], orders[j]
		if ta, tb := a.RealExitMS(), b.RealExitMS(); ta != tb {
			return ta < tb
		}
		if ta, tb := a.RealEnterMS(), b.RealEnterMS(); ta != tb {
			return ta < tb
		}
		return a.ID < b.ID
	})
	startMs := int64(math.MaxInt64)
	endMs := int64(0)
	tagMap := make(map[string][]*TimeVal)
	groupOrder := make([]string, 0, 8)
	for _, od := range orders {
		startMs = min(startMs, od.RealEnterMS())
		endMs = max(endMs, od.RealExitMS())
		key := genKey(od)
		items, exists := tagMap[key]
		if !exists {
			groupOrder = append(groupOrder, key)
		}
		curVal := float64(0)
		if len(items) > 0 {
			curVal = items[len(items)-1].Value
		}
		tagMap[key] = append(items, &TimeVal{Time: od.RealExitMS(), Value: curVal + od.Profit})
	}
	if endMs < startMs {
		endMs = startMs
	}
	spanMs := endMs - startMs
	sampleTime := func(i int) int64 {
		if i == xNum {
			return endMs
		}
		return startMs + spanMs*int64(i)/int64(xNum)
	}
	labels := make([]string, xNum+1)
	for i := range labels {
		if deps == nil {
			labels[i] = btime.ToDateStr(sampleTime(i), "")
		} else {
			labels[i] = deps.dateStr(sampleTime(i), "")
		}
	}
	var res []*ChartDs
	for _, tag := range groupOrder {
		items := tagMap[tag]
		arr := make([]float64, xNum+1)
		i := 0
		curVal := float64(0)
		for j := range arr {
			curMs := sampleTime(j)
			for i < len(items) && items[i].Time <= curMs {
				curVal = items[i].Value
				i += 1
			}
			arr[j] = curVal
		}
		res = append(res, &ChartDs{
			Label: tag,
			Data:  arr,
		})
	}
	return labels, res
}

/*
CalcGroupCumProfits

calculate cumulative profit curve data for orders (obtain K-line and calculate real-time cumulative profit for open positions)

生成订单累计利润曲线数据（获取K线，计算实时持仓累计利润）
*/
func CalcGroupCumProfits(odList []*ormo.InOutOrder, genKey func(o *ormo.InOutOrder) string, xNum int) ([]string, []*ChartDs, *errs.Error) {
	return calcGroupCumProfitsWithDeps(odList, genKey, xNum, nil)
}

// CalcGroupCumProfitsWithRuntimeDeps calculates cumulative profit curves from
// one runtime's symbol catalog, storage owner, and display configuration.
func CalcGroupCumProfitsWithRuntimeDeps(odList []*ormo.InOutOrder, genKey func(o *ormo.InOutOrder) string, xNum int, deps biz.RuntimeDeps) ([]string, []*ChartDs, *errs.Error) {
	return calcGroupCumProfitsWithDeps(odList, genKey, xNum, NewReportDeps(deps))
}

func calcGroupCumProfitsWithDeps(odList []*ormo.InOutOrder, genKey func(o *ormo.InOutOrder) string, xNum int, deps *ReportDeps) ([]string, []*ChartDs, *errs.Error) {
	if len(odList) == 0 {
		return nil, nil, nil
	}
	if deps != nil {
		if err := deps.validateSeries(); err != nil {
			return nil, nil, err
		}
	}
	groups := make(map[string]map[string][]*ormo.InOutOrder)
	groupOrder := make([]string, 0, 8)
	minTimeMS, maxTimeMS := int64(math.MaxInt64), int64(0)
	for _, od := range odList {
		key := genKey(od)
		odMap, ok1 := groups[key]
		if !ok1 {
			odMap = make(map[string][]*ormo.InOutOrder)
			groups[key] = odMap
			groupOrder = append(groupOrder, key)
		}
		old, _ := odMap[od.Symbol]
		odMap[od.Symbol] = append(old, od)
		minTimeMS = min(minTimeMS, od.RealEnterMS())
		maxTimeMS = max(maxTimeMS, od.RealExitMS())
	}
	unitSecs := int((maxTimeMS-minTimeMS)/1000) / xNum
	tf := utils.RoundSecsTF(max(unitSecs, 60))
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	startMS := utils2.AlignTfMSecs(minTimeMS, tfMSecs)
	endMS := utils2.AlignTfMSecs(maxTimeMS, tfMSecs) + tfMSecs
	var result []*ChartDs
	maxXNum := 0
	for _, key := range groupOrder {
		pairMap := groups[key]
		var cumRets []float64
		var err *errs.Error
		if deps == nil {
			cumRets, err = calcCumCurve(pairMap, startMS, endMS, tf, 0)
		} else {
			cumRets, err = calcCumCurveWithDeps(pairMap, startMS, endMS, tf, 0, deps)
		}
		if err != nil {
			return nil, nil, err
		}
		maxXNum = max(maxXNum, len(cumRets))
		result = append(result, &ChartDs{
			Label: key,
			Data:  cumRets,
		})
	}
	var labels = make([]string, 0, maxXNum)
	curMS := startMS
	lay := core.DefaultDateFmt
	if int(tfMSecs/1000) >= utils2.TFToSecs("1d") {
		lay = "2006-01-02"
	}
	for i := 0; i < maxXNum; i++ {
		var dateStr string
		if deps == nil {
			dateStr = btime.ToDateStr(curMS, lay)
		} else {
			dateStr = deps.dateStr(curMS, lay)
		}
		labels = append(labels, dateStr)
		curMS += tfMSecs
	}
	return labels, result, nil
}

// 计算给定订单的累计收益曲线
func calcCumCurve(pairOrders map[string][]*ormo.InOutOrder, startMS, endMS int64, tf string, baseVal float64) ([]float64, *errs.Error) {
	return calcCumCurveWithDeps(pairOrders, startMS, endMS, tf, baseVal, nil)
}

func calcCumCurveWithDeps(pairOrders map[string][]*ormo.InOutOrder, startMS, endMS int64, tf string, baseVal float64, deps *ReportDeps) ([]float64, *errs.Error) {
	if deps != nil {
		if err := deps.validateSeries(); err != nil {
			return nil, err
		}
	}
	var glbRets []float64
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	pairs := make([]string, 0, len(pairOrders))
	for pair := range pairOrders {
		pairs = append(pairs, pair)
	}
	sort.Strings(pairs)
	for _, pair := range pairs {
		orders := pairOrders[pair]
		var exs *orm.ExSymbol
		var err *errs.Error
		if deps == nil {
			exs = orm.GetExSymbol2(core.ExgName, core.Market, pair)
		} else {
			exs, err = deps.symbol(pair)
			if err != nil {
				return nil, err
			}
		}
		var closes []float64
		if deps == nil {
			_, closes, err = getOHLCVNoLack(exs, tf, startMS, endMS, tfMSecs)
		} else {
			_, closes, err = getOHLCVNoLackWithDeps(deps, exs, tf, startMS, endMS, tfMSecs)
		}
		if err != nil {
			return nil, err
		}
		// 计算每日回报
		returns, _, _ := ormo.CalcUnitReturns(orders, closes, startMS, endMS, tfMSecs)
		if glbRets == nil {
			glbRets = returns
		} else {
			for i, v := range returns {
				glbRets[i] += v
			}
		}
	}
	// 计算累计回报
	var cumRets = make([]float64, len(glbRets))
	sumRet := baseVal
	for i, v := range glbRets {
		sumRet += v
		cumRets[i] = sumRet
	}
	return cumRets, nil
}

func getOHLCVNoLack(exs *orm.ExSymbol, tf string, startMS, endMS, tfMSecs int64) ([]*banexg.Kline, []float64, *errs.Error) {
	_, bars, err := orm.GetOHLCV(exs, tf, startMS, endMS, 0, false)
	return fillReportOHLCVLacks(bars, err, startMS, endMS, tfMSecs)
}

func getOHLCVNoLackWithDeps(deps *ReportDeps, exs *orm.ExSymbol, tf string, startMS, endMS, tfMSecs int64) ([]*banexg.Kline, []float64, *errs.Error) {
	if deps == nil {
		return getOHLCVNoLack(exs, tf, startMS, endMS, tfMSecs)
	}
	if exs == nil {
		return nil, nil, errs.NewMsg(core.ErrInvalidSymbol, "report symbol is required")
	}
	queries, release, err := deps.queries()
	if err != nil {
		return nil, nil, err
	}
	defer release()
	_, bars, err := queries.GetOHLCV(exs, tf, startMS, endMS, 0, false)
	return fillReportOHLCVLacks(bars, err, startMS, endMS, tfMSecs)
}

func fillReportOHLCVLacks(bars []*banexg.Kline, err *errs.Error, startMS, endMS, tfMSecs int64) ([]*banexg.Kline, []float64, *errs.Error) {
	if err != nil {
		return nil, nil, err
	}
	var closes []float64
	if len(bars) > 0 {
		bars, _ = utils.FillOHLCVLacks(bars, startMS, endMS, tfMSecs)
		closes = make([]float64, len(bars))
		for i, b := range bars {
			closes[i] = b.Close
		}
	}
	return bars, closes, nil
}

func calcBtResult(odList []*ormo.InOutOrder, funds map[string]float64, outDir string) (*BTResult, *errs.Error) {
	backUp := biz.BackupVars()
	biz.ResetVars()
	defer func() {
		biz.RestoreVars(backUp)
	}()
	return calcBtResultWithDeps(odList, funds, outDir, nil)
}

func calcBtResultWithRuntimeDeps(odList []*ormo.InOutOrder, funds map[string]float64, outDir string, deps biz.RuntimeDeps) (*BTResult, *errs.Error) {
	return calcBtResultWithDeps(odList, funds, outDir, NewReportDeps(deps))
}

func calcBtResultWithDeps(odList []*ormo.InOutOrder, funds map[string]float64, outDir string, deps *ReportDeps) (*BTResult, *errs.Error) {
	btRes := NewBTResult()
	btRes.reportDeps = deps
	if deps != nil {
		if err := deps.validateResult(); err != nil {
			return nil, err
		}
	}
	if len(odList) == 0 {
		return btRes, nil
	}
	var err *errs.Error
	var wallets *biz.BanWallets
	if deps == nil {
		wallets = biz.GetWallets(config.DefAcc)
	} else {
		wallets = biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	}
	if wallets == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "report wallet is required")
	}
	wallets.SetWallets(funds)
	wallets.TryUpdateStakePctAmt()
	totalLegal := wallets.TotalLegal(nil, false)
	if totalLegal == 0 {
		return nil, errs.NewMsg(errs.CodeRunTime, "TotalLegal of wallets is empty")
	}
	// 更新起止时间、最小周期
	var startMS = odList[0].RealEnterMS()
	var endMS = odList[0].RealExitMS()
	var tfSecs = utils2.TFToSecs(odList[0].Timeframe)
	pairOrders := make(map[string][]*ormo.InOutOrder)
	hitSlTp := 0
	for _, od := range odList {
		enterAt := od.RealEnterMS()
		if enterAt > 0 && enterAt < startMS {
			startMS = enterAt
		}
		endMS = max(endMS, od.RealExitMS())
		curSecs := utils2.TFToSecs(od.Timeframe)
		tfSecs = min(tfSecs, curSecs)
		items, _ := pairOrders[od.Symbol]
		pairOrders[od.Symbol] = append(items, od)
		if od.GetInfoString(ormo.OdInfoSLTP) == "yes" {
			hitSlTp += 1
		}
	}
	btRes.HitSlTp = hitSlTp
	tfMSecs := int64(tfSecs * 1000)
	startMS = utils2.AlignTfMSecs(startMS, tfMSecs)
	endMS = utils2.AlignTfMSecs(endMS, tfMSecs) + tfMSecs
	btRes.StartMS = startMS
	btRes.EndMS = endMS
	btRes.OrderNum = len(odList)
	tf := utils2.SecsToTF(tfSecs)
	// 获取各个品种信息
	var pairStats []*PairStat
	if deps == nil {
		pairStats, err = CalcPairStats(pairOrders, startMS, endMS, tf)
	} else {
		pairStats, err = calcPairStats(pairOrders, startMS, endMS, tf, deps)
	}
	if err != nil {
		return nil, err
	}
	// 检查计算结果是否正确
	numMap := make(map[int]int)
	failMap := make(map[string]float64)
	for _, sta := range pairStats {
		key := len(sta.KLines)
		if len(sta.Returns) != key {
			return nil, errs.NewMsg(errs.CodeRunTime, "%s len(Klines) != len(Returns)", sta.Symbol)
		}
		num, _ := numMap[key]
		numMap[key] = num + 1
		profitSum := float64(0)
		for _, od := range sta.Orders {
			profitSum += od.Profit
		}
		retSum := floats.Sum(sta.Returns)
		diffRate := math.Abs(profitSum-retSum) / max(profitSum, retSum)
		if diffRate > 0.03 {
			failMap[sta.Symbol] = math.Round(diffRate * 100)
		}
	}
	if len(numMap) > 1 {
		return nil, errs.NewMsg(errs.CodeRunTime, "pairStats len not same: %v", numMap)
	}
	if len(failMap) > 0 {
		log.Warn("Sum(Returns) and Sum(Profits) differs too much", zap.Any("pair pcts", failMap))
	}
	odList = reportReplayOrder(odList)
	if deps == nil {
		core.Pairs = utils.KeysOfMap(pairOrders)
	} else if deps.Core != nil {
		deps.Core.SetPairs(utils.KeysOfMap(pairOrders), nil)
	}
	btRes.logPlot(wallets, startMS, 0, totalLegal)
	btRes.MaxReal = totalLegal
	btRes.MinReal = totalLegal
	btRes.TotalInvest = totalLegal
	offsetMS := min(tfMSecs/2, 120000) // 定位到K线后120s，避免整点时订单尚未成交
	curAlignMS := startMS
	var openOds map[int64]*ormo.InOutOrder
	var lock *deadlock.Mutex
	if deps != nil {
		openOds, lock = deps.Orders.GetOpenODs(deps.account())
	} else {
		openOds, lock = ormo.GetOpenODs(config.DefAcc)
	}
	nextOd, failEnter := 0, 0
	for curAlignMS < endMS {
		barStartMs := curAlignMS
		curAlignMS += tfMSecs
		curMS := curAlignMS + offsetMS
		// 更新所有品种价格
		prices := make(map[string]float64)
		for _, sta := range pairStats {
			for sta.Idx < len(sta.KLines) {
				bar := sta.KLines[sta.Idx]
				if bar.Time < barStartMs {
					sta.Idx++
					continue
				}
				if bar.Time == barStartMs {
					sta.Idx++
					prices[sta.Symbol] = bar.Close
				}
				break
			}
		}
		if deps == nil {
			com.SetPrices(prices, "")
		} else {
			if deps.Clock != nil {
				deps.Clock.SetTimeMS(curMS)
			}
			deps.Market.Prices.SetPricesAt(curMS, prices, "")
		}
		// 更新当前持仓订单
		lock.Lock()
		openList := reportOpenOrderViewWithDeps(openOds, deps)
		lock.Unlock()
		for _, od := range openList {
			if od.RealExitMS() < curMS {
				wallets.ExitOd(od, od.Exit.Filled)
				wallets.ConfirmOdExit(od, od.Exit.Average)
				if deps == nil {
					ormo.HistODs = append(ormo.HistODs, od)
				} else {
					deps.Orders.AddHistoricalOrder(od)
				}
				lock.Lock()
				delete(openOds, od.ID)
				lock.Unlock()
			}
		}
		for nextOd < len(odList) {
			od := odList[nextOd]
			if od.RealEnterMS() < curMS {
				nextOd += 1
				if od.RealExitMS() > curMS {
					_, err = wallets.EnterOd(od)
					if err != nil {
						failEnter += 1
						continue
					}
					wallets.ConfirmOdEnter(od, od.Enter.Average)
					lock.Lock()
					openOds[od.ID] = od
					lock.Unlock()
				}
			} else {
				break
			}
		}
		// 更新状态
		lock.Lock()
		settleMap := make(map[string][]*ormo.InOutOrder)
		for _, od := range openOds {
			_, _, settle, _ := core.SplitSymbol(od.Symbol)
			arr, _ := settleMap[settle]
			settleMap[settle] = append(arr, od)
		}
		openNum := len(openOds)
		btRes.MaxOpenOrders = max(btRes.MaxOpenOrders, openNum)
		lock.Unlock()
		strict := false
		if deps == nil {
			strict = config.StrictBacktest()
		} else {
			strict = deps.strictBacktest()
		}
		for code := range utils.MapKeys(settleMap, strict) {
			odArr := settleMap[code]
			err = wallets.UpdateOds(odArr, code)
			if err != nil {
				return nil, err
			}
		}
		btRes.BarNum += len(prices)
		btRes.TimeNum += 1
		btRes.logState(barStartMs, curAlignMS, openNum)
	}
	if failEnter > 0 {
		log.Warn("skip enter failed orders", zap.Int("num", failEnter))
	}
	// 退出终止时尚未退出的订单
	lock.Lock()
	openList := reportOpenOrderViewWithDeps(openOds, deps)
	lock.Unlock()
	for _, od := range openList {
		wallets.ExitOd(od, od.Exit.Filled)
		wallets.ConfirmOdExit(od, od.Exit.Average)
		lock.Lock()
		delete(openOds, od.ID)
		lock.Unlock()
	}
	btRes.logState(curAlignMS, curAlignMS, 0)
	// 统计结果并输出
	if deps == nil {
		ormo.HistODs = odList
	} else {
		for _, od := range odList {
			deps.Orders.AddHistoricalOrder(od)
		}
	}
	btRes.Collect()
	log.Info("BackTest Reports:\n" + btRes.cmdReports(odList))
	if outDir != "" {
		btRes.OutDir = outDir
		btRes.dumpBtFiles(true)
		log.Info("Saved", zap.String("at", outDir))
	}
	if btRes.CalcDiff > 0.01 {
		log.Error("TotInvestment + TotProfit != FinalBalance, may be bug, please report on github",
			zap.Float64("total_invest", btRes.TotalInvest), zap.Float64("total_profit", btRes.TotProfit),
			zap.Float64("final_balance", btRes.FinBalance), zap.Float64("final_withdraw", btRes.FinWithdraw),
			zap.Float64("calc_diff", btRes.CalcDiff))
	}
	return btRes, nil
}

func reportOpenOrderView(orders map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	return reportOpenOrderViewWithDeps(orders, nil)
}

func reportOpenOrderViewWithDeps(orders map[int64]*ormo.InOutOrder, deps *ReportDeps) []*ormo.InOutOrder {
	result := utils2.ValsOfMap(orders)
	strict := false
	if deps == nil {
		strict = config.StrictBacktest()
	} else {
		strict = deps.strictBacktest()
	}
	if strict {
		slices.SortFunc(result, func(a, b *ormo.InOutOrder) int {
			if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
				return order
			}
			return cmp.Compare(a.ID, b.ID)
		})
	}
	return result
}

func reportReplayOrder(orders []*ormo.InOutOrder) []*ormo.InOutOrder {
	result := slices.Clone(orders)
	slices.SortStableFunc(result, func(a, b *ormo.InOutOrder) int {
		if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return result
}

type PairStat struct {
	*orm.ExSymbol
	Orders  []*ormo.InOutOrder
	KLines  []*banexg.Kline
	Returns []float64
	Idx     int
}

func CalcPairStats(pairOrders map[string][]*ormo.InOutOrder, startMS, endMS int64, tf string) ([]*PairStat, *errs.Error) {
	return calcPairStats(pairOrders, startMS, endMS, tf, nil)
}

// CalcPairStatsWithRuntimeDeps resolves symbols and series through one
// explicit runtime. It never consults the package-level ORM catalog.
func CalcPairStatsWithRuntimeDeps(pairOrders map[string][]*ormo.InOutOrder, startMS, endMS int64, tf string, deps biz.RuntimeDeps) ([]*PairStat, *errs.Error) {
	return calcPairStats(pairOrders, startMS, endMS, tf, NewReportDeps(deps))
}

func calcPairStats(pairOrders map[string][]*ormo.InOutOrder, startMS, endMS int64, tf string, deps *ReportDeps) ([]*PairStat, *errs.Error) {
	if deps != nil {
		if err := deps.validateSeries(); err != nil {
			return nil, err
		}
	}
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	var result = make([]*PairStat, 0, len(pairOrders))
	pairs := make([]string, 0, len(pairOrders))
	for pair := range pairOrders {
		pairs = append(pairs, pair)
	}
	sort.Strings(pairs)
	for _, pair := range pairs {
		orders := pairOrders[pair]
		var exs *orm.ExSymbol
		var err *errs.Error
		if deps == nil {
			exs = orm.GetExSymbol2(core.ExgName, core.Market, pair)
		} else {
			exs, err = deps.symbol(pair)
			if err != nil {
				return nil, err
			}
		}
		var bars []*banexg.Kline
		var closes []float64
		if deps == nil {
			bars, closes, err = getOHLCVNoLack(exs, tf, startMS, endMS, tfMSecs)
		} else {
			bars, closes, err = getOHLCVNoLackWithDeps(deps, exs, tf, startMS, endMS, tfMSecs)
		}
		if err != nil {
			return nil, err
		}
		// 计算每日回报
		returns, _, _ := ormo.CalcUnitReturns(orders, closes, startMS, endMS, tfMSecs)
		result = append(result, &PairStat{
			Orders:   orders,
			ExSymbol: exs,
			KLines:   bars,
			Returns:  returns,
		})
	}
	return result, nil
}

type TimeVal struct {
	Time  int64
	Value float64
}

/*
SampleOdNums
对一系列订单在整个时间范围的每个时间节点采样计算
*/
func SampleOdNums(odList []*ormo.InOutOrder, num int) ([]int, int64, int64) {
	if len(odList) == 0 {
		return make([]int, num), 0, 0
	}
	sort.Slice(odList, func(i, j int) bool {
		return odList[i].RealEnterMS() < odList[j].RealEnterMS()
	})
	nums := make([]int, num)
	minTimeMS, maxTimeMS := odList[0].RealEnterMS(), int64(0)
	for _, od := range odList {
		maxTimeMS = max(od.RealExitMS(), maxTimeMS)
	}
	gapMS := (maxTimeMS - minTimeMS) / int64(num)
	if gapMS == 0 {
		return make([]int, num), 0, 0
	}
	for _, od := range odList {
		startIdx := int((od.RealEnterMS() - minTimeMS) / gapMS)
		endPos := len(nums)
		if od.RealExitMS() > 0 {
			endPos = int(math.Ceil(float64(od.RealExitMS()-minTimeMS)/float64(gapMS))) + 1
			endPos = min(len(nums), endPos)
		}
		for i := startIdx; i < endPos; i++ {
			nums[i] += 1
		}
	}
	return nums, minTimeMS, maxTimeMS
}

func sortTfMap(intMap map[string]int) string {
	arr := make([]*core.StrInt64, 0, len(intMap))
	for k, v := range intMap {
		arr = append(arr, &core.StrInt64{
			Str: k, Int: int64(v),
		})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].Int != arr[j].Int {
			return arr[i].Int > arr[j].Int
		}
		return arr[i].Str < arr[j].Str
	})
	tfArr := make([]string, len(arr))
	for i, v := range arr {
		tfArr[i] = v.Str
	}
	return strings.Join(tfArr, "/")
}
