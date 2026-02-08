package entry

import (
	"fmt"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/strat"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg/errs"
)

type FuncEntry = func(args *config.CmdArgs) *errs.Error
type FuncGetEntry = func(name string) (FuncEntry, []string)

var (
	groups   = make([]string, 0, 5)
	groupMap = make(map[string]*JobGroup)
)

type CmdJob struct {
	Name      string
	Parent    string
	Run       FuncEntry
	Options   []string
	NoOptions []string // dlock
	RunRaw    func(args []string) error
	Help      string
}

type JobGroup struct {
	Name string
	Help string
	Jobs map[string]*CmdJob
}

func AddGroup(name, help string) {
	if _, ok := groupMap[name]; !ok {
		groupMap[name] = &JobGroup{
			Name: name,
			Help: help,
			Jobs: make(map[string]*CmdJob),
		}
		groups = append(groups, name)
	}
}

func AddCmdJob(job *CmdJob) {
	gp, ok := groupMap[job.Parent]
	if !ok {
		panic(fmt.Sprint("no cmd group found: ", job.Parent))
	}
	gp.Jobs[job.Name] = job
}

func init() {
	// Initialize root command group
	AddGroup("", "Root Commands")
	AddGroup("data", "run data export/import")
	AddGroup("kline", "run kline commands")
	AddGroup("tick", "run tick commands")
	AddGroup("tool", "run tools commands")
	AddGroup("live", "run live order manager commands")

	// Root command group
	AddCmdJob(&CmdJob{
		Name:      "trade",
		Run:       RunTrade,
		Options:   []string{"stake_amount", "pairs", "with_spider", "out"},
		NoOptions: []string{"dlock"},
		Help:      "live trade",
	})
	AddCmdJob(&CmdJob{
		Name:    "backtest",
		Run:     RunBackTest,
		Options: []string{"out", "timerange", "timestart", "timeend", "stake_amount", "pairs", "prg", "separate"},
		Help:    "backtest with strategies and data",
	})
	AddCmdJob(&CmdJob{
		Name:      "spider",
		Run:       RunSpider,
		NoOptions: []string{"dlock"},
		Help:      "start the spider",
	})
	AddCmdJob(&CmdJob{
		Name:    "optimize",
		Run:     opt.RunOptimize,
		Options: []string{"out", "opt_rounds", "sampler", "picker", "each_pairs", "concur"},
		Help:    "run hyper parameters optimization",
	})
	AddCmdJob(&CmdJob{
		Name:    "init",
		Run:     runInit,
		Options: nil,
		Help:    "initialize config.yml/config.local.yml in BanDataDir",
	})
	AddCmdJob(&CmdJob{
		Name: "bt_opt",
		Run:  opt.RunBTOverOpt,
		Options: []string{"review_period", "run_period", "opt_rounds", "sampler", "picker", "each_pairs",
			"concur", "alpha", "pair_picker"},
		Help: "rolling backtest with hyperparameter optimization",
	})
	AddCmdJob(&CmdJob{
		Name:   "web",
		RunRaw: web.RunDev,
		Help:   "run web ui",
	})

	// data command group
	AddCmdJob(&CmdJob{
		Name:    "export",
		Parent:  "data",
		Run:     runDataExport,
		Options: []string{"out", "concur"},
		Help:    "export data to protobuf file from db",
	})
	AddCmdJob(&CmdJob{
		Name:    "import",
		Parent:  "data",
		Run:     runDataImport,
		Options: []string{"in", "concur"},
		Help:    "import data from protobuf files to db",
	})

	// kline command group
	AddCmdJob(&CmdJob{
		Name:    "down",
		Parent:  "kline",
		Run:     RunDownData,
		Options: []string{"timerange", "timestart", "timeend", "pairs", "timeframes", "medium"},
		Help:    "download kline data from exchange",
	})
	AddCmdJob(&CmdJob{
		Name:    "load",
		Parent:  "kline",
		Run:     LoadKLinesToDB,
		Options: []string{"in"},
		Help:    "load kline data from zip/csv files",
	})
	AddCmdJob(&CmdJob{
		Name:    "agg",
		Parent:  "kline",
		Run:     AggKlineBigs,
		Options: []string{"pairs", "timeframes"},
		Help:    "agg kline for big timeframes",
	})
	AddCmdJob(&CmdJob{
		Name:    "export",
		Parent:  "kline",
		Run:     runExportData,
		Options: []string{"out", "pairs", "timeframes", "adj", "tz"},
		Help:    "export kline to csv files from db",
	})
	AddCmdJob(&CmdJob{
		Name:    "purge",
		Parent:  "kline",
		Run:     runPurgeData,
		Options: []string{"exg_real", "pairs", "timeframes"},
		Help:    "purge/delete kline data with args",
	})
	AddCmdJob(&CmdJob{
		Name:    "correct",
		Parent:  "kline",
		Run:     RunKlineCorrect,
		Options: []string{"pairs"},
		Help:    "sync klines between timeframes",
	})
	AddCmdJob(&CmdJob{
		Name:    "verify",
		Parent:  "kline",
		Run:     RunVerifyData,
		Options: []string{"pairs", "tables", "batch_size"},
		Help:    "verify kline data matches sranges metadata",
	})
	AddCmdJob(&CmdJob{
		Name:    "adj_calc",
		Parent:  "kline",
		Run:     RunKlineAdjFactors,
		Options: []string{"out", "pairs"},
		Help:    "recalculate adjust factors",
	})
	AddCmdJob(&CmdJob{
		Name:    "adj_export",
		Parent:  "kline",
		Run:     biz.ExportAdjFactors,
		Options: []string{"out", "pairs", "tz"},
		Help:    "export adjust factors to csv",
	})

	// tick command group
	AddCmdJob(&CmdJob{
		Name:    "convert",
		Parent:  "tick",
		Run:     data.RunFormatTick,
		Options: []string{"in", "out"},
		Help:    "convert tick data format",
	})
	AddCmdJob(&CmdJob{
		Name:    "to_kline",
		Parent:  "tick",
		Run:     data.Build1mWithTicks,
		Options: []string{"in", "out"},
		Help:    "build kline from ticks",
	})

	// tool command group
	AddCmdJob(&CmdJob{
		Name:    "collect_opt",
		Parent:  "tool",
		Run:     opt.CollectOptLog,
		Options: []string{"in", "picker"},
		Help:    "collect result of optimize, and print in order",
	})
	AddCmdJob(&CmdJob{
		Name:    "sim_bt",
		Parent:  "tool",
		Run:     opt.RunSimBT,
		Options: []string{"in"},
		Help:    "run backtest simulation from report",
	})
	AddCmdJob(&CmdJob{
		Name:   "test_pickers",
		Parent: "tool",
		Run:    opt.RunRollBTPicker,
		Options: []string{"review_period", "run_period", "opt_rounds", "sampler", "each_pairs", "concur",
			"picker", "pair_picker"},
		Help: "test pickers in roll backtest",
	})
	AddCmdJob(&CmdJob{
		Name:    "load_cal",
		Parent:  "tool",
		Run:     biz.LoadCalendars,
		Options: []string{"in"},
		Help:    "load calenders",
	})
	AddCmdJob(&CmdJob{
		Name:    "data_server",
		Parent:  "tool",
		Run:     biz.RunDataServer,
		Options: nil,
		Help:    "serve a grpc server as data feeder",
	})
	AddCmdJob(&CmdJob{
		Name:    "calc_perfs",
		Parent:  "tool",
		Run:     data.CalcFilePerfs,
		Options: []string{"in", "in_type", "out"},
		Help:    "calculate sharpe/sortino ratio for input data",
	})
	AddCmdJob(&CmdJob{
		Name:    "corr",
		Parent:  "tool",
		Run:     biz.CalcCorrelation,
		Options: []string{"out", "out_type", "timeframes", "batch_size", "run_every"},
		Help:    "calculate correlation matrix for symbols",
	})
	AddCmdJob(&CmdJob{
		Name:   "merge_assets",
		Parent: "tool",
		RunRaw: runMergeAssets,
		Help:   "merge multiple assets.html files into one",
	})
	AddCmdJob(&CmdJob{
		Name:   "cmp_orders",
		Parent: "tool",
		RunRaw: opt.CompareExgBTOrders,
		Help:   "compare exchange orders with backtest",
	})
	AddCmdJob(&CmdJob{
		Name:   "list_strats",
		Parent: "tool",
		RunRaw: strat.ListStrats,
		Help:   "list registered strategies",
	})
	AddCmdJob(&CmdJob{
		Name:   "bt_factor",
		Parent: "tool",
		RunRaw: opt.BtFactors,
		Help:   "backtest factors with orders",
	})
	AddCmdJob(&CmdJob{
		Name:    "bt_result",
		Parent:  "tool",
		Run:     opt.BuildBtResult,
		Options: []string{"in", "out"},
		Help:    "build backtest result from orders.gob and config",
	})
	AddCmdJob(&CmdJob{
		Name:   "test_live_bars",
		Parent: "tool",
		RunRaw: biz.TestKLineConsistency,
		Help:   "test kline bars from live trade with local klines",
	})

	AddCmdJob(&CmdJob{
		Name:   "down_order",
		Parent: "live",
		RunRaw: biz.DownExgOrders,
		Help:   "download exchange order for account in specified exchange",
	})
	AddCmdJob(&CmdJob{
		Name:   "close_order",
		Parent: "live",
		RunRaw: live.RunTradeClose,
		Help:   "close orders with account/pair/strategy",
	})
}

// GetCmdJob get command job by name and parent
func GetCmdJob(name, parent string) *CmdJob {
	if gp, ok := groupMap[parent]; ok {
		if job, ok := gp.Jobs[name]; ok {
			return job
		}
	}
	return nil
}

// GetGroup get command group by name
func GetGroup(name string) *JobGroup {
	gp, _ := groupMap[name]
	return gp
}
