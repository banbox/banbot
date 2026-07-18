package entry

import (
	"fmt"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type FuncEntry = func(args *config.CmdArgs) *errs.Error
type flagBinder func(args *config.CmdArgs, flags *pflag.FlagSet)

type commandGroup struct {
	name string
	help string
}

type registeredCommand struct {
	parent  string
	command *cobra.Command
}

var (
	commandGroups = []commandGroup{
		{name: "data", help: "export and import data"},
		{name: "kline", help: "manage kline data"},
		{name: "tick", help: "manage tick data"},
		{name: "tool", help: "run maintenance and analysis tools"},
		{name: "live", help: "manage live orders"},
	}
	extraGroups   []commandGroup
	extraCommands []registeredCommand
)

// AddGroup registers a Cobra command group for applications embedding banbot.
func AddGroup(name, help string) {
	if name == "" {
		panic("command group name must not be empty")
	}
	if hasGroup(name) {
		return
	}
	extraGroups = append(extraGroups, commandGroup{name: name, help: help})
}

// AddCommand registers a Cobra command. Command-specific flags should be local
// variables captured by RunE, so extending the CLI does not require CmdArgs changes.
func AddCommand(parent string, command *cobra.Command) {
	if command == nil {
		panic("command must not be nil")
	}
	if parent != "" && !hasGroup(parent) {
		panic(fmt.Sprintf("no command group found: %s", parent))
	}
	extraCommands = append(extraCommands, registeredCommand{parent: parent, command: command})
}

func hasGroup(name string) bool {
	for _, group := range commandGroups {
		if group.name == name {
			return true
		}
	}
	for _, group := range extraGroups {
		if group.name == name {
			return true
		}
	}
	return false
}

func registerBuiltInCommands(root *cobra.Command, groups map[string]*cobra.Command) {
	add := func(parent string, command *cobra.Command) {
		if parent == "" {
			root.AddCommand(command)
			return
		}
		groups[parent].AddCommand(command)
	}

	add("", newConfigCommand("trade", "live trade", RunTrade, false,
		bindStakeAmount, bindPairs, bindSpider, bindOut))
	add("", newConfigCommand("backtest", "backtest with strategies and data", RunBackTest, true,
		bindOut, bindTimeRange, bindTimeStart, bindTimeEnd, bindStakeAmount, bindPairs, bindProgress, bindSeparate))
	add("", newConfigCommand("spider", "start the spider", RunSpider, false))
	add("", newConfigCommand("optimize", "run hyperparameter optimization", opt.RunOptimize, true,
		bindOut, bindOptRounds, bindSampler, bindPicker, bindEachPairs, bindConcur))
	add("", newConfigCommand("init", "initialize config.yml/config.local.yml in the data directory", runInit, true))
	add("", withAliases(newConfigCommand("bt-opt", "run rolling backtests with hyperparameter optimization", opt.RunBTOverOpt, true,
		bindReviewPeriod, bindRunPeriod, bindOptRounds, bindSampler, bindPicker, bindEachPairs,
		bindConcur, bindAlpha, bindPairPicker), "bt_opt"))
	add("", web.NewCommand())

	add("data", newConfigCommand("export", "export data from the database to protobuf files", runDataExport, true,
		bindOut, bindConcur))
	add("data", newConfigCommand("import", "import protobuf files into the database", runDataImport, true,
		bindIn, bindConcur))

	add("kline", newConfigCommand("down", "download kline data from an exchange", RunDownData, true,
		bindTimeRange, bindTimeStart, bindTimeEnd, bindPairs, bindTimeFrames, bindMedium))
	add("kline", newConfigCommand("load", "load kline data from zip or CSV files", LoadKLinesToDB, true, bindIn))
	add("kline", newConfigCommand("agg", "aggregate kline data into larger timeframes", AggKlineBigs, true,
		bindPairs, bindTimeFrames))
	add("kline", newConfigCommand("export", "export kline data from the database to CSV files", runExportData, true,
		bindOut, bindPairs, bindTimeFrames, bindAdjustment, bindTimeZone))
	add("kline", newConfigCommand("purge", "delete matching kline data", runPurgeData, true,
		bindRealExchange, bindPairs, bindTimeFrames))
	add("kline", newConfigCommand("correct", "synchronize klines between timeframes", RunKlineCorrect, true, bindPairs))
	add("kline", newConfigCommand("verify", "verify kline data against series-range metadata", RunVerifyData, true,
		bindPairs, bindTables, bindBatchSize))
	add("kline", withAliases(newConfigCommand("adj-calc", "recalculate adjustment factors", RunKlineAdjFactors, true,
		bindOut, bindPairs), "adj_calc"))
	add("kline", withAliases(newConfigCommand("adj-export", "export adjustment factors to CSV", biz.ExportAdjFactors, true,
		bindOut, bindPairs, bindTimeZone), "adj_export"))

	add("tick", newConfigCommand("convert", "convert tick data formats", data.RunFormatTick, true, bindIn, bindOut))
	add("tick", withAliases(newConfigCommand("to-kline", "build klines from tick data", data.Build1mWithTicks, true, bindIn, bindOut), "to_kline"))

	add("tool", withAliases(newConfigCommand("collect-opt", "collect and rank optimization results", opt.CollectOptLog, true,
		bindIn, bindPicker), "collect_opt"))
	add("tool", withAliases(newConfigCommand("sim-bt", "run a backtest simulation from a report", opt.RunSimBT, true, bindIn), "sim_bt"))
	add("tool", withAliases(newConfigCommand("test-pickers", "test pickers in rolling backtests", opt.RunRollBTPicker, true,
		bindReviewPeriod, bindRunPeriod, bindOptRounds, bindSampler, bindEachPairs, bindConcur, bindPicker, bindPairPicker), "test_pickers"))
	add("tool", withAliases(newConfigCommand("load-cal", "load calendars", biz.LoadCalendars, true, bindIn), "load_cal"))
	add("tool", withAliases(newConfigCommand("data-server", "serve a gRPC data feeder", biz.RunDataServer, true), "data_server"))
	add("tool", withAliases(newConfigCommand("calc-perfs", "calculate Sharpe and Sortino ratios for input data", data.CalcFilePerfs, true,
		bindIn, bindInType, bindOut), "calc_perfs"))
	add("tool", newConfigCommand("corr", "calculate a symbol correlation matrix", biz.CalcCorrelation, true,
		bindOut, bindOutType, bindTimeFrames, bindBatchSize, bindRunEvery))
	add("tool", newMergeAssetsCommand())
	add("tool", opt.NewCompareExgBTOrdersCommand())
	add("tool", strat.NewListStratsCommand())
	add("tool", opt.NewBtFactorsCommand())
	add("tool", withAliases(newConfigCommand("bt-result", "build a backtest result from orders.gob and config", opt.BuildBtResult, true,
		bindIn, bindOut), "bt_result"))
	add("tool", withAliases(newPositionalCommand("test-live-bars", "compare live-trade klines with local data", "DUMP_FILE", biz.TestKLineConsistency), "test_live_bars"))

	add("live", biz.NewDownExgOrdersCommand())
	add("live", live.NewTradeCloseCommand())
}

func withAliases(command *cobra.Command, aliases ...string) *cobra.Command {
	command.Aliases = append(command.Aliases, aliases...)
	return command
}

func registerExtraCommands(root *cobra.Command, groups map[string]*cobra.Command) {
	for _, item := range extraCommands {
		if item.parent == "" {
			root.AddCommand(item.command)
			continue
		}
		groups[item.parent].AddCommand(item.command)
	}
}

func bindStakeAmount(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.Float64Var(&args.StakeAmount, "stake-amount", 0, "override stake_amount in config")
}

func bindPairs(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RawPairs, "pairs", "", "comma-separated pairs")
}

func bindSpider(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.BoolVar(&args.WithSpider, "spider", false, "start the spider if it is not running")
}

func bindTimeRange(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.TimeRange, "timerange", "", "time range")
}

func bindTimeStart(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.TimeStart, "timestart", "", "start time in a supported time format")
}

func bindTimeEnd(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.TimeEnd, "timeend", "", "end time; requires --timestart")
}

func bindTimeFrames(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RawTimeFrames, "timeframes", "", "comma-separated timeframes")
}

func bindMedium(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.Medium, "medium", "", "data medium: db or file")
}

func bindTables(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RawTables, "tables", "", "comma-separated database tables")
}

func bindProgress(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.PrgOut, "prg", "", "prefix for progress output")
}

func bindIn(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.InPath, "in", "", "input file or directory")
}

func bindInType(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.InType, "in-type", "", "input data type")
}

func bindOut(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.OutPath, "out", "", "output file or directory")
}

func bindOutType(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.OutType, "out-type", "", "output data type")
}

func bindAdjustment(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.AdjType, "adj", "", "kline adjustment: pre, post, or none")
}

func bindTimeZone(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.TimeZone, "tz", "", "timezone; defaults to UTC")
}

func bindRealExchange(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.ExgReal, "exg-real", "", "real exchange identifier")
}

func bindOptRounds(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.IntVar(&args.OptRounds, "opt-rounds", 30, "rounds per optimization job")
}

func bindSampler(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.Sampler, "sampler", "bayes", "optimization method: tpe, bayes, random, cmaes, ipop-cmaes, or bipop-cmaes")
}

func bindPicker(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.Picker, "picker", "good3", "method for selecting targets from optimization results")
}

func bindAlpha(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.Float64Var(&args.Alpha, "alpha", 1, "EMA smoothing factor for hyperparameter optimization")
}

func bindPairPicker(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.PairPicker, "pair-picker", "", "pair picker for hyperparameter optimization")
}

func bindEachPairs(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.BoolVar(&args.EachPairs, "each-pairs", false, "run once for each pair")
}

func bindConcur(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.IntVar(&args.Concur, "concur", 1, "number of concurrent jobs")
}

func bindReviewPeriod(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.ReviewPeriod, "review-period", "3y", "optimization review period")
}

func bindRunPeriod(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RunPeriod, "run-period", "6M", "effective run period after optimization")
}

func bindBatchSize(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.IntVar(&args.BatchSize, "batch-size", 0, "task batch size")
}

func bindRunEvery(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RunEveryTF, "run-every", "", "interval between runs")
}

func bindSeparate(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.BoolVar(&args.Separate, "separate", false, "backtest each policy separately")
}
