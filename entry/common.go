package entry

import (
	"fmt"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
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
	factory CommandFactory
}

// CommandFactory constructs a fresh Cobra command for one root tree.
type CommandFactory func() *cobra.Command

var (
	commandGroups = []commandGroup{
		{name: "data", help: "export and import data"},
		{name: "kline", help: "manage kline data"},
		{name: "series", help: "manage custom series data"},
		{name: "tick", help: "manage tick data"},
		{name: "tool", help: "run maintenance and analysis tools"},
		{name: "live", help: "manage live orders"},
	}
	extraGroups       []commandGroup
	extraCommands     []registeredCommand
	commandRegistryMu sync.RWMutex
	rootCommandMu     sync.Mutex
)

// AddGroup registers a Cobra command group for applications embedding banbot.
func AddGroup(name, help string) {
	if name == "" {
		panic("command group name must not be empty")
	}
	commandRegistryMu.Lock()
	defer commandRegistryMu.Unlock()
	if hasGroupLocked(name) {
		return
	}
	extraGroups = append(extraGroups, commandGroup{name: name, help: help})
}

// AddCommand registers a Cobra command in the process-wide command registry.
// Command-specific flags should be local variables captured by RunE, so
// extending the CLI does not require CmdArgs changes.
func AddCommand(parent string, command *cobra.Command) {
	registerCommand(parent, command)
}

// AddCommandFactory registers an extension that can be materialized safely in
// multiple root trees. Prefer it when an embedding process builds more than
// one root or builds roots concurrently.
func AddCommandFactory(parent string, factory CommandFactory) {
	if factory == nil {
		panic("command factory must not be nil")
	}
	commandRegistryMu.Lock()
	defer commandRegistryMu.Unlock()
	if parent != "" && !hasGroupLocked(parent) {
		panic(fmt.Sprintf("no command group found: %s", parent))
	}
	extraCommands = append(extraCommands, registeredCommand{parent: parent, factory: factory})
}

func registerCommand(parent string, command *cobra.Command) {
	if command == nil {
		panic("command must not be nil")
	}
	commandRegistryMu.Lock()
	defer commandRegistryMu.Unlock()
	if parent != "" && !hasGroupLocked(parent) {
		panic(fmt.Sprintf("no command group found: %s", parent))
	}
	extraCommands = append(extraCommands, registeredCommand{parent: parent, command: command})
}

func hasGroup(name string) bool {
	commandRegistryMu.RLock()
	defer commandRegistryMu.RUnlock()
	return hasGroupLocked(name)
}

func hasGroupLocked(name string) bool {
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

func commandRegistrySnapshot() ([]commandGroup, []registeredCommand) {
	commandRegistryMu.RLock()
	defer commandRegistryMu.RUnlock()
	return append([]commandGroup(nil), extraGroups...), append([]registeredCommand(nil), extraCommands...)
}

func registerBuiltInCommands(root *cobra.Command, groups map[string]*cobra.Command) {
	add := func(parent string, command *cobra.Command) {
		if parent == "" {
			root.AddCommand(command)
			return
		}
		groups[parent].AddCommand(command)
	}

	add("", newRuntimeConfigCommand("trade", "live trade", runTradeEntry, false,
		bindStakeAmount, bindPairs, bindSpider, bindOut))
	add("", newInternalCommand())
	add("", newRuntimeConfigCommand("backtest", "backtest with strategies and data", runBackTestEntry, true,
		bindOut, bindTimeRange, bindTimeStart, bindTimeEnd, bindStakeAmount, bindPairs, bindProgress, bindSeparate, bindBTStrict))
	add("", newRuntimeConfigCommand("spider", "start the spider", runSpider, false))
	add("", newRuntimeConfigCommand("optimize", "run hyperparameter optimization", func(args *config.CmdArgs) *errs.Error { return runExplicitOptimization(args, opt.RunOptimize) }, true,
		bindOut, bindOptRounds, bindSampler, bindPicker, bindEachPairs, bindConcur, bindBTStrict))
	add("", newRuntimeConfigCommand("init", "initialize config.yml/config.local.yml in the data directory", runInit, true))
	add("", withAliases(newRuntimeConfigCommand("bt-opt", "run rolling backtests with hyperparameter optimization", func(args *config.CmdArgs) *errs.Error { return runExplicitOptimization(args, opt.RunBTOverOpt) }, true,
		bindReviewPeriod, bindRunPeriod, bindOptRounds, bindSampler, bindPicker, bindEachPairs,
		bindConcur, bindAlpha, bindPairPicker, bindBTStrict), "bt_opt"))
	add("", web.NewDevCommandWithFactory(newDevWebServer))

	add("data", newRuntimeConfigCommand("export", "export data from the database to protobuf files", runDataExport, true,
		bindOut, bindConcur))
	add("data", newRuntimeConfigCommand("import", "import protobuf files into the database", runDataImport, true,
		bindIn, bindConcur))

	add("kline", newRuntimeConfigCommand("down", "download kline data from an exchange", RunDownData, true,
		bindTimeRange, bindTimeStart, bindTimeEnd, bindPairs, bindTimeFrames, bindMedium))
	add("kline", newRuntimeConfigCommand("repair-ranges", "rebuild kline range metadata from stored bars", RunRepairKlineRanges, true,
		bindTimeRange, bindTimeStart, bindTimeEnd, bindPairs, bindTimeFrames))
	add("kline", newRuntimeConfigCommand("load", "load kline data from zip or CSV files", LoadKLinesToDB, true, bindIn))
	add("kline", newRuntimeConfigCommand("agg", "aggregate kline data into larger timeframes", AggKlineBigs, true,
		bindPairs, bindTimeFrames))
	add("kline", newRuntimeConfigCommand("export", "export kline data from the database to CSV files", runExportData, true,
		bindOut, bindPairs, bindTimeFrames, bindAdjustment, bindTimeZone))
	add("kline", newRuntimeConfigCommand("purge", "delete matching kline data", runPurgeData, true,
		bindRealExchange, bindPairs, bindTimeFrames))
	add("kline", newRuntimeConfigCommand("correct", "synchronize klines between timeframes", runKlineCorrect, true, bindPairs))
	add("kline", newRuntimeConfigCommand("verify", "verify kline data against series-range metadata", RunVerifyData, true,
		bindPairs, bindTables, bindBatchSize))
	add("kline", withAliases(newRuntimeConfigCommand("adj-calc", "recalculate adjustment factors", runKlineAdjFactors, true,
		bindOut, bindPairs), "adj_calc"))
	add("kline", withAliases(newRuntimeConfigCommand("adj-export", "export adjustment factors to CSV", runExportAdjFactors, true,
		bindOut, bindPairs, bindTimeZone), "adj_export"))

	add("series", newRuntimeConfigCommand("down", "download registered custom series", RunSeriesDown, true,
		bindTimeRange, bindTimeStart, bindTimeEnd, bindPairs, bindSeriesSources))
	add("series", newSeriesListCommand())

	add("tick", newRuntimeConfigCommand("convert", "convert tick data formats", data.RunFormatTick, true, bindIn, bindOut))
	add("tick", withAliases(newRuntimeConfigCommand("to-kline", "build klines from tick data", data.Build1mWithTicks, true, bindIn, bindOut), "to_kline"))

	add("tool", withAliases(newRuntimeConfigCommand("collect-opt", "collect and rank optimization results", func(args *config.CmdArgs) *errs.Error { return runExplicitOptimization(args, opt.CollectOptLog) }, true,
		bindIn, bindPicker), "collect_opt"))
	add("tool", withAliases(newRuntimeConfigCommand("sim-bt", "run a backtest simulation from a report", runExplicitSimulation, true,
		bindIn, bindBTStrict), "sim_bt"))
	add("tool", withAliases(newRuntimeConfigCommand("test-pickers", "test pickers in rolling backtests", func(args *config.CmdArgs) *errs.Error { return runExplicitOptimization(args, opt.RunRollBTPicker) }, true,
		bindReviewPeriod, bindRunPeriod, bindOptRounds, bindSampler, bindEachPairs, bindConcur, bindPicker, bindPairPicker,
		bindBTStrict), "test_pickers"))
	add("tool", withAliases(newRuntimeConfigCommand("load-cal", "load calendars", runExplicitLoadCalendars, true, bindIn), "load_cal"))
	add("tool", withAliases(newRuntimeConfigCommand("data-server", "serve a gRPC data feeder", runExplicitDataServer, true), "data_server"))
	add("tool", withAliases(newRuntimeConfigCommand("calc-perfs", "calculate Sharpe and Sortino ratios for input data", data.CalcFilePerfs, true,
		bindIn, bindInType, bindOut), "calc_perfs"))
	add("tool", newRuntimeConfigCommand("corr", "calculate a symbol correlation matrix", runExplicitCorrelation, true,
		bindOut, bindOutType, bindTimeFrames, bindBatchSize, bindRunEvery))
	add("tool", newMergeAssetsCommand())
	add("tool", NewRuntimeCompareExgBTOrdersCommand())
	add("tool", strat.NewListStratsCommand())
	add("tool", opt.NewBtFactorsCommandWithRun(runExplicitFactors))
	add("tool", withAliases(newRuntimeConfigCommand("bt-result", "build a backtest result from orders.gob and config", runExplicitReport, true,
		bindIn, bindOut, bindBTStrict), "bt_result"))
	add("tool", NewRuntimeKlineConsistencyCommand())

	add("live", NewRuntimeDownExgOrdersCommand())
	add("live", NewRuntimeTradeCloseCommand())
}

func withAliases(command *cobra.Command, aliases ...string) *cobra.Command {
	command.Aliases = append(command.Aliases, aliases...)
	return command
}

func registerExtraCommands(root *cobra.Command, groups map[string]*cobra.Command, commands []registeredCommand) {
	for _, item := range commands {
		command := item.command
		if item.factory != nil {
			command = item.factory()
		}
		if command == nil {
			panic("command factory returned nil")
		}
		if command.Parent() != nil {
			panic(fmt.Sprintf("command %q is already attached; use AddCommandFactory when building multiple roots", command.Name()))
		}
		if item.parent == "" {
			root.AddCommand(command)
			continue
		}
		groups[item.parent].AddCommand(command)
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

func bindSeriesSources(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.StringVar(&args.RawTables, "tables", "", "comma-separated registered source names")
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

func bindBTStrict(args *config.CmdArgs, flags *pflag.FlagSet) {
	flags.BoolVar(&args.BTStrict, "bt-strict", false, "enable strict backtest mode")
}
