package entry

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type explicitEntrySession struct {
	process        *runtime.Process
	ctx            context.Context
	cancel         context.CancelFunc
	exchange       banexg.BanExchange
	storage        *orm.Storage
	netDisable     bool
	cpuProfile     bool
	memProfile     bool
	logArgs        config.CmdArgs
	logger         *zap.Logger
	closeLogger    func()
	profileMu      sync.Mutex
	exchangeMu     sync.Mutex
	profileStarted bool
	profileStop    func()
}

func openExplicitEntrySession(args *config.CmdArgs) (*explicitEntrySession, *config.Snapshot, *errs.Error) {
	if args == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "command arguments are required")
	}
	snapshot, err := config.LoadRuntimeSnapshot(args)
	if err != nil {
		return nil, nil, err
	}
	cfg := snapshot.View()
	if cfg == nil || cfg.Database == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "database config is required")
	}
	if args.DeadLock {
		deadlock.Opts.Disable = false
	}
	logArgs := *args
	if logArgs.Logfile != "" {
		logArgs.Logfile = snapshot.ParsePath(logArgs.Logfile)
	}
	logger, closeLogger, logErr := openEntryLogger(logArgs)
	if logErr != nil {
		return nil, nil, logErr
	}
	opened := false
	defer func() {
		if !opened {
			closeLogger()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	storage, err := orm.OpenStorage(ctx, cfg.Database, snapshot.DataDir)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	opened = true
	return &explicitEntrySession{
		process:     runtime.NewProcess(),
		ctx:         ctx,
		cancel:      cancel,
		storage:     storage,
		netDisable:  args.NetDisable,
		cpuProfile:  args.CPUProfile,
		memProfile:  args.MemProfile,
		logArgs:     logArgs,
		logger:      logger,
		closeLogger: closeLogger,
	}, snapshot, nil
}

func (s *explicitEntrySession) ensureExchange(snapshot *config.Snapshot, mode string) *errs.Error {
	if s == nil || snapshot == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime session is not configured")
	}
	s.exchangeMu.Lock()
	defer s.exchangeMu.Unlock()
	if s.exchange != nil {
		return nil
	}
	exchange, err := exg.NewForRuntime(snapshot, s.netDisable)
	if err != nil {
		return err
	}
	if err = initializeExplicitExchange(exchange, snapshot, s.netDisable, mode, s.logger); err != nil {
		_ = exchange.Close()
		return err
	}
	s.exchange = exchange
	return nil
}

// initializeExplicitExchange prepares contract margin rules without reading
// process-global configuration. Offline replays use Banexg's deterministic
// initializer. Networked runs first load account-specific exchange rules.
func initializeExplicitExchange(exchange banexg.BanExchange, snapshot *config.Snapshot, netDisabled bool, mode string, logger *zap.Logger) *errs.Error {
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	if _, err := exchange.LoadMarkets(false, nil); err != nil {
		return err
	}
	info := exchange.Info()
	if info == nil || !exchange.IsContract(info.MarketType) {
		return nil
	}
	if netDisabled || hasExplicitMarketSnapshot(snapshot, mode) {
		return exchange.InitLeverageBrackets()
	}
	account := ""
	if snapshot != nil {
		account = snapshot.DefaultAccount()
	}
	if err := exchange.LoadLeverageBrackets(false, map[string]interface{}{banexg.ParamAccount: account}); err == nil {
		return nil
	} else if logger != nil {
		logger.Error("LoadLeverageBrackets fail, maint margin calculation may have large deviation", zap.Error(err))
	}
	return exchange.InitLeverageBrackets()
}

func hasExplicitMarketSnapshot(snapshot *config.Snapshot, mode string) bool {
	if mode != core.RunModeBackTest && mode != core.RunModeData {
		return false
	}
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil {
		return false
	}
	cfg := snapshot.View()
	return cfg.Exchange.Items[cfg.Exchange.Name]["market_snapshot"] != nil
}

func (s *explicitEntrySession) configureRuntimeLogger(session *rpc.Session) (*zap.Logger, *errs.Error) {
	if s == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime session is not configured")
	}
	if s.logger == nil {
		var err *errs.Error
		s.logger, s.closeLogger, err = openEntryLogger(s.logArgs)
		if err != nil {
			return nil, err
		}
	}
	if session == nil {
		return s.logger, nil
	}
	return s.logger.WithOptions(zap.WrapCore(func(base zapcore.Core) zapcore.Core {
		return zapcore.NewTee(base, rpc.NewExcNotifyWithSession(session))
	})), nil
}

func (s *explicitEntrySession) startProfiles() *errs.Error {
	if s == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime session is not configured")
	}
	s.profileMu.Lock()
	if s.profileStarted {
		s.profileMu.Unlock()
		return nil
	}
	cleanup, err := startProfilesFor(s.cpuProfile, s.memProfile)
	if err != nil {
		s.profileMu.Unlock()
		return err
	}
	s.profileStarted = true
	s.profileStop = cleanup
	s.profileMu.Unlock()
	return nil
}

func (s *explicitEntrySession) close() {
	if s == nil {
		return
	}
	if s.process != nil {
		s.process.Close()
	}
	if s.storage != nil {
		s.storage.Close()
	}
	if s.exchange != nil {
		_ = s.exchange.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.profileMu.Lock()
	if s.profileStop != nil {
		s.profileStop()
		s.profileStop = nil
	}
	s.profileStarted = false
	s.profileMu.Unlock()
	if s.closeLogger != nil {
		s.closeLogger()
		s.closeLogger = nil
	}
}

func (s *explicitEntrySession) newRuntime(snapshot *config.Snapshot, mode string, startAt int64) (*runtime.Runtime, *errs.Error) {
	if err := s.ensureExchange(snapshot, mode); err != nil {
		return nil, err
	}
	return s.newStorageRuntime(snapshot, mode, startAt)
}

func (s *explicitEntrySession) newStorageRuntime(snapshot *config.Snapshot, mode string, startAt int64) (*runtime.Runtime, *errs.Error) {
	if s == nil || s.process == nil || snapshot == nil || snapshot.View() == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime session is not configured")
	}
	cfg := snapshot.View()
	catalog, catalogErr := data.RuntimeCatalogFromRegisteredSources()
	if catalogErr != nil {
		return nil, errs.New(core.ErrRunTime, catalogErr)
	}
	var exchangeName, market, contractType string
	if cfg.Exchange != nil {
		exchangeName = cfg.Exchange.Name
	}
	market, contractType = cfg.MarketType, cfg.ContractType
	rt, err := s.process.NewRuntime(runtime.Options{
		Context:         s.ctx,
		Logger:          s.logger,
		Config:          cfg,
		DataDir:         snapshot.DataDir,
		StrategyDir:     snapshot.StrategyDir,
		Mode:            mode,
		Env:             cfg.Env,
		StartAt:         startAt,
		DisplayLocation: snapshot.Location(),
		NetDisable:      s.netDisable,
		Exchange:        s.exchange,
		Storage:         s.storage,
		ExchangeName:    exchangeName,
		Market:          market,
		ContractType:    contractType,
		Pairs:           cfg.Pairs,
		Catalog:         catalog,
	})
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return rt, nil
}

func (s *explicitEntrySession) spiderExchangeFactory(snapshot *config.Snapshot, primary banexg.BanExchange) (data.SpiderExchangeFactory, func()) {
	var mu sync.Mutex
	created := make([]banexg.BanExchange, 0)
	factory := func(exchangeName, market string) (banexg.BanExchange, *errs.Error) {
		if primary != nil && snapshot.View().Exchange != nil && exchangeName == snapshot.View().Exchange.Name && market == snapshot.View().MarketType {
			return primary, nil
		}
		cfg := snapshot.View().Clone()
		if cfg.Exchange == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "exchange config is required")
		}
		cfg.Exchange.Name, cfg.MarketType = exchangeName, market
		childSnapshot := config.NewSnapshotWithDirs(cfg, snapshot.DataDir, snapshot.StrategyDir)
		exchange, err := exg.NewForRuntime(childSnapshot, s.netDisable)
		if err != nil {
			return nil, err
		}
		if _, err = exchange.LoadMarkets(false, nil); err != nil {
			_ = exchange.Close()
			return nil, err
		}
		mu.Lock()
		created = append(created, exchange)
		mu.Unlock()
		return exchange, nil
	}
	cleanup := func() {
		mu.Lock()
		defer mu.Unlock()
		for _, exchange := range created {
			_ = exchange.Close()
		}
	}
	return factory, cleanup
}

func runExplicitBackTest(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	cfg := snapshot.View()
	outPath := args.OutPath
	allocatedOutput := false
	if outPath == "" {
		hash, hashErr := cfg.HashCode()
		if hashErr != nil {
			return hashErr
		}
		basePath := snapshot.ParsePath(fmt.Sprintf("$backtest/%s", hash))
		allocatedPath, pathErr := config.AllocateOutputDir(basePath)
		if pathErr != nil {
			return errs.New(errs.CodeIOWriteFail, pathErr)
		}
		outPath = allocatedPath
		allocatedOutput = true
	}
	keepOutput := !allocatedOutput
	defer func() {
		if allocatedOutput && !keepOutput {
			_ = os.RemoveAll(outPath)
		}
	}()
	if args.Separate && len(cfg.RunPolicy) > 1 {
		session.logger.Info("run backtest separately for policies", zap.Int("num", len(cfg.RunPolicy)))
		for i, policy := range cfg.RunPolicy {
			if policy == nil {
				return errs.NewMsg(core.ErrBadConfig, "nil run policy")
			}
			session.logger.Info("start backtest", zap.Int("id", i+1), zap.String("name", policy.Name))
			policyCfg := cfg.Clone()
			policyCfg.RunPolicy = []*config.RunPolicyConfig{policy.Clone()}
			policySnapshot := config.NewSnapshotWithDirs(policyCfg, snapshot.DataDir, snapshot.StrategyDir)
			policyOut := filepath.Join(outPath, fmt.Sprintf("policy_%d", i+1))
			policyOut, runErr := runExplicitBackTestOnce(session, policySnapshot, policyOut, "")
			if runErr != nil {
				return runErr
			}
		}
		if configErr := writeDesensitizedBacktestConfig(outPath, cfg); configErr != nil {
			return configErr
		}
		keepOutput = true
		return nil
	}
	_, err = runExplicitBackTestOnce(session, snapshot, outPath, args.PrgOut)
	if err == nil {
		keepOutput = true
	}
	return err
}

func writeDesensitizedBacktestConfig(outDir string, cfg *config.Config) *errs.Error {
	if cfg == nil || outDir == "" {
		return errs.NewMsg(core.ErrBadConfig, "backtest output config is not configured")
	}
	data, err := cfg.Desensitize().DumpYaml()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "config.yml"), data, 0644); err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	return nil
}

func runExplicitBackTestOnce(session *explicitEntrySession, snapshot *config.Snapshot, outDir, prgOut string) (string, *errs.Error) {
	cfg := snapshot.View()
	startAt := int64(0)
	if cfg != nil && cfg.TimeRange != nil {
		startAt = cfg.TimeRange.StartMS
	}
	rt, err := session.newRuntime(snapshot, core.RunModeBackTest, startAt)
	if err != nil {
		return "", err
	}
	defer func() {
		rt.Close()
		rt.Join()
	}()
	logger, logErr := session.configureRuntimeLogger(rt.Notifications)
	if logErr != nil {
		return "", logErr
	}
	rt.Core.Logger = logger
	b, err := opt.NewBackTestWithRuntimeDeps(rt.BizDeps(), false, outDir)
	if err != nil {
		return "", err
	}
	if profileErr := session.startProfiles(); profileErr != nil {
		return "", profileErr
	}
	if prgOut != "" {
		lastSave := btime.UTCStamp()
		b.PBar.AddTrigger("", func(_ string, rate float64) {
			cur := btime.UTCStamp()
			if cur-lastSave < 200 && rate < 1 {
				return
			}
			lastSave = cur
			fmt.Printf("%s: %v\n", prgOut, rate)
		})
	}
	return executeBackTest(b.OutDir, b.Run)
}

func runExplicitTrade(args *config.CmdArgs, startup live.CryptoTraderStartupFunc) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	startAt := btime.UTCStamp()
	rt, err := session.newRuntime(snapshot, core.RunModeLive, startAt)
	if err != nil {
		return err
	}
	defer func() {
		rt.Close()
		rt.Join()
	}()
	logger, logErr := session.configureRuntimeLogger(rt.Notifications)
	if logErr != nil {
		return logErr
	}
	rt.Core.Logger = logger
	if args.OutPath != "" {
		dumpPath := snapshot.ParsePath(args.OutPath)
		file, fileErr := os.OpenFile(dumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if fileErr != nil {
			logger.Error("open live dump file fail", zap.Error(fileErr))
		} else {
			rt.Dump = orm.NewDumpSink(file, rt.Clock.TimeMS)
			// banexg's websocket replay stream has a different gob format from
			// the application dump, so keep it in a sibling file instead of
			// letting two encoders append to the same path.
			if dumpErr := session.exchange.SetDump(dumpPath + ".ws"); dumpErr != nil {
				_ = rt.Dump.Close()
				rt.Dump = nil
				_ = file.Close()
				return dumpErr
			}
		}
	}
	t, err := live.NewCryptoTraderWithRuntimeDeps(rt.BizDeps(), startup)
	if err != nil {
		return err
	}
	if profileErr := session.startProfiles(); profileErr != nil {
		return profileErr
	}
	return t.Run()
}

func runExplicitDownData(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()

	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() {
		rt.Close()
		rt.Join()
	}()
	logger, logErr := session.configureRuntimeLogger(rt.Notifications)
	if logErr != nil {
		return logErr
	}
	rt.Core.Logger = logger
	if profileErr := session.startProfiles(); profileErr != nil {
		return profileErr
	}

	cfg := snapshot.View()
	pairs, err := goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{
		Core: rt.Core, Clock: rt.Clock, Config: cfg, DataDir: snapshot.DataDir,
		Symbols: rt.Symbols, Storage: rt.Storage, Exchange: rt.Exchange, ShowLog: true,
	}, rt.Clock.TimeMS())
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		logger.Warn("no pairs to download")
		return nil
	}
	logger.Info("start down kline for pairs", zap.Int("num", len(pairs)), zap.Strings("tfs", args.TimeFrames))
	exsMap := make(map[int32]*orm.ExSymbol, len(pairs))
	for _, pair := range pairs {
		exs, symbolErr := rt.Symbols.GetExSymbolCur(pair)
		if symbolErr != nil {
			return symbolErr
		}
		exsMap[exs.ID] = exs
	}
	var startMS, endMS int64
	if cfg.TimeRange != nil {
		startMS, endMS = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
	}
	options := orm.NewKlineRuntimeOptions(rt.Core, cfg, rt.Clock.TimeMS(), rt.Storage)
	for _, tf := range args.TimeFrames {
		if err := orm.BulkDownOHLCVWithOptions(rt.Exchange, exsMap, tf, startMS, endMS, 0, nil, options); err != nil {
			return err
		}
	}
	return nil
}

// backtestFactory keeps process resources shared while each trial owns all
// mutable strategy, order, wallet and clock state.
func (s *explicitEntrySession) backtestFactory(snapshot *config.Snapshot, isOpt bool, outDir string) (*opt.BackTest, func(), *errs.Error) {
	if snapshot == nil || snapshot.View() == nil || snapshot.View().TimeRange == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "backtest time range is required")
	}
	rt, err := s.newRuntime(snapshot, core.RunModeBackTest, snapshot.View().TimeRange.StartMS)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { rt.Close(); rt.Join() }
	logger, err := s.configureRuntimeLogger(rt.Notifications)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	rt.Core.Logger = logger
	bt, err := opt.NewBackTestWithRuntimeDeps(rt.BizDeps(), isOpt, outDir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return bt, cleanup, nil
}

func runExplicitSimulation(args *config.CmdArgs) *errs.Error {
	if args == nil || args.InPath == "" {
		return errs.NewMsg(core.ErrBadConfig, "simulation input directory is required")
	}
	input := *args
	input.Configs = append(append(config.ArrString(nil), args.Configs...), filepath.Join(args.InPath, "config.yml"))
	return runExplicitOptimization(&input, opt.RunSimBT)
}

func runExplicitOptimization(args *config.CmdArgs, run func(*config.CmdArgs, *config.Snapshot, opt.BacktestFactory) *errs.Error) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	input := *args
	input.InPath = snapshot.ParsePath(input.InPath)
	input.OutPath = snapshot.ParsePath(input.OutPath)
	if err := session.startProfiles(); err != nil {
		return err
	}
	return run(&input, snapshot, session.backtestFactory)
}

func runExplicitReport(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	startMS := int64(0)
	if snapshot.View().TimeRange != nil {
		startMS = snapshot.View().TimeRange.StartMS
	}
	rt, err := session.newRuntime(snapshot, core.RunModeBackTest, startMS)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	return opt.BuildBtResultWithRuntimeDeps(args, rt.BizDeps())
}

func runExplicitFactors(raw []string) error {
	// NewBtFactorsCommand supplies normalized flag/value pairs.
	args := &config.CmdArgs{}
	for i := 0; i+1 < len(raw); i++ {
		if raw[i] == "--config" {
			args.Configs = append(args.Configs, raw[i+1])
			i++
		}
	}
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	startMS := int64(0)
	if snapshot.View().TimeRange != nil {
		startMS = snapshot.View().TimeRange.StartMS
	}
	rt, err := session.newRuntime(snapshot, core.RunModeBackTest, startMS)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	return opt.BtFactorsWithRuntimeDeps(raw, rt.BizDeps())
}
