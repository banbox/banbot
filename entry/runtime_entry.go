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
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
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
	profileMu      sync.Mutex
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
	logArgs.SetLog(true)
	ctx, cancel := context.WithCancel(context.Background())
	exchange, err := exg.NewForRuntime(snapshot, args.NetDisable)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if _, err = exchange.LoadMarkets(false, nil); err != nil {
		_ = exchange.Close()
		cancel()
		return nil, nil, err
	}
	storage, err := orm.OpenStorage(ctx, cfg.Database, snapshot.DataDir)
	if err != nil {
		_ = exchange.Close()
		cancel()
		return nil, nil, err
	}
	return &explicitEntrySession{
		process:    runtime.NewProcess(),
		ctx:        ctx,
		cancel:     cancel,
		exchange:   exchange,
		storage:    storage,
		netDisable: args.NetDisable,
		cpuProfile: args.CPUProfile,
		memProfile: args.MemProfile,
		logArgs:    logArgs,
	}, snapshot, nil
}

func (s *explicitEntrySession) configureRuntimeLogger(session *rpc.Session) {
	if s == nil {
		return
	}
	args := s.logArgs
	args.SetLog(true, rpc.NewExcNotifyWithSession(session))
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
}

func (s *explicitEntrySession) newRuntime(snapshot *config.Snapshot, mode string, startAt int64) (*runtime.Runtime, *errs.Error) {
	if s == nil || s.process == nil || snapshot == nil || snapshot.View() == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime session is not configured")
	}
	cfg := snapshot.View()
	var exchangeName, market, contractType string
	if cfg.Exchange != nil {
		exchangeName = cfg.Exchange.Name
	}
	market, contractType = cfg.MarketType, cfg.ContractType
	rt, err := s.process.NewRuntime(runtime.Options{
		Context:         s.ctx,
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
	})
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return rt, nil
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
		log.Info("run backtest separately for policies", zap.Int("num", len(cfg.RunPolicy)))
		for i, policy := range cfg.RunPolicy {
			if policy == nil {
				return errs.NewMsg(core.ErrBadConfig, "nil run policy")
			}
			log.Info("start backtest", zap.Int("id", i+1), zap.String("name", policy.Name))
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
	session.configureRuntimeLogger(rt.Notifications)
	b, err := opt.NewBackTestWithRuntimeDataDepsOwned(runtimeRunnerDeps(rt), rt.Symbols, false, outDir, runtimeRunnerDataDeps(rt))
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
	session.configureRuntimeLogger(rt.Notifications)
	if args.OutPath != "" {
		dumpPath := snapshot.ParsePath(args.OutPath)
		file, fileErr := os.OpenFile(dumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if fileErr != nil {
			log.Error("open live dump file fail", zap.Error(fileErr))
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
	t := live.NewCryptoTraderWithRuntimeDataDeps(rt, runtimeRunnerDeps(rt), rt.Symbols, startup, runtimeRunnerDataDeps(rt))
	if profileErr := session.startProfiles(); profileErr != nil {
		return profileErr
	}
	return t.Run()
}
