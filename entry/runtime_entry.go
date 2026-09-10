package entry

import (
	"context"
	"fmt"
	"os"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type explicitEntrySession struct {
	process    *runtime.Process
	ctx        context.Context
	cancel     context.CancelFunc
	exchange   banexg.BanExchange
	storage    *orm.Storage
	netDisable bool
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
	}, snapshot, nil
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
	if outPath == "" {
		hash, hashErr := cfg.HashCode()
		if hashErr != nil {
			return hashErr
		}
		outPath = fmt.Sprintf("$backtest/%s", hash)
	}
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
			policyOut, runErr := runExplicitBackTestOnce(session, policySnapshot, fmt.Sprintf("%s%d", outPath, i+1), "")
			if runErr != nil {
				return runErr
			}
			if copyErr := utils.CopyDir(policyOut, fmt.Sprintf("%s_%d", policyOut, i+1)); copyErr != nil {
				return errs.New(errs.CodeIOWriteFail, copyErr)
			}
		}
		return nil
	}
	_, err = runExplicitBackTestOnce(session, snapshot, outPath, args.PrgOut)
	return err
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
	b, err := opt.NewBackTestWithRuntimeDataDepsOwned(runtimeRunnerDeps(rt), rt.Symbols, false, outDir, runtimeRunnerDataDeps(rt))
	if err != nil {
		return "", err
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
	return t.Run()
}
