package entry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
)

func runFactorHubSpec(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, groups map[string][]runner.Config, merged map[string]runner.Config, accounts, tsAccounts []string, out io.Writer, startup live.CryptoTraderStartupFunc, sourceOptions data.SubscriptionPlanOptions) (result error) {
	session, snapshot, openErr := openExplicitEntrySessionFromSpecContext(ctx, args, spec, "factor", "trade")
	if openErr != nil {
		return openErr
	}
	defer func() { result = errors.Join(result, closeFactorLiveSession(session, nil)) }()
	public, err := robotPublicSnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := ensureRobotPublicExchange(session, public); err != nil {
		return err
	}
	if err := session.startProfiles(); err != nil {
		return err
	}
	tsConfigs := map[string]runner.Config{}
	if len(tsAccounts) > 0 {
		tsConfigs, err = buildTimeSeriesLiveConfigs(spec)
		if err != nil {
			return err
		}
	}
	all := append(append([]string(nil), accounts...), tsAccounts...)
	bindings := make([]FactorLiveBinding, len(all))
	clients := make([]*execution.HubClient, len(all))
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()
	for i, account := range all {
		provider, _ := accountExecutionFields(spec.Config(), account)["live_provider"].(string)
		if provider != "" && provider != "banexg" && provider != "hub" {
			return errors.New("factor: cannot mix Hub and embedded execution providers")
		}
		c, ok := merged[account]
		if !ok {
			c = tsConfigs[account]
		}
		binding, err := robotPublicBinding(ctx, session.exchange, public, c)
		if err != nil {
			return err
		}
		if _, err := session.prepareMixedLiveBinding(snapshot, c, &binding); err != nil {
			return err
		}
		socket, err := robotHubSocket(spec, account, snapshot.DataDir)
		if err != nil {
			return err
		}
		clients[i] = execution.NewHubClient(socket)
		configs := groups[account]
		if len(configs) == 0 {
			configs = []runner.Config{c}
		}
		binding.Account, err = registerRobotHubAccount(ctx, clients[i], snapshot, configs, binding)
		if err != nil {
			return err
		}
		bindings[i] = binding
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(all))
	writer := &factorLiveWriter{Writer: out}
	for i, account := range all {
		go func(i int, account string) {
			var err error
			if configs := groups[account]; len(configs) > 0 {
				err = session.runFactorHubAccount(ctx, snapshot, configs, bindings[i], clients[i], writer, startup, sourceOptions)
			} else {
				err = session.runTimeSeriesHubAccount(ctx, snapshot, tsConfigs[account], bindings[i], clients[i], startup)
			}
			results <- err
			cancel()
		}(i, account)
	}
	for range all {
		result = errors.Join(result, <-results)
	}
	return result
}

func (s *explicitEntrySession) runFactorHubAccount(ctx context.Context, snapshot *config.Snapshot, configs []runner.Config, binding FactorLiveBinding, client *execution.HubClient, out io.Writer, startup live.CryptoTraderStartupFunc, sourceOptions data.SubscriptionPlanOptions) (result error) {
	c, err := factorLiveAccountConfig(configs)
	if err != nil {
		return err
	}
	catalog, err := data.RuntimeCatalogFromRegisteredSources()
	if err != nil {
		return err
	}
	for _, source := range binding.Sources {
		if err := catalog.RegisterDataSource(source); err != nil {
			return err
		}
	}
	cfg := mixedLiveAccountConfig(snapshot.View(), c.AccountID, mixedLivePolicies(s.runSpec, c.AccountID))
	// Robot runtimes own public inputs and computation only. Even dry_run account
	// execution and authoritative funding settlement belong to the Hub.
	options := runtimepkg.Options{Context: ctx, Logger: s.logger, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeData, Env: cfg.Env, Exchange: s.exchange, Storage: s.storage, ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType, ContractType: cfg.ContractType, Pairs: cfg.Pairs, Catalog: catalog, HubMarketData: true, SourcePlanOptions: sourceOptions, NetDisable: s.netDisable, DisplayLocation: snapshot.Location()}
	if binding.Legacy != nil {
		options.HubOrderBridge, err = robotTSHubBridge(snapshot, binding, client)
		if err != nil {
			return err
		}
		options.Mode = core.RunModeLive
	}
	rt, err := s.process.NewRuntime(options)
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	for _, symbol := range binding.Symbols {
		if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			return err
		}
	}
	if binding.Legacy != nil {
		if binding.Legacy.automatic {
			if err := loadMixedLiveJobs(rt, &binding); err != nil {
				return err
			}
		} else if err := rt.BindFactorLegacyJobs(binding.Legacy.Jobs, binding.Legacy.Subscriptions); err != nil {
			return err
		}
	}
	if startup != nil {
		trader, err := live.NewCryptoTraderWithRuntimeDeps(rt.BizDeps(), nil)
		if err != nil {
			return err
		}
		if err := startup(ctx, trader); err != nil {
			return err
		}
	}
	group := runner.NewComputationGroup()
	engines := make([]*runner.Live, 0, len(configs))
	intervals := []int64{}
	mappers := []func(*orm.DataSeries, int64) (factor.VersionRecord, error){}
	defer func() {
		for _, engine := range engines {
			engine.Stop()
		}
		for _, engine := range engines {
			result = errors.Join(result, engine.Join(context.Background()))
		}
	}()
	configs = append([]runner.Config(nil), configs...)
	for i, c := range configs {
		prepared, err := runner.PreparePortfolioConfig(c)
		if err != nil {
			return err
		}
		configs[i] = prepared
		writer, err := client.WriterLease(ctx, binding.Account, execution.StrategyID(c.StrategyID))
		if err != nil {
			return err
		}
		var nonce [24]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		claim := execution.StrategyWriterClaim{Strategy: execution.StrategyID(c.StrategyID), WriterID: "factor-" + hex.EncodeToString(nonce[:]), ExpectedEpoch: writer.Epoch}
		sink, err := runner.NewHubAccountSink(ctx, client, binding.Account, claim, prepared)
		if err != nil {
			return err
		}
		sink.Clock = rt.Clock.TimeMS
		configs[i].ComputationGroup = group
		configs[i].ComputationContext = runner.ComputationContext{DataNamespace: rt.ID, ClockDomain: rt.ID, SamplingIdentity: "live-publication-v1"}
		engine, err := runner.NewLive(configs[i], sink, rt.Clock.TimeMS, &runner.JSONOutput{Writer: out, SIDs: c.Snapshot.Universe.Evaluation})
		if err != nil {
			return err
		}
		engines = append(engines, engine)
		intervals = append(intervals, c.DecisionInterval)
		mappers = append(mappers, binding.Record)
	}
	provider, failures, err := rt.BindFactorsLive(engines, binding.Record, intervals)
	if err != nil {
		return err
	}
	defer func() { rt.Stop(); result = errors.Join(result, provider.Stop()); provider.Join() }()
	installation, err := rt.InstallFactorsLive(provider, engines, configs, mappers)
	if err != nil {
		return err
	}
	defer func() { installation.Stop(); result = errors.Join(result, installation.Join()) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rt.Context().Done():
		return hubRobotRuntimeError(rt, c.AccountID)
	case err := <-failures:
		return err
	case err := <-installation.Errors():
		return err
	case <-installation.Done():
		return hubRobotRuntimeError(rt, c.AccountID)
	}
}
