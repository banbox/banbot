package entry

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
)

func timeSeriesUsesHub(spec *config.RunSpec, accounts []string) bool {
	for _, account := range accounts {
		provider, _ := accountExecutionFields(spec.Config(), account)["live_provider"].(string)
		if provider == "" || provider == "banexg" || provider == "hub" {
			return true
		}
	}
	return false
}

func runTimeSeriesHubSpec(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, configs map[string]runner.Config, accounts []string, startup live.CryptoTraderStartupFunc) (result error) {
	for _, account := range accounts {
		provider, _ := accountExecutionFields(spec.Config(), account)["live_provider"].(string)
		if provider != "" && provider != "banexg" && provider != "hub" {
			return errors.New("time-series execution: cannot mix default Hub and embedded execution providers in one robot")
		}
	}
	session, snapshot, openErr := openExplicitEntrySessionFromSpecContext(ctx, args, spec, "trade")
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
	bindings := make([]FactorLiveBinding, len(accounts))
	clients := make([]*execution.HubClient, len(accounts))
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()
	for i, account := range accounts {
		c := configs[account]
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
		binding.Account, err = registerRobotHubAccount(ctx, clients[i], snapshot, []runner.Config{c}, binding)
		if err != nil {
			return err // A missing Hub is an error, never permission for a local sender.
		}
		bindings[i] = binding
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(accounts))
	for i, account := range accounts {
		go func(c runner.Config, binding FactorLiveBinding, client *execution.HubClient) {
			results <- session.runTimeSeriesHubAccount(ctx, snapshot, c, binding, client, startup)
			cancel()
		}(configs[account], bindings[i], clients[i])
	}
	for range accounts {
		result = errors.Join(result, <-results)
	}
	return result
}

func robotTSHubBridge(snapshot *config.Snapshot, binding FactorLiveBinding, client *execution.HubClient) (*biz.HubOrderBridgeConfig, error) {
	if snapshot == nil || client == nil || binding.Legacy == nil || binding.Legacy.Bridge == nil {
		return nil, errors.New("time-series execution: Hub client and explicit strategy bridge required")
	}
	var strategies []string
	for _, strategy := range binding.Legacy.Bridge.Strategies {
		strategies = append(strategies, string(strategy.ID))
	}
	slices.Sort(strategies)
	identity := fmt.Sprintf("%s/%s/%s/%q", binding.Account.VenueSessionIdentity, binding.Account.Account, binding.Account.SettlementDomain, strategies)
	path := filepath.Join(snapshot.DataDir, "execution", "robots", fmt.Sprintf("ts-%x.json", sha256.Sum256([]byte(identity))))
	return &biz.HubOrderBridgeConfig{Client: client, Account: binding.Account, Bridge: *binding.Legacy.Bridge, WriterID: fmt.Sprintf("ts-%x", sha256.Sum256([]byte(path))), StatePath: path}, nil
}

func hubRobotRuntimeError(rt *runtimepkg.Runtime, account string) error {
	if manager, ok := biz.GetOdMgrWithState(rt.Trading, account).(*biz.HubOrderMgr); ok {
		if err := manager.LastError(); err != nil {
			return err
		}
	}
	return rt.Context().Err()
}

func (s *explicitEntrySession) runTimeSeriesHubAccount(ctx context.Context, snapshot *config.Snapshot, c runner.Config, binding FactorLiveBinding, client *execution.HubClient, startup live.CryptoTraderStartupFunc) (result error) {
	bridge, err := robotTSHubBridge(snapshot, binding, client)
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
	sourceOptions, err := factorSourcePlanOptions(s.runSpec.Config().Data)
	if err != nil {
		return err
	}
	cfg := mixedLiveAccountConfig(snapshot.View(), c.AccountID, mixedLivePolicies(s.runSpec, c.AccountID))
	// The SDK above has public capabilities only; private account lifecycle stays
	// behind the Hub socket. In particular no SharedExecution owner is created.
	rt, err := s.process.NewRuntime(runtimepkg.Options{Context: ctx, Logger: s.logger, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeLive, Env: cfg.Env, Exchange: s.exchange, Storage: s.storage, ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType, ContractType: cfg.ContractType, Pairs: cfg.Pairs, Catalog: catalog, HubMarketData: true, HubOrderBridge: bridge, SourcePlanOptions: sourceOptions, NetDisable: s.netDisable, DisplayLocation: snapshot.Location()})
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	for _, symbol := range binding.Symbols {
		if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			return err
		}
	}
	if binding.Legacy.automatic {
		if err := loadMixedLiveJobs(rt, &binding); err != nil {
			return err
		}
	} else if err := rt.BindFactorLegacyJobs(binding.Legacy.Jobs, binding.Legacy.Subscriptions); err != nil {
		return err
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
	provider, err := rt.NewFactorLiveProvider(func(*orm.DataSeries) {}, nil)
	if err != nil {
		return err
	}
	installation, err := rt.InstallTimeSeriesLive(provider)
	if err != nil {
		return err
	}
	defer func() { installation.Stop(); result = errors.Join(result, installation.Join()) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rt.Context().Done():
		return hubRobotRuntimeError(rt, c.AccountID)
	case err := <-installation.Errors():
		return err
	}
}
