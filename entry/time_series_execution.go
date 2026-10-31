package entry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/shopspring/decimal"
)

// These declarations describe an account, not a synthetic factor strategy.
// All risk, capital and unit choices remain explicit configuration inputs.
func buildTimeSeriesLiveConfigs(spec *config.RunSpec) (map[string]runner.Config, error) {
	u := spec.Config()
	if (u.Root.Env != core.RunEnvProd && u.Root.Env != core.RunEnvDryRun) || u.Root.MarketType != "linear" || len(u.Root.StakeCurrency) != 1 {
		return nil, errors.New("time-series execution: Hub execution requires a prod or dry_run linear account and one settlement currency")
	}
	if len(u.Root.FatalStop) > 0 || u.Root.Leverage > 0 {
		return nil, errors.New("time-series execution: legacy fatal_stop and dynamic leverage need ledger-native migration before shared live execution")
	}
	currency := u.Root.StakeCurrency[0]
	nav := u.Root.WalletAmounts[currency]
	if nav <= 0 || math.IsNaN(nav) || math.IsInf(nav, 0) {
		return nil, errors.New("time-series execution: explicit positive wallet_amounts capital is required")
	}
	result := map[string]runner.Config{}
	for _, policy := range u.RunPolicy {
		if policy.Engine != config.EngineTimeSeries {
			continue
		}
		accounts, err := u.PolicyAccounts(policy)
		if err != nil {
			return nil, err
		}
		for _, account := range accounts {
			if configured := u.Root.Accounts[account]; configured != nil && configured.Leverage > 0 {
				return nil, fmt.Errorf("time-series execution: account %s dynamic leverage is not supported", account)
			}
			if _, exists := result[account]; exists {
				continue
			}
			c := runner.Config{AccountID: account, InitialNAV: nav, AccountInitialNAV: nav, ExpiryMS: 60_000, FundingSource: "execution_funding"}
			c.Manifest.Currency = currency
			c.Manifest.Costs.FundingPolicy, _ = accountExecutionFields(u, account)["funding_policy"].(string)
			if err := applyAccountExecutionConfig(spec, account, &c.Execution); err != nil {
				return nil, fmt.Errorf("time-series execution: account %s: %w", account, err)
			}
			e := c.Execution
			if u.Root.Env == core.RunEnvDryRun && c.Manifest.Costs.FundingPolicy != "explicit-zero" {
				return nil, errors.New("time-series execution: dry_run Hub requires funding_policy: explicit-zero until simulated funding ingestion is supported")
			}
			fundingValid := u.Root.Env == core.RunEnvProd && c.Manifest.Costs.FundingPolicy == "required-stream" || u.Root.Env == core.RunEnvDryRun && c.Manifest.Costs.FundingPolicy == "explicit-zero"
			if !fundingValid || e.HistoryPath != "" || e.PaperFillModel != "" || e.StorePath == "" || e.SenderLeaseDir == "" || !e.MarginRate.IsPositive() || e.MarginRate.GreaterThan(decimal.NewFromInt(1)) || !e.MaxAccountMargin.IsPositive() || !e.MaxVirtualGross.IsPositive() || !e.StrategyGrossLimit.IsPositive() || len(e.Instruments) == 0 {
				return nil, fmt.Errorf("time-series execution: account %s requires a durable live ledger, explicit units, funding and positive risk limits", account)
			}
			c.Snapshot.SIDMap = map[int32]string{}
			pairs := slices.Clone(u.Root.Pairs)
			for _, configured := range u.RunPolicy {
				if configured.Engine == config.EngineTimeSeries && mixedLivePolicyAccount(u, configured, account) {
					pairs = append(pairs, configured.Pairs...)
				}
			}
			for sid, unit := range e.Instruments {
				if err := unit.Validate(); err != nil {
					return nil, fmt.Errorf("time-series execution: %w", err)
				}
				if sid <= 0 || unit.Valuation != "linear_perpetual" || unit.SettlementCurrency != currency {
					return nil, errors.New("time-series execution: explicit linear perpetual SID/symbol units required")
				}
				// The TS declaration uses canonical venue symbols as instrument IDs.
				c.Snapshot.SIDMap[sid] = unit.ID
			}
			for _, pair := range pairs {
				found := false
				for _, symbol := range c.Snapshot.SIDMap {
					found = found || symbol == pair
				}
				if !found {
					return nil, fmt.Errorf("time-series execution: pair %s requires explicit SID/instrument units", pair)
				}
			}
			result[account] = c
		}
	}
	if len(result) == 0 {
		return nil, errors.New("time-series execution: no configured trading strategy")
	}
	return result, nil
}

func runTimeSeriesExecution(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, startup live.CryptoTraderStartupFunc) (result error) {
	configs, err := buildTimeSeriesLiveConfigs(spec)
	if err != nil {
		return err
	}
	var accounts []string
	for account := range configs {
		accounts = append(accounts, account)
	}
	slices.Sort(accounts)
	if timeSeriesUsesHub(spec, accounts) {
		return runTimeSeriesHubSpec(ctx, args, spec, configs, accounts, startup)
	}
	if spec.Config().Root.Env == core.RunEnvDryRun {
		return errors.New("time-series execution: dry_run requires Hub paper execution; embedded live adapters are forbidden")
	}
	factories, err := resolveFactorLiveFactories(spec, accounts, "")
	if err != nil {
		return err
	}
	session, snapshot, openErr := openExplicitEntrySessionFromSpecContext(ctx, args, spec, "trade")
	if openErr != nil {
		return openErr
	}
	var bindings []FactorLiveBinding
	defer func() { result = errors.Join(result, closeFactorLiveSession(session, bindings)) }()
	if err := session.ensureExchange(snapshot, core.RunModeLive); err != nil {
		return err
	}
	// Prepare all accounts before any strategy receives live observations.
	for _, account := range accounts {
		binding, err := factories[account](ctx, session.exchange, snapshot, configs[account])
		bindings = append(bindings, binding)
		if err != nil {
			return err
		}
		if _, err := session.prepareMixedLiveBinding(snapshot, configs[account], &bindings[len(bindings)-1]); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(accounts))
	for i, account := range accounts {
		go func(c runner.Config, binding FactorLiveBinding) {
			results <- session.runTimeSeriesExecutionAccount(ctx, snapshot, c, binding, startup)
			cancel()
		}(configs[account], bindings[i])
	}
	for range accounts {
		result = errors.Join(result, <-results)
	}
	return result
}

func (s *explicitEntrySession) runTimeSeriesExecutionAccount(ctx context.Context, snapshot *config.Snapshot, c runner.Config, binding FactorLiveBinding, startup live.CryptoTraderStartupFunc) (result error) {
	if binding.Legacy == nil || binding.Legacy.Bridge == nil || binding.Transport == nil || binding.Account.Account != c.AccountID || binding.Account.SettlementDomain != c.Manifest.Currency || binding.VerifyFunding == nil {
		return errors.New("time-series execution: complete verified account binding required")
	}
	if evidence, err := binding.VerifyFunding(ctx, "required-stream"); err != nil || evidence == "" {
		return errors.Join(errors.New("time-series execution: authoritative funding evidence required"), err)
	}
	units := slices.Clone(binding.AccountInstruments)
	if len(units) == 0 {
		for sid, unit := range c.Execution.Instruments {
			units = append(units, execution.BanexgInstrument{Symbol: c.Snapshot.SIDMap[sid], Instrument: unit})
		}
	}
	for symbol, instrument := range binding.Legacy.Bridge.Instruments {
		found := false
		for _, unit := range units {
			found = found || unit.Symbol == symbol && sameFactorInstrument(unit.Instrument, instrument)
		}
		if !found {
			return fmt.Errorf("time-series execution: bridge instrument %s lacks matching account SDK units", symbol)
		}
	}
	adapter, err := execution.NewBanexgAdapter(ctx, s.exchange, execution.BanexgAdapterConfig{Account: binding.Account, Instruments: units, Transport: binding.Transport, Absence: binding.Absence})
	if err != nil {
		return err
	}
	bridge := *binding.Legacy.Bridge
	bridge.QuoteContext = func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return adapter.Observe(ctx, id)
	}
	bridge.OrderPolicy = execution.OrderPolicy{Mode: c.Execution.OrderMode, NearLimitBPS: c.Execution.NearLimitBPS, ProtectionMode: c.Execution.ProtectionMode}
	bridge.DefaultLimitPrice = adapter.DefaultLimitPrice
	if err := bridge.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.Execution.StorePath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(c.Execution.SenderLeaseDir, 0755); err != nil {
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
	sourceOptions, err := factorSourcePlanOptions(s.runSpec.Config().Data)
	if err != nil {
		return err
	}
	options := biz.SharedExecutionOptions{StorePath: c.Execution.StorePath, SenderLeaseDir: c.Execution.SenderLeaseDir, Adapter: adapter, AuthoritativeSnapshot: true}
	options.TakeoverStrategy, err = executionTakeoverOwner(cfg, &bridge)
	if err != nil {
		return err
	}
	rt, err := s.process.NewRuntime(runtimepkg.Options{Context: ctx, Logger: s.logger, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeLive, Env: cfg.Env, Exchange: s.exchange, Storage: s.storage, ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType, ContractType: cfg.ContractType, Pairs: cfg.Pairs, Catalog: catalog, AccountOwnerKey: &binding.Account, SharedExecution: &options, SharedOrderBridge: &bridge, SharedMarketData: true, SourcePlanOptions: sourceOptions, NetDisable: s.netDisable, DisplayLocation: snapshot.Location()})
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	for _, symbol := range binding.Symbols {
		if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			return err
		}
	}
	account := rt.SharedExecution()
	var instruments []execution.Instrument
	for _, unit := range units {
		instruments = append(instruments, unit.Instrument)
	}
	if err := account.RegisterAccountQuotes(instruments, func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return adapter.Observe(ctx, id)
	}, rt.Clock.TimeMS); err != nil {
		return err
	}
	strategies := map[execution.StrategyID]bool{}
	if binding.Legacy.automatic {
		for _, configured := range bridge.Strategies {
			strategies[configured.ID] = true
		}
	} else {
		for _, job := range binding.Legacy.Jobs {
			if job == nil || job.Strat == nil {
				return errors.New("time-series execution: incomplete legacy job")
			}
			configured, ok := bridge.Strategies[job.Strat.Name]
			if !ok {
				return errors.New("time-series execution: legacy job strategy is undeclared")
			}
			strategies[configured.ID] = true
		}
	}
	if err := account.ValidateAccountBindings(strategies); err != nil {
		return err
	}
	if binding.BootstrapCapital {
		if err := account.BootstrapCapital(ctx, binding.Legacy.capital, rt.Clock.TimeMS()); err != nil {
			return err
		}
	}
	if err := account.RecoverPersisted(ctx); err != nil {
		return err
	}
	if err := adapter.RecoverCash(ctx, account); err != nil {
		return err
	}
	if err := account.Reconcile("time-series-live-startup", rt.Clock.TimeMS()); err != nil {
		return err
	}
	if err := account.StartReportsContext(ctx); err != nil {
		return err
	}
	if binding.Legacy.automatic {
		if err := loadMixedLiveJobs(rt, &binding); err != nil {
			return err
		}
	} else {
		if err := rt.BindFactorLegacyJobs(binding.Legacy.Jobs, binding.Legacy.Subscriptions); err != nil {
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
	provider, providerErr := rt.NewFactorLiveProvider(func(*orm.DataSeries) {}, nil)
	if providerErr != nil {
		return providerErr
	}
	installation, err := rt.InstallTimeSeriesLive(provider)
	if err != nil {
		return err
	}
	defer func() { installation.Stop(); result = errors.Join(result, installation.Join()) }()
	reports, err := account.ReportErrors()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-reports:
			return err
		case err := <-installation.Errors():
			return errors.Join(err, account.FreezeFailure("time-series-source-error", rt.Clock.TimeMS()))
		case <-ticker.C:
			if err := adapter.RecoverCash(ctx, account); err != nil {
				return errors.Join(err, account.FreezeFailure("time-series-funding-error", rt.Clock.TimeMS()))
			}
		}
	}
}
