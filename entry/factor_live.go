package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// FactorLiveBinding is supplied at the banbot composition boundary. The SDK
// never needs to import a banbot proof interface. Transport.Verify must establish
// actual session capabilities; YAML flags cannot establish those guarantees.
type FactorLiveBinding struct {
	Account            execution.AccountKey
	Transport          execution.BanexgExecutionTransport
	Symbols            map[int32]*orm.ExSymbol
	Absence            func(error) bool
	Record             func(*orm.DataSeries, int64) (factor.VersionRecord, error)
	AccountInstruments []execution.BanexgInstrument
	Legacy             *FactorLegacyLiveBinding
	// VerifyFunding proves either authoritative cash settlement metadata or
	// absence of funding obligations for the supplied policy and instruments.
	VerifyFunding func(context.Context, string) (string, error)
	// Sources are fresh account-local providers, never process-global closures.
	Sources []data.DataSource
	// BootstrapCapital seeds only an empty ledger from a verified flat account.
	BootstrapCapital bool
	// Close joins resources created by this factory, after account work joins.
	// It also cleans partial resources when the factory returns an error. The
	// entry session owns the shared SDK exchange; Close must not close it.
	Close func() error
}

type FactorLegacyLiveBinding struct {
	Bridge        *biz.SharedOrderBridgeConfig
	Jobs          []*strat.StratJob
	Subscriptions []*strat.DataSub
	automatic     bool
	capital       map[execution.StrategyID]decimal.Decimal
}

type FactorLiveBindingFactory func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error)

var factorLiveBindings = struct {
	sync.RWMutex
	items map[string]FactorLiveBindingFactory
}{items: make(map[string]FactorLiveBindingFactory)}

// RegisterFactorLiveBinding installs a verified session integration supplied by
// an embedding application. Registrations cannot silently replace one another.
func RegisterFactorLiveBinding(name string, factory FactorLiveBindingFactory) error {
	if name == "" || factory == nil {
		return errors.New("factor: live binding name and factory required")
	}
	factorLiveBindings.Lock()
	defer factorLiveBindings.Unlock()
	if factorLiveBindings.items[name] != nil {
		return fmt.Errorf("factor: live binding %q already registered", name)
	}
	factorLiveBindings.items[name] = factory
	return nil
}

func factorLiveFactory(name string) (FactorLiveBindingFactory, error) {
	if name == "" || name == "banexg" {
		return newBanexgFactorLiveBinding, nil
	}
	factorLiveBindings.RLock()
	f := factorLiveBindings.items[name]
	factorLiveBindings.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("factor: unsupported live capability: binding %q is not registered", name)
	}
	return f, nil
}

// resolveFactorLiveFactories applies the provider precedence per account:
// explicit CLI name, account override, global execution default, then the
// registry default. It validates every provider before opening any session.
func resolveFactorLiveFactories(spec *config.RunSpec, accounts []string, cliName string) (map[string]FactorLiveBindingFactory, error) {
	if spec == nil {
		return nil, errors.New("factor: live configuration is required")
	}
	result := make(map[string]FactorLiveBindingFactory, len(accounts))
	global, _ := spec.Config().Execution["live_provider"].(string)
	for _, account := range accounts {
		provider := cliName
		if provider == "" {
			provider, _ = spec.Config().AccountExecution[account]["live_provider"].(string)
		}
		if provider == "" {
			provider = global
		}
		factory, err := factorLiveFactory(provider)
		if err != nil {
			return nil, fmt.Errorf("account %s: %w", account, err)
		}
		result[account] = factory
	}
	return result, nil
}

func closeFactorLiveBindings(bindings []FactorLiveBinding) (result error) {
	for i := len(bindings) - 1; i >= 0; i-- {
		if bindings[i].Close != nil {
			result = errors.Join(result, bindings[i].Close())
		}
	}
	return result
}

// Preparation owns partial sessions until every account is ready. No account
// starts trading while another factory is still capable of failing.
func prepareFactorLiveBindings(ctx context.Context, factory FactorLiveBindingFactory, exchange banexg.BanExchange, snapshot *config.Snapshot, accounts []string, configs map[string]runner.Config) (bindings []FactorLiveBinding, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, closeFactorLiveBindings(bindings))
			bindings = nil
		}
	}()
	for _, account := range accounts {
		if err := ctx.Err(); err != nil {
			return bindings, err
		}
		binding, err := factory(ctx, exchange, snapshot, configs[account])
		bindings = append(bindings, binding)
		if err != nil {
			return bindings, err
		}
	}
	return bindings, ctx.Err()
}

func closeFactorLiveSession(session *explicitEntrySession, bindings []FactorLiveBinding) error {
	// A binding may own transport workers still used by the Process. Join all
	// admitted operations before closing those workers and the shared exchange.
	session.process.Close()
	result := errors.Join(session.process.CloseError(), closeFactorLiveBindings(bindings))
	session.close()
	return result
}

// runFactorLiveSpecWithArgs is the CLI/runtime integration boundary for factor
// live runs, preserving command arguments, logging and the startup lifecycle.
func runFactorLiveSpecWithArgs(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, configs []runner.Config, name string, out io.Writer, startup live.CryptoTraderStartupFunc) (resultErr error) {
	if len(configs) == 0 || spec == nil {
		return errors.New("factor: live strategies are required")
	}
	if args == nil {
		args = &config.CmdArgs{}
	}
	for _, c := range configs {
		if err := validateFactorLiveConfig(c); err != nil {
			return err
		}
	}
	for _, policy := range spec.Config().RunPolicy {
		if policy.Engine == config.EngineTimeSeries {
			if _, err := spec.Config().PolicyAccounts(policy); err != nil {
				return err
			}
			if _, registered := strat.GetStrategyFactory(policy.Name); !registered {
				return fmt.Errorf("mixed live: TS strategy %s is not registered", policy.Name)
			}
		}
	}
	sourceOptions, err := factorSourcePlanOptions(spec.Config().Data)
	if err != nil {
		return err
	}
	groups := map[string][]runner.Config{}
	mergedAccounts := map[string]runner.Config{}
	var accounts []string
	for _, c := range configs {
		if _, exists := groups[c.AccountID]; !exists {
			accounts = append(accounts, c.AccountID)
		}
		groups[c.AccountID] = append(groups[c.AccountID], c)
	}
	for _, account := range accounts {
		merged, err := factorLiveAccountConfig(groups[account])
		if err != nil {
			return err
		}
		mergedAccounts[account] = merged
	}
	factories, err := resolveFactorLiveFactories(spec, accounts, name)
	if err != nil {
		return err
	}
	session, snapshot, openErr := openExplicitEntrySessionFromSpecContext(ctx, args, spec, "factor", "trade")
	if openErr != nil {
		return openErr
	}
	var bindings []FactorLiveBinding
	defer func() { resultErr = errors.Join(resultErr, closeFactorLiveSession(session, bindings)) }()
	if err := session.ensureExchange(snapshot, core.RunModeLive); err != nil {
		return err
	}
	if err := session.startProfiles(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := &factorLiveWriter{Writer: out}
	var tsAccounts []string
	for _, policy := range spec.Config().RunPolicy {
		if policy.Engine == config.EngineTimeSeries {
			policyAccounts, err := spec.Config().PolicyAccounts(policy)
			if err != nil {
				return err
			}
			for _, account := range policyAccounts {
				if groups[account] == nil && !slices.Contains(tsAccounts, account) {
					tsAccounts = append(tsAccounts, account)
				}
			}
		}
	}
	results := make(chan error, len(accounts)+len(tsAccounts))
	// prepareFactorLiveBindings is account-scoped; route each request to its
	// already validated provider while retaining one cleanup lifecycle.
	routedFactory := func(ctx context.Context, exchange banexg.BanExchange, snapshot *config.Snapshot, cfg runner.Config) (FactorLiveBinding, error) {
		factory := factories[cfg.AccountID]
		if factory == nil {
			return FactorLiveBinding{}, fmt.Errorf("account %s: live provider is not configured", cfg.AccountID)
		}
		return factory(ctx, exchange, snapshot, cfg)
	}
	bindings, err = prepareFactorLiveBindings(ctx, routedFactory, session.exchange, snapshot, accounts, mergedAccounts)
	if err != nil {
		return err
	}
	for index, account := range accounts {
		if _, err := session.prepareMixedLiveBinding(snapshot, mergedAccounts[account], &bindings[index]); err != nil {
			return err
		}
	}
	for index, account := range accounts {
		group, binding := groups[account], bindings[index]
		go func() {
			err := session.runFactorsLiveWithStartup(ctx, snapshot, group, binding, writer, startup, sourceOptions)
			results <- err
			cancel()
		}()
	}
	for _, account := range tsAccounts {
		go func() { results <- session.runMixedLiveTSAccount(ctx, snapshot, account, startup); cancel() }()
	}
	for range len(accounts) + len(tsAccounts) {
		resultErr = errors.Join(resultErr, <-results)
	}
	return resultErr
}

// Check all choices that need no session evidence before creating resources.
// Binding-specific symbol and venue capabilities are checked after preparation.
func validateFactorLiveConfig(c runner.Config) error {
	if c.Execution.HistoryPath != "" {
		return errors.New("factor: cold history is only available for simulated replay")
	}
	if len(c.Chunks) != 0 {
		return errors.New("factor: live trade refuses archive chunks; use --dry-run for archive replay")
	}
	e := c.Execution
	if !e.MarginRate.IsPositive() || e.MarginRate.GreaterThan(decimal.NewFromInt(1)) || !e.MaxAccountMargin.IsPositive() || !e.MaxVirtualGross.IsPositive() || !e.StrategyGrossLimit.IsPositive() || len(e.Instruments) == 0 {
		return errors.New("factor: live requires explicit instrument units and positive absolute risk limits")
	}
	if !filepath.IsAbs(e.StorePath) || !filepath.IsAbs(e.SenderLeaseDir) {
		return errors.New("factor: live ledger and sender lease paths must be absolute")
	}
	for _, sid := range append(slices.Clone(c.Snapshot.Universe.Tracked), c.Snapshot.Universe.Investable...) {
		if slices.Contains(c.Snapshot.Universe.Tracked, sid) || slices.Contains(c.Snapshot.Universe.Tradable, sid) {
			if _, exists := e.Instruments[sid]; !exists {
				return fmt.Errorf("factor: execution live SID %d has no instrument units", sid)
			}
		}
	}
	seen := make(map[string]bool, len(e.Instruments))
	for sid, unit := range e.Instruments {
		if err := unit.Validate(); err != nil {
			return err
		}
		if sid <= 0 || seen[unit.ID] || unit.SettlementCurrency != c.Manifest.Currency || c.Snapshot.SIDMap[sid] == "" {
			return fmt.Errorf("factor: invalid live instrument identity or currency for SID %d", sid)
		}
		seen[unit.ID] = true
	}
	if c.Prices.Source == "" || c.Prices.Field == "" || c.Prices.TimeFrame != "event" && c.Prices.TimeFrame != "1m" {
		return errors.New("factor: live requires event or 1m observable price metadata")
	}
	return runner.ValidateLiveConfig(c)
}

func (s *explicitEntrySession) runFactorLive(ctx context.Context, snapshot *config.Snapshot, c runner.Config, binding FactorLiveBinding, out io.Writer) (resultErr error) {
	return s.runFactorsLive(ctx, snapshot, []runner.Config{c}, binding, out)
}

type factorLiveWriter struct {
	sync.Mutex
	Writer io.Writer
}

func (w *factorLiveWriter) Write(p []byte) (int, error) {
	w.Lock()
	defer w.Unlock()
	return w.Writer.Write(p)
}

// One account gets one verified transport, provider and owner. Strategy limits
// remain independent while common account policy must agree before any I/O.
func factorLiveAccountConfig(configs []runner.Config) (runner.Config, error) {
	if len(configs) == 0 {
		return runner.Config{}, errors.New("factor: live strategies required")
	}
	c := configs[0]
	c.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
	if c.Snapshot.SIDMap == nil {
		c.Snapshot.SIDMap = map[int32]string{}
	}
	c.Execution.Instruments = map[int32]execution.Instrument{}
	seen := map[string]bool{}
	for _, cfg := range configs {
		if cfg.Execution.HistoryPath != "" {
			return runner.Config{}, errors.New("factor: cold history is only available for simulated replay")
		}
		if cfg.StrategyID == "" || seen[cfg.StrategyID] || len(cfg.Chunks) != 0 || cfg.AccountID != c.AccountID || cfg.Manifest.Currency != c.Manifest.Currency || cfg.Manifest.Costs.FundingPolicy != c.Manifest.Costs.FundingPolicy || cfg.FundingSource != c.FundingSource || cfg.Execution.StorePath != c.Execution.StorePath || cfg.Execution.SenderLeaseDir != c.Execution.SenderLeaseDir || !cfg.Execution.MarginRate.Equal(c.Execution.MarginRate) || !cfg.Execution.MaxAccountMargin.Equal(c.Execution.MaxAccountMargin) || !cfg.Execution.MaxVirtualGross.Equal(c.Execution.MaxVirtualGross) {
			return runner.Config{}, errors.New("factor: incompatible shared live account strategy declarations")
		}
		seen[cfg.StrategyID] = true
		for source, version := range cfg.Snapshot.SourceVersions {
			if previous := c.Snapshot.SourceVersions[source]; previous != "" && previous != version {
				return runner.Config{}, errors.New("factor: conflicting shared live source versions")
			}
			if c.Snapshot.SourceVersions == nil {
				c.Snapshot.SourceVersions = map[string]string{}
			}
			c.Snapshot.SourceVersions[source] = version
		}
		for sid, symbol := range cfg.Snapshot.SIDMap {
			if previous, ok := c.Snapshot.SIDMap[sid]; ok && previous != symbol {
				return runner.Config{}, errors.New("factor: conflicting live SID mapping")
			}
			c.Snapshot.SIDMap[sid] = symbol
		}
		for sid, unit := range cfg.Execution.Instruments {
			if previous, ok := c.Execution.Instruments[sid]; ok && (previous.ID != unit.ID || previous.Version != unit.Version || previous.Valuation != unit.Valuation || previous.SettlementCurrency != unit.SettlementCurrency || previous.MoneyScale != unit.MoneyScale || previous.MinSteps != unit.MinSteps || !previous.QuantityStep.Equal(unit.QuantityStep) || !previous.ContractSize.Equal(unit.ContractSize) || !previous.PriceTick.Equal(unit.PriceTick) || !previous.MinNotional.Equal(unit.MinNotional)) {
				return runner.Config{}, errors.New("factor: conflicting live instrument units")
			}
			c.Execution.Instruments[sid] = unit
		}
		for _, pools := range []struct {
			dst *[]int32
			src []int32
		}{{&c.Snapshot.Universe.Tracked, cfg.Snapshot.Universe.Tracked}, {&c.Snapshot.Universe.Investable, cfg.Snapshot.Universe.Investable}, {&c.Snapshot.Universe.Tradable, cfg.Snapshot.Universe.Tradable}, {&c.Snapshot.Universe.Reference, cfg.Snapshot.Universe.Reference}, {&c.Snapshot.Universe.Evaluation, cfg.Snapshot.Universe.Evaluation}} {
			for _, sid := range pools.src {
				if !slices.Contains(*pools.dst, sid) {
					*pools.dst = append(*pools.dst, sid)
				}
			}
		}
	}
	return c, nil
}

func (s *explicitEntrySession) runFactorsLive(ctx context.Context, snapshot *config.Snapshot, configs []runner.Config, binding FactorLiveBinding, out io.Writer, sourceOptions ...data.SubscriptionPlanOptions) (resultErr error) {
	return s.runFactorsLiveWithStartup(ctx, snapshot, configs, binding, out, nil, sourceOptions...)
}

func (s *explicitEntrySession) runFactorsLiveWithStartup(ctx context.Context, snapshot *config.Snapshot, configs []runner.Config, binding FactorLiveBinding, out io.Writer, startup live.CryptoTraderStartupFunc, sourceOptions ...data.SubscriptionPlanOptions) (resultErr error) {
	for _, cfg := range configs {
		if err := validateFactorLiveConfig(cfg); err != nil {
			return err
		}
	}
	c, err := factorLiveAccountConfig(configs)
	if err != nil {
		return err
	}
	if len(c.Chunks) != 0 || binding.Record == nil || binding.Transport == nil || binding.Account.Account != c.AccountID || binding.Account.SettlementDomain != c.Manifest.Currency {
		return errors.New("factor: live requires current record metadata, verified transport and matching account/currency")
	}
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil {
		return errors.New("factor: live runtime exchange configuration required")
	}
	policy := c.Manifest.Costs.FundingPolicy
	tsCapital, err := s.prepareMixedLiveBinding(snapshot, c, &binding)
	if err != nil {
		return err
	}
	autoLegacy := binding.Legacy != nil && binding.Legacy.automatic
	if (policy != "explicit-zero" && policy != "required-stream") || binding.VerifyFunding == nil {
		return errors.New("factor: live funding policy requires verified session evidence")
	}
	if evidence, err := binding.VerifyFunding(ctx, policy); err != nil {
		return err
	} else if evidence == "" {
		return errors.New("factor: funding session evidence absent")
	}
	e := c.Execution
	units := make([]execution.BanexgInstrument, 0, len(e.Instruments))
	units = append(units, binding.AccountInstruments...)
	for sid, unit := range e.Instruments {
		symbol := binding.Symbols[sid]
		if symbol == nil || symbol.ID != sid || symbol.Symbol == "" || c.Snapshot.SIDMap[sid] != symbol.Symbol {
			return fmt.Errorf("factor: live SID %d missing symbol/manifest mapping", sid)
		}
		found := false
		for _, existing := range units {
			if existing.Instrument.ID == unit.ID {
				a, _ := json.Marshal(existing.Instrument)
				b, _ := json.Marshal(unit)
				if existing.Symbol != symbol.Symbol || string(a) != string(b) {
					return errors.New("factor: conflicting explicit account instrument mapping")
				}
				found = true
			}
		}
		if !found {
			units = append(units, execution.BanexgInstrument{Symbol: symbol.Symbol, Instrument: unit})
		}
	}
	required := append([]int32(nil), c.Snapshot.Universe.Tracked...)
	for _, sid := range c.Snapshot.Universe.Investable {
		if slices.Contains(c.Snapshot.Universe.Tradable, sid) {
			required = append(required, sid)
		}
	}
	for _, sid := range required {
		if _, ok := e.Instruments[sid]; !ok {
			return fmt.Errorf("factor: execution live SID %d has no instrument units", sid)
		}
	}
	unitIDs, unitSymbols := map[string]bool{}, map[string]bool{}
	for _, unit := range units {
		if err := unit.Instrument.Validate(); err != nil {
			return err
		}
		if unit.Symbol == "" || strings.TrimSpace(unit.Symbol) != unit.Symbol || unit.Instrument.SettlementCurrency != binding.Account.SettlementDomain || unitIDs[unit.Instrument.ID] || unitSymbols[unit.Symbol] {
			return errors.New("factor: invalid or duplicate account instrument mapping")
		}
		unitIDs[unit.Instrument.ID], unitSymbols[unit.Symbol] = true, true
	}
	if err := os.MkdirAll(filepath.Dir(e.StorePath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(e.SenderLeaseDir, 0755); err != nil {
		return err
	}
	adapter, err := execution.NewBanexgAdapter(ctx, s.exchange, execution.BanexgAdapterConfig{Account: binding.Account, Instruments: units, Transport: binding.Transport, Absence: binding.Absence})
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
	cfg := snapshot.View()
	if s.runSpec != nil {
		cfg = mixedLiveAccountConfig(cfg, c.AccountID, mixedLivePolicies(s.runSpec, c.AccountID))
	}
	opts := biz.SharedExecutionOptions{StorePath: e.StorePath, SenderLeaseDir: e.SenderLeaseDir, Adapter: adapter, AuthoritativeSnapshot: true}
	var bridge *biz.SharedOrderBridgeConfig
	if binding.Legacy != nil {
		if binding.Legacy.Bridge == nil || !autoLegacy && len(binding.Legacy.Jobs) == 0 {
			return errors.New("factor: legacy bridge and configured jobs required")
		}
		copyBridge := *binding.Legacy.Bridge
		for _, instrument := range copyBridge.Instruments {
			found := false
			for _, unit := range units {
				if unit.Instrument.ID == instrument.ID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("factor: legacy instrument %s requires explicit account SDK mapping", instrument.ID)
			}
		}
		copyBridge.QuoteContext = func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
			return adapter.Observe(ctx, id)
		}
		bridge = &copyBridge
	}
	var planOptions data.SubscriptionPlanOptions
	if len(sourceOptions) > 0 {
		planOptions = sourceOptions[0]
	}
	rt, err := s.process.NewRuntime(runtime.Options{Context: ctx, Logger: s.logger, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeLive, Env: cfg.Env, Exchange: s.exchange, Storage: s.storage, ExchangeName: cfg.Exchange.Name, Market: cfg.MarketType, ContractType: cfg.ContractType, Pairs: cfg.Pairs, Catalog: catalog, AccountOwnerKey: &binding.Account, SharedExecution: &opts, SharedOrderBridge: bridge, SharedMarketData: true, SourcePlanOptions: planOptions, NetDisable: s.netDisable, DisplayLocation: snapshot.Location()})
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	rt.Core.LogFile = s.logArgs.Logfile
	for _, symbol := range binding.Symbols {
		if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			return err
		}
	}
	account := rt.SharedExecution()
	instruments := make([]execution.Instrument, 0, len(units))
	for _, i := range units {
		instruments = append(instruments, i.Instrument)
	}
	if err := account.RegisterAccountQuotes(instruments, func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return adapter.Observe(ctx, id)
	}, rt.Clock.TimeMS); err != nil {
		return err
	}
	strategies := map[execution.StrategyID]bool{}
	for _, cfg := range configs {
		strategies[execution.StrategyID(cfg.StrategyID)] = true
	}
	if autoLegacy {
		for _, configured := range bridge.Strategies {
			strategies[configured.ID] = true
		}
	} else if binding.Legacy != nil {
		for _, job := range binding.Legacy.Jobs {
			if job == nil || job.Strat == nil {
				return errors.New("factor: incomplete legacy job")
			}
			configured, ok := bridge.Strategies[job.Strat.Name]
			if !ok {
				return errors.New("factor: legacy job strategy is undeclared")
			}
			strategies[configured.ID] = true
		}
	}
	if err := account.ValidateAccountBindings(strategies); err != nil {
		return err
	}
	if binding.BootstrapCapital {
		capital := map[execution.StrategyID]decimal.Decimal{}
		for id, value := range tsCapital {
			capital[id] = value
		}
		for _, cfg := range configs {
			if cfg.InitialNAV <= 0 || math.IsNaN(cfg.InitialNAV) || math.IsInf(cfg.InitialNAV, 0) {
				return errors.New("factor: explicit positive initial_nav required for live capital allocation")
			}
			capital[execution.StrategyID(cfg.StrategyID)] = decimal.NewFromFloat(cfg.InitialNAV)
		}
		if err := account.BootstrapCapital(ctx, capital, rt.Clock.TimeMS()); err != nil {
			return err
		}
	}
	if err := account.RecoverPersisted(ctx); err != nil {
		return err
	}
	if err := adapter.RecoverCash(ctx, account); err != nil {
		return err
	}
	if err := account.Reconcile("factor-live-startup", rt.Clock.TimeMS()); err != nil {
		return err
	}
	if err := account.StartReportsContext(ctx); err != nil {
		return err
	}
	reports, err := account.ReportErrors()
	if err != nil {
		return err
	}
	if autoLegacy {
		if err := loadMixedLiveJobs(rt, &binding); err != nil {
			return err
		}
	}
	if binding.Legacy != nil && !autoLegacy {
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
	group := runner.NewComputationGroup()
	engines := make([]*runner.Live, 0, len(configs))
	intervals := make([]int64, 0, len(configs))
	mappers := make([]func(*orm.DataSeries, int64) (factor.VersionRecord, error), 0, len(configs))
	configs = append([]runner.Config(nil), configs...)
	defer func() {
		for _, engine := range engines {
			engine.Stop()
		}
		for _, engine := range engines {
			resultErr = errors.Join(resultErr, engine.Join(context.Background()))
		}
	}()
	writer := &factorLiveWriter{Writer: out}
	for index, cfg := range configs {
		e := cfg.Execution
		sink := &runner.AccountSink{Account: account, AccountID: cfg.AccountID, StrategyID: cfg.StrategyID, Currency: cfg.Manifest.Currency, Instruments: e.Instruments, Clock: rt.Clock.TimeMS, AuthoritativeFunding: true,
			VisibleQuote: func(ctx context.Context, instrument string, _ int64) (execution.VisibleQuote, error) {
				return account.VisibleQuote(ctx, instrument, rt.Clock.TimeMS())
			},
			Risk: execution.PortfolioRisk{MarginRate: e.MarginRate, MaxAccountMargin: e.MaxAccountMargin, MaxVirtualGross: e.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{execution.StrategyID(cfg.StrategyID): e.StrategyGrossLimit}}}
		if policy == "required-stream" {
			sink.FundingInstruments = map[int32]execution.Instrument{}
			for _, unit := range units {
				found := false
				for sid, symbol := range binding.Symbols {
					if symbol.Symbol == unit.Symbol {
						sink.FundingInstruments[sid] = unit.Instrument
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("factor: account funding SID mapping missing: %s", unit.Symbol)
				}
			}
		}
		sink.PolicySIDMap = make(map[int32]string, len(cfg.Snapshot.SIDMap))
		for sid, symbol := range cfg.Snapshot.SIDMap {
			sink.PolicySIDMap[sid] = symbol
		}
		if err := sink.RegisterExecution(); err != nil {
			return err
		}
		configs[index].ComputationGroup = group
		configs[index].ComputationContext = runner.ComputationContext{DataNamespace: rt.ID, ClockDomain: rt.ID, SamplingIdentity: "live-publication-v1"}
		engine, err := runner.NewLive(configs[index], sink, rt.Clock.TimeMS, &runner.JSONOutput{Writer: writer, SIDs: cfg.Snapshot.Universe.Evaluation})
		if err != nil {
			return err
		}
		engines = append(engines, engine)
		intervals = append(intervals, cfg.DecisionInterval)
		mappers = append(mappers, binding.Record)
	}
	provider, failures, bindErr := rt.BindFactorsLive(engines, binding.Record, intervals)
	if bindErr != nil {
		return bindErr
	}
	defer func() {
		rt.Stop()
		if err := provider.Stop(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		provider.Join()
	}()
	sourceOwner, err := rt.InstallFactorsLive(provider, engines, configs, mappers)
	if err != nil {
		return err
	}
	streamErrors := sourceOwner.Errors()
	var result error
	select {
	case <-sourceOwner.Done():
	case result = <-failures:
	case result = <-streamErrors:
	case result = <-reports:
	case <-ctx.Done():
		result = ctx.Err()
	}
	rt.Stop()
	if err := provider.Stop(); err != nil {
		result = errors.Join(result, err)
	}
	provider.Join()
	sourceOwner.Stop()
	result = errors.Join(result, sourceOwner.Join())
	// Source Join retains producer failures even when provider shutdown wins
	// the select. Complete owned source/report lifecycles before draining causes.
	rt.Close()
	rt.Join()
	// A provider can finish immediately after a source stops it. Retain the
	// already published failure even when the loop completion wins the select.
	for _, failures := range []<-chan error{failures, streamErrors, reports} {
	drain:
		for {
			select {
			case err, ok := <-failures:
				if !ok {
					break drain
				}
				result = errors.Join(result, err)
			default:
				break drain
			}
		}
	}
	result = errors.Join(result, ctx.Err())
	return result
}

func factorLiveKlineSubscriptions(engine *runner.Live, c runner.Config, symbols map[int32]*orm.ExSymbol) ([]data.Subscription, error) {
	var subs []data.Subscription
	add := func(input factor.InputSpec, sids []int32) error {
		if input.Source != orm.SeriesSourceKline {
			return nil
		}
		for _, sid := range sids {
			symbol := symbols[sid]
			if symbol == nil || symbol.ID != sid || symbol.Symbol != c.Snapshot.SIDMap[sid] {
				return fmt.Errorf("factor: live kline SID %d mapping is absent or mismatched", sid)
			}
			subs = append(subs, data.Subscription{Source: input.Source, ExSymbol: symbol, TimeFrame: input.TimeFrame, Fields: append([]string(nil), input.Fields...), WarmupNum: input.WarmupLength})
		}
		return nil
	}
	for _, input := range engine.Inputs() {
		if err := add(input, engine.DataSIDs()); err != nil {
			return nil, err
		}
	}
	if err := add(factor.InputSpec{Source: c.Prices.Source, TimeFrame: c.Prices.TimeFrame, Fields: []string{c.Prices.Field}}, engine.ExecutionSIDs()); err != nil {
		return nil, err
	}
	return subs, nil
}
